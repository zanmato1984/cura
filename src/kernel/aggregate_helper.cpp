#include "aggregate_helper.h"
#include "cura/data/column_scalar.h"
#include "cura/data/column_vector.h"
#include "cura/data/fragment.h"
#include "cura/expression/literal.h"
#include "helper.h"

#include <arrow/compute/api.h>
#include <arrow/visitor_inline.h>
#include <sstream>

#ifdef USE_CUDF
#include <cudf/aggregation.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/groupby.hpp>
#include <cudf/reduction.hpp>
#include <cudf/unary.hpp>
#endif

namespace cura::kernel::detail {

using cura::data::Column;
using cura::data::ColumnScalar;
using cura::data::ColumnVector;
using cura::expression::Literal;

#ifdef USE_CUDF
using cura::data::ColumnVectorCudfColumn;
using cura::data::createCudfColumnVector;

std::shared_ptr<cudf::column> cast(std::shared_ptr<cudf::column> src,
                                   const DataType &data_type) {
  if (src->type() == data_type) {
    return src;
  }

  return cudf::cast(src->view(), data_type);
}

std::unique_ptr<cudf::aggregation>
toCudfAggregation(const PhysicalAggregation &aggregation) {
  switch (aggregation.op) {
  case AggregationOperator::SUM:
    return cudf::make_sum_aggregation();
  case AggregationOperator::PRODUCT:
    return cudf::make_product_aggregation();
  case AggregationOperator::MIN:
    return cudf::make_min_aggregation();
  case AggregationOperator::MAX:
    return cudf::make_max_aggregation();
  case AggregationOperator::COUNT_VALID:
    return cudf::make_count_aggregation(cudf::null_policy::EXCLUDE);
  case AggregationOperator::COUNT_ALL:
    return cudf::make_count_aggregation(cudf::null_policy::INCLUDE);
  case AggregationOperator::ANY:
    return cudf::make_any_aggregation();
  case AggregationOperator::ALL:
    return cudf::make_all_aggregation();
  case AggregationOperator::SUM_OF_SQUARES:
    return cudf::make_sum_of_squares_aggregation();
  case AggregationOperator::MEAN:
    return cudf::make_mean_aggregation();
  case AggregationOperator::MEDIAN:
    return cudf::make_median_aggregation();
  case AggregationOperator::QUANTILE:
    return cudf::make_quantile_aggregation({});
  case AggregationOperator::ARGMAX:
    return cudf::make_argmax_aggregation();
  case AggregationOperator::ARGMIN:
    return cudf::make_argmin_aggregation();
  case AggregationOperator::NUNIQUE:
    return cudf::make_nunique_aggregation(cudf::null_policy::INCLUDE);
  case AggregationOperator::NTH_ELEMENT:
    return cudf::make_nth_element_aggregation(aggregation.n);
  default:
    CURA_FAIL("Unknown aggregation operator " +
              std::to_string(static_cast<int32_t>(aggregation.op)));
  }
}

using cura::data::ColumnVector;
std::shared_ptr<Fragment>
doAggregateWithoutKey(const Context &ctx, const Schema &schema,
                      const std::vector<PhysicalAggregation> &aggregations,
                      std::shared_ptr<const Fragment> fragment) {
  std::vector<std::shared_ptr<const Column>> results;
  for (const auto &aggregation : aggregations) {
    std::shared_ptr<const cudf::scalar> scalar = [&]() {
      // Reduction in cudf doesn't support `COUNT_ALL` and `COUNT_VALID`,
      // conclude them separate.
      if (aggregation.op == AggregationOperator::COUNT_ALL) {
        return Literal(aggregation.data_type.type_id, fragment->size()).cudf();
      } else if (aggregation.op == AggregationOperator::COUNT_VALID) {
        const auto &column = fragment->column<Column>(aggregation.idx);
        if (auto cv = std::dynamic_pointer_cast<const ColumnVector>(column);
            cv) {
          return Literal(
                     aggregation.data_type.type_id,
                     fragment->column(aggregation.idx)->size() -
                         fragment->column(aggregation.idx)->cudf().null_count())
              .cudf();
        } else if (auto cs =
                       std::dynamic_pointer_cast<const ColumnScalar>(column);
                   cs) {
          return cs->cudf().is_valid()
                     ? Literal(aggregation.data_type.type_id, fragment->size())
                           .cudf()
                     : Literal(aggregation.data_type.type_id, 0).cudf();

        } else {
          CURA_FAIL("Neither column vector nor column scalar");
        }
      }
      return std::shared_ptr<const cudf::scalar>(
          cudf::reduce(fragment->column(aggregation.idx)->cudf(),
                       toCudfAggregation(aggregation), aggregation.data_type,
                       ctx.memory_resource->converge()));
    }();

    auto column = cudf::make_column_from_scalar(
        *scalar, 1, rmm::cuda_stream_default, ctx.memory_resource->converge());
    if (aggregation.data_type != column->type()) {
      column = cudf::cast(column->view(), aggregation.data_type,
                          ctx.memory_resource->converge());
    }
    results.emplace_back(createCudfColumnVector<ColumnVectorCudfColumn>(
        aggregation.data_type, std::move(column)));
  }
  return std::make_shared<Fragment>(std::move(results));
}

std::shared_ptr<Fragment>
doAggregate(const Context &ctx, const Schema &schema,
            const std::vector<ColumnIdx> &keys,
            const std::vector<PhysicalAggregation> &aggregations,
            std::shared_ptr<const Fragment> fragment) {
  /// Shortcut for non-key aggregations.
  if (keys.empty()) {
    return doAggregateWithoutKey(ctx, schema, aggregations, fragment);
  }

  /// Make cudf groupby object.
  std::vector<cudf::column_view> column_views(keys.size());
  std::transform(
      keys.begin(), keys.end(), column_views.begin(),
      [&](const auto &key) { return fragment->column(key)->cudf(); });
  cudf::groupby::groupby gby(cudf::table_view(column_views),
                             cudf::null_policy::INCLUDE);

  /// Perform aggregations in one pass.
  std::vector<cudf::groupby::aggregation_request> requests(aggregations.size());
  std::transform(aggregations.begin(), aggregations.end(), requests.begin(),
                 [&](const auto &aggregation) {
                   std::vector<std::unique_ptr<cudf::aggregation>> aggs;
                   aggs.emplace_back(toCudfAggregation(aggregation));
                   return cudf::groupby::aggregation_request{
                       fragment->column(aggregation.idx)->cudf(),
                       std::move(aggs)};
                 });
  auto [result_keys, result_aggs] =
      gby.aggregate(requests, ctx.memory_resource->converge());

  std::vector<std::shared_ptr<const Column>> results;

  /// Pack group columns.
  {
    auto key_cols = result_keys->release();
    for (size_t i = 0; i < key_cols.size(); i++) {
      results.emplace_back(createCudfColumnVector<ColumnVectorCudfColumn>(
          schema[i], std::move(key_cols[i])));
    }
  }

  /// Pack aggregation columns.
  {
    for (size_t i = 0; i < result_aggs.size(); i++) {
      auto casted =
          cast(std::move(result_aggs[i].results[0]), aggregations[i].data_type);
      results.emplace_back(createCudfColumnVector<ColumnVectorCudfColumn>(
          aggregations[i].data_type, std::move(casted)));
    }
  }

  return std::make_shared<Fragment>(std::move(results));
}

} // namespace detail
#else
// TODO: Rewrite these shit in a mature arrow fashion, i.e., array data visitors
// or so.
using cura::data::createArrowColumnVector;

struct AppendScalarTypeVisitor : public arrow::TypeVisitor {
  explicit AppendScalarTypeVisitor(arrow::ArrayBuilder &builder_)
      : builder(builder_) {}

  template <typename ScalarType>
  struct AppendBuilderTypeVisitor : public arrow::TypeVisitor {
    explicit AppendBuilderTypeVisitor(arrow::ArrayBuilder &builder_,
                                      std::shared_ptr<ScalarType> scalar_)
        : builder(builder_), scalar(scalar_) {}

    template <typename BuilderType,
              typename CorrespondingScalarType = typename arrow::TypeTraits<
                  typename BuilderType::TypeClass>::ScalarType>
    auto Append() {
      auto b = dynamic_cast<BuilderType *>(&builder);
      CURA_ASSERT(b, "Cast to concrete builder failed");
      if (scalar->is_valid) {
        auto casted = CURA_GET_ARROW_RESULT(arrow::compute::Cast(
            std::static_pointer_cast<arrow::Scalar>(scalar), builder.type()));
        return b->Append(
            std::dynamic_pointer_cast<CorrespondingScalarType>(casted.scalar())
                ->value);
      } else {
        return b->AppendNull();
      }
    }

    arrow::Status Visit(const arrow::BooleanType &type) {
      return Append<arrow::BooleanBuilder>();
    }

    arrow::Status Visit(const arrow::Int8Type &type) {
      return Append<arrow::Int8Builder>();
    }

    arrow::Status Visit(const arrow::Int16Type &type) {
      return Append<arrow::Int16Builder>();
    }

    arrow::Status Visit(const arrow::Int32Type &type) {
      return Append<arrow::Int32Builder>();
    }

    arrow::Status Visit(const arrow::Int64Type &type) {
      return Append<arrow::Int64Builder>();
    }

    arrow::Status Visit(const arrow::UInt8Type &type) {
      return Append<arrow::UInt8Builder>();
    }

    arrow::Status Visit(const arrow::UInt16Type &type) {
      return Append<arrow::UInt16Builder>();
    }

    arrow::Status Visit(const arrow::UInt32Type &type) {
      return Append<arrow::UInt32Builder>();
    }

    arrow::Status Visit(const arrow::UInt64Type &type) {
      return Append<arrow::UInt64Builder>();
    }

    arrow::Status Visit(const arrow::FloatType &type) {
      return Append<arrow::FloatBuilder>();
    }

    arrow::Status Visit(const arrow::DoubleType &type) {
      return Append<arrow::DoubleBuilder>();
    }

    arrow::Status Visit(const arrow::Date32Type &type) {
      return Append<arrow::Date32Builder>();
    }

    arrow::Status Visit(const arrow::TimestampType &type) {
      return Append<arrow::TimestampBuilder>();
    }

    arrow::Status Visit(const arrow::DurationType &type) {
      return Append<arrow::DurationBuilder>();
    }

    template <typename T> arrow::Status Visit(const T &type) {
      return arrow::Status::NotImplemented(
          "AppendBuilderTypeVisitor not implemented for " +
          builder.type()->ToString() + ": " + scalar->ToString());
    }

    arrow::ArrayBuilder &builder;
    std::shared_ptr<ScalarType> scalar;
  };

  template <typename ScalarType,
            std::enable_if_t<!std::is_same_v<ScalarType, arrow::StringScalar>>
                * = nullptr>
  auto Append() {
    AppendBuilderTypeVisitor visitor(
        builder, std::dynamic_pointer_cast<ScalarType>(scalar));
    return arrow::VisitTypeInline(*builder.type(), &visitor);
  }

  auto Append(std::shared_ptr<arrow::StringScalar> string_scalar) {
    auto b = dynamic_cast<arrow::StringBuilder *>(&builder);
    CURA_ASSERT(b, "Cast to concrete builder failed");
    if (scalar->is_valid) {
      return b->Append(
          static_cast<arrow::util::string_view>(*string_scalar->value));
    } else {
      return b->AppendNull();
    }
  }

  arrow::Status Visit(const arrow::BooleanType &type) {
    return Append<arrow::BooleanScalar>();
  }

  arrow::Status Visit(const arrow::Int8Type &type) {
    return Append<arrow::Int8Scalar>();
  }

  arrow::Status Visit(const arrow::Int16Type &type) {
    return Append<arrow::Int16Scalar>();
  }

  arrow::Status Visit(const arrow::Int32Type &type) {
    return Append<arrow::Int32Scalar>();
  }

  arrow::Status Visit(const arrow::Int64Type &type) {
    return Append<arrow::Int64Scalar>();
  }

  arrow::Status Visit(const arrow::UInt8Type &type) {
    return Append<arrow::UInt8Scalar>();
  }

  arrow::Status Visit(const arrow::UInt16Type &type) {
    return Append<arrow::UInt16Scalar>();
  }

  arrow::Status Visit(const arrow::UInt32Type &type) {
    return Append<arrow::UInt32Scalar>();
  }

  arrow::Status Visit(const arrow::UInt64Type &type) {
    return Append<arrow::UInt64Scalar>();
  }

  arrow::Status Visit(const arrow::FloatType &type) {
    return Append<arrow::FloatScalar>();
  }

  arrow::Status Visit(const arrow::DoubleType &type) {
    return Append<arrow::DoubleScalar>();
  }

  arrow::Status Visit(const arrow::StringType &type) {
    return Append(std::dynamic_pointer_cast<arrow::StringScalar>(scalar));
  }

  arrow::Status Visit(const arrow::Date32Type &type) {
    return Append<arrow::Date32Scalar>();
  }

  arrow::Status Visit(const arrow::TimestampType &type) {
    return Append<arrow::TimestampScalar>();
  }

  arrow::Status Visit(const arrow::DurationType &type) {
    return Append<arrow::DurationScalar>();
  }

  template <typename T> arrow::Status Visit(const T &type) {
    return arrow::Status::NotImplemented(
        "AppendScalarTypeVisitor not implemented for " +
        builder.type()->ToString() + ": " + scalar->ToString());
  }

  arrow::ArrayBuilder &builder;
  std::shared_ptr<arrow::Scalar> scalar;
};

std::shared_ptr<arrow::Scalar> dispatchAggregationOperatorColumnVector(
    const Context &ctx, std::shared_ptr<arrow::Array> column,
    const PhysicalAggregation &aggregation) {
  arrow::compute::ExecContext context(ctx.memory_resource->converge());
  switch (aggregation.op) {
  case AggregationOperator::SUM:
    return CURA_GET_ARROW_RESULT(arrow::compute::Sum(column, &context))
        .scalar();
  case AggregationOperator::MIN:
    return CURA_GET_ARROW_RESULT(
               arrow::compute::MinMax(
                   column, arrow::compute::MinMaxOptions::Defaults(), &context))
        .scalar_as<arrow::StructScalar>()
        .value[0];
  case AggregationOperator::MAX:
    return CURA_GET_ARROW_RESULT(
               arrow::compute::MinMax(
                   column, arrow::compute::MinMaxOptions::Defaults(), &context))
        .scalar_as<arrow::StructScalar>()
        .value[1];
  case AggregationOperator::COUNT_VALID:
    return CURA_GET_ARROW_RESULT(
               arrow::compute::Count(
                   column, arrow::compute::CountOptions::Defaults(), &context))
        .scalar();
  case AggregationOperator::COUNT_ALL:
    return std::make_shared<arrow::Int64Scalar>(column->length());
  case AggregationOperator::MEAN:
    return CURA_GET_ARROW_RESULT(arrow::compute::Mean(column, &context))
        .scalar();
  case AggregationOperator::NTH_ELEMENT:
    if (column->length() == 0) {
      return Literal(aggregation.data_type.type_id).arrow();
    }
    return CURA_GET_ARROW_RESULT(column->GetScalar(aggregation.n));
  default:
    CURA_FAIL("Unsupported aggregation operator: " +
              aggregationOperatorToString(aggregation.op));
  }
}

std::shared_ptr<arrow::Scalar>
dispatchAggregationOperatorColumnScalar(std::shared_ptr<arrow::Scalar> scalar,
                                        size_t size, AggregationOperator op) {
  switch (op) {
  case AggregationOperator::COUNT_VALID:
    if (scalar->is_valid) {
      return std::make_shared<arrow::Int64Scalar>(size);
    } else {
      return std::make_shared<arrow::Int64Scalar>(0);
    }
  case AggregationOperator::COUNT_ALL:
    return std::make_shared<arrow::Int64Scalar>(size);
  default:
    CURA_FAIL("Unsupported aggregation operator: " +
              aggregationOperatorToString(op));
  }
}

std::shared_ptr<Fragment>
doAggregateWithoutKey(const Context &ctx, const Schema &schema,
                      const std::vector<PhysicalAggregation> &aggregations,
                      std::shared_ptr<const Fragment> fragment) {
  std::vector<std::shared_ptr<const Column>> results;
  for (const auto &aggregation : aggregations) {
    const auto &column = fragment->column<Column>(aggregation.idx);
    auto scalar = [&]() {
      if (auto cv = std::dynamic_pointer_cast<const ColumnVector>(column); cv) {
        return dispatchAggregationOperatorColumnVector(ctx, cv->arrow(),
                                                       aggregation);
      } else if (auto cs =
                     std::dynamic_pointer_cast<const ColumnScalar>(column);
                 cs) {
        return dispatchAggregationOperatorColumnScalar(cs->arrow(), cs->size(),
                                                       aggregation.op);
      } else {
        CURA_FAIL("Neither column vector nor column scalar");
      }
    }();
    auto casted = CURA_GET_ARROW_RESULT(
        arrow::compute::Cast(scalar, aggregation.data_type.arrow()));
    auto array =
        CURA_GET_ARROW_RESULT(arrow::MakeArrayFromScalar(*casted.scalar(), 1));
    results.emplace_back(createArrowColumnVector(aggregation.data_type, array));
  }
  return std::make_shared<Fragment>(std::move(results));
}

std::shared_ptr<Fragment>
doAggregate(const Context &ctx, const Schema &schema,
            const std::vector<ColumnIdx> &keys,
            const std::vector<PhysicalAggregation> &aggregations,
            std::shared_ptr<const Fragment> fragment) {
  /// Shortcut for non-key aggregations.
  if (keys.empty()) {
    return doAggregateWithoutKey(ctx, schema, aggregations, fragment);
  }

  auto pool = ctx.memory_resource->converge();
  arrow::compute::ExecContext context(pool);

  using Indices = std::pair<std::unique_ptr<arrow::ArrayBuilder>,
                            std::shared_ptr<arrow::Array>>;
  using IndicesHashTable = std::unordered_map<Key, Indices, RowHash, RowEqual>;
  IndicesHashTable indices;
  std::vector<std::shared_ptr<const Column>> results;

  /// Build hash table for group keys and record the grouped indices.
  {
    auto rows = fragment->size();
    for (size_t i = 0; i < rows; i++) {
      Key key_values;
      for (auto key : keys) {
        const auto &key_col = fragment->column(key);
        const auto &value =
            CURA_GET_ARROW_RESULT(key_col->arrow()->GetScalar(i));
        key_values.emplace_back(value);
      }
      auto it = indices.find(key_values);
      if (it == indices.end()) {
        std::unique_ptr<arrow::ArrayBuilder> builder;
        CURA_ASSERT_ARROW_OK(
            arrow::MakeBuilder(pool, arrow::uint64(), &builder),
            "Create indices builder failed");
        std::tie(it, std::ignore) = indices.emplace(
            std::move(key_values), std::make_pair(std::move(builder), nullptr));
      }
      auto indices_builder =
          dynamic_cast<arrow::UInt64Builder *>(it->second.first.get());
      CURA_ASSERT_ARROW_OK(indices_builder->Append(i), "Append index failed");
    }
    for (auto &entry : indices) {
      CURA_ASSERT_ARROW_OK(entry.second.first->Finish(&entry.second.second),
                           "Finish indices failed");
    }
  }

  /// Gather group columns based on groups.
  {
    for (size_t i = 0; i < keys.size(); i++) {
      std::unique_ptr<arrow::ArrayBuilder> builder;
      CURA_ASSERT_ARROW_OK(
          arrow::MakeBuilder(pool, schema[i].arrow(), &builder),
          "Create group builder failed");
      AppendScalarTypeVisitor visitor(*builder);
      for (const auto &entry : indices) {
        visitor.scalar = entry.first[i];
        CURA_ASSERT_ARROW_OK(
            arrow::VisitTypeInline(*entry.first[i]->type, &visitor),
            "Gather group scalar failed");
      }
      std::shared_ptr<arrow::Array> array;
      CURA_ASSERT_ARROW_OK(builder->Finish(&array), "Finish group failed");
      results.emplace_back(createArrowColumnVector(schema[i], array));
    }
  }

  /// Gather aggregation columns based on grouped indices.
  {
    for (const auto &aggregation : aggregations) {
      std::unique_ptr<arrow::ArrayBuilder> builder;
      CURA_ASSERT_ARROW_OK(
          arrow::MakeBuilder(pool, aggregation.data_type.arrow(), &builder),
          "Create aggregation builder failed");
      AppendScalarTypeVisitor visitor(*builder);
      for (const auto &entry : indices) {
        const auto &datum = CURA_GET_ARROW_RESULT(arrow::compute::Take(
            fragment->column(aggregation.idx)->arrow(), entry.second.second,
            arrow::compute::TakeOptions::Defaults(), &context));
        const auto &array = datum.make_array();
        auto scalar =
            dispatchAggregationOperatorColumnVector(ctx, array, aggregation);
        visitor.scalar = scalar;
        CURA_ASSERT_ARROW_OK(arrow::VisitTypeInline(*scalar->type, &visitor),
                             "Gather aggregation scalar failed");
      }
      std::shared_ptr<arrow::Array> array;
      CURA_ASSERT_ARROW_OK(builder->Finish(&array),
                           "Finish aggregation failed");
      results.emplace_back(
          createArrowColumnVector(aggregation.data_type, array));
    }
  }

  return std::make_shared<Fragment>(std::move(results));
}

} // namespace detail
#endif