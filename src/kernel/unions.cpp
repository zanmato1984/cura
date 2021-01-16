#include "cura/kernel/unions.h"
#include "cura/data/column_factories.h"
#include "cura/data/column_vector.h"
#include "cura/data/fragment.h"
#include "helper.h"

#include <arrow/visitor.h>
#include <numeric>
#include <unordered_set>

#ifdef USE_CUDF
#include <cudf/concatenate.hpp>
#include <cudf/stream_compaction.hpp>
#endif

namespace cura::kernel {

using cura::data::Column;
using cura::data::ColumnVector;
using cura::data::createArrowColumnVector;
using cura::type::DataType;
using cura::type::Schema;

void Union::push(const Context &ctx, ThreadId thread_id, KernelId upstream,
                 std::shared_ptr<const Fragment> fragment) const {
  std::lock_guard<std::mutex> lock(push_mutex);
  pushed_fragments.emplace_back(fragment);
}

#ifdef USE_CUDF
namespace detail {

std::shared_ptr<Fragment> doUnion(const Context &ctx, const Schema &schema,
                                  std::shared_ptr<const Fragment> fragment) {
  auto table = fragment->cudf();

  std::vector<cudf::size_type> keys(table.num_columns());
  std::iota(keys.begin(), keys.end(), 0);
  auto dedup = cudf::drop_duplicates(
      table, keys, cudf::duplicate_keep_option::KEEP_FIRST,
      cudf::null_equality::EQUAL, ctx.memory_resource->converge());

  return std::make_shared<Fragment>(schema, std::move(dedup));
}

} // namespace detail
#else
// TODO: Rewrite these shit in a mature arrow fashion, i.e., array data visitors
// or so.
namespace detail {

using Set = std::unordered_set<Key, RowHash, RowEqual>;

struct UnionTypeVisitor : public arrow::TypeVisitor {
  template <
      typename Type, typename BuilderType, typename ScalarType,
      std::enable_if_t<!std::is_same_v<Type, arrow::StringType>> * = nullptr>
  auto Append(BuilderType *builder, ScalarType *scalar) {
    return builder->Append(scalar->value);
  }

  template <
      typename Type, typename BuilderType, typename ScalarType,
      std::enable_if_t<std::is_same_v<Type, arrow::StringType>> * = nullptr>
  auto Append(BuilderType *builder, ScalarType *scalar) {
    return builder->Append(
        static_cast<arrow::util::string_view>(*scalar->value));
  }

  template <typename Type, typename BuilderType, typename ScalarType>
  arrow::Status Visit(const Type &type) {
    auto pool = arrow::default_memory_pool();
    std::unique_ptr<arrow::ArrayBuilder> builder;
    {
      CURA_ASSERT_ARROW_OK(
          arrow::MakeBuilder(pool, data_type.arrow(), &builder),
          "Creating arrow column builder failed");
    }
    auto type_builder = dynamic_cast<BuilderType *>(builder.get());
    CURA_ASSERT(type_builder, "Cast to concrete builder failed");
    for (const auto &row : set) {
      auto scalar = row.at(column_id);
      auto type_scalar = std::dynamic_pointer_cast<ScalarType>(scalar);
      if (type_scalar->is_valid) {
        CURA_ASSERT_ARROW_OK((Append<Type, BuilderType, ScalarType>(
                                 type_builder, type_scalar.get())),
                             "Append scalar failed");
      } else {
        CURA_ASSERT_ARROW_OK(type_builder->AppendNull(),
                             "Append scalar failed");
      }
    }
    std::shared_ptr<arrow::Array> array;
    CURA_ASSERT_ARROW_OK(type_builder->Finish(&array),
                         "Build arrow array failed");
    cv = createArrowColumnVector(data_type, array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanType &type) override {
    return Visit<arrow::BooleanType, arrow::BooleanBuilder,
                 arrow::BooleanScalar>(type);
  }

  arrow::Status Visit(const arrow::Int8Type &type) override {
    return Visit<arrow::Int8Type, arrow::Int8Builder, arrow::Int8Scalar>(type);
  }

  arrow::Status Visit(const arrow::Int16Type &type) override {
    return Visit<arrow::Int16Type, arrow::Int16Builder, arrow::Int16Scalar>(
        type);
  }

  arrow::Status Visit(const arrow::Int32Type &type) override {
    return Visit<arrow::Int32Type, arrow::Int32Builder, arrow::Int32Scalar>(
        type);
  }

  arrow::Status Visit(const arrow::Int64Type &type) override {
    return Visit<arrow::Int64Type, arrow::Int64Builder, arrow::Int64Scalar>(
        type);
  }

  arrow::Status Visit(const arrow::UInt8Type &type) override {
    return Visit<arrow::UInt8Type, arrow::UInt8Builder, arrow::UInt8Scalar>(
        type);
  }

  arrow::Status Visit(const arrow::UInt16Type &type) override {
    return Visit<arrow::UInt16Type, arrow::UInt16Builder, arrow::UInt16Scalar>(
        type);
  }

  arrow::Status Visit(const arrow::UInt32Type &type) override {
    return Visit<arrow::UInt32Type, arrow::UInt32Builder, arrow::UInt32Scalar>(
        type);
  }

  arrow::Status Visit(const arrow::UInt64Type &type) override {
    return Visit<arrow::UInt64Type, arrow::UInt64Builder, arrow::UInt64Scalar>(
        type);
  }

  arrow::Status Visit(const arrow::FloatType &type) override {
    return Visit<arrow::FloatType, arrow::FloatBuilder, arrow::FloatScalar>(
        type);
  }

  arrow::Status Visit(const arrow::DoubleType &type) override {
    return Visit<arrow::DoubleType, arrow::DoubleBuilder, arrow::DoubleScalar>(
        type);
  }

  arrow::Status Visit(const arrow::StringType &type) override {
    return Visit<arrow::StringType, arrow::StringBuilder, arrow::StringScalar>(
        type);
  }

  arrow::Status Visit(const arrow::Date32Type &type) override {
    return Visit<arrow::Date32Type, arrow::Date32Builder, arrow::Date32Scalar>(
        type);
  }

  arrow::Status Visit(const arrow::TimestampType &type) override {
    return Visit<arrow::TimestampType, arrow::TimestampBuilder,
                 arrow::TimestampScalar>(type);
  }

  arrow::Status Visit(const arrow::DurationType &type) override {
    return Visit<arrow::DurationType, arrow::DurationBuilder,
                 arrow::DurationScalar>(type);
  }

  explicit UnionTypeVisitor(DataType data_type_, const Set &set_,
                            size_t column_id_)
      : data_type(std::move(data_type_)), set(set_), column_id(column_id_) {}

  DataType data_type;
  const Set &set;
  size_t column_id;

  std::shared_ptr<ColumnVector> cv;
};

std::shared_ptr<Fragment> doUnion(const Context &ctx, const Schema &schema,
                                  std::shared_ptr<const Fragment> fragment) {
  detail::Set set;

  for (size_t row = 0; row < fragment->size(); row++) {
    std::vector<std::shared_ptr<arrow::Scalar>> key;
    for (size_t i = 0; i < fragment->numColumns(); i++) {
      const auto &scalar =
          CURA_GET_ARROW_RESULT(fragment->column(i)->arrow()->GetScalar(row));
      key.emplace_back(scalar);
    }
    set.emplace(std::move(key));
  }

  std::vector<std::shared_ptr<const Column>> converged_columns;
  for (size_t i = 0; i < fragment->numColumns(); i++) {
    const auto &data_type = fragment->column(i)->dataType();
    UnionTypeVisitor visitor(data_type, set, i);
    CURA_ASSERT_ARROW_OK(data_type.arrow()->Accept(&visitor),
                         "Copy row from set failed");
    converged_columns.emplace_back(std::move(visitor.cv));
  }

  return std::make_shared<Fragment>(std::move(converged_columns));
}

} // namespace detail
#endif

void Union::concatenate(const Context &ctx) const {
  if (pushed_fragments.empty()) {
    return;
  }

  auto fragments = std::move(pushed_fragments);
  concatenated_fragment = detail::concatFragments(
      ctx.memory_resource->concatenate(), schema, fragments);
}

void Union::converge(const Context &ctx) const {
  if (!concatenated_fragment) {
    return;
  }

  auto concatenated = std::move(concatenated_fragment);

  converged_fragment = detail::doUnion(ctx, schema, concatenated);
}

std::shared_ptr<const Fragment>
UnionAll::streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
                     std::shared_ptr<const Fragment> fragment) const {
  return fragment;
}

} // namespace cura::kernel
