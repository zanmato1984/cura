#include "cura/kernel/sort.h"
#include "cura/data/column_vector.h"
#include "cura/type/data_type.h"
#include "helper.h"

#include <arrow/compute/api.h>
#include <arrow/visitor_inline.h>
#include <map>

#ifdef USE_CUDF
#include <cudf/sorting.hpp>
#endif

namespace cura::kernel {

using cura::relational::SortInfo;

void Sort::push(const Context &ctx, ThreadId thread_id, KernelId upstream,
                std::shared_ptr<const Fragment> fragment) const {
  std::lock_guard<std::mutex> lock(push_mutex);
  pushed_fragments.emplace_back(fragment);
}

#ifdef USE_CUDF
namespace detail {

std::shared_ptr<Fragment>
doSort(const Context &ctx, const Schema &schema,
       const std::vector<PhysicalSortInfo> &sort_infos,
       std::shared_ptr<const Fragment> fragment) {
  std::vector<cudf::column_view> key_columns(sort_infos.size());
  std::transform(sort_infos.begin(), sort_infos.end(), key_columns.begin(),
                 [&](const auto &sort_info) {
                   return fragment->column(sort_info.idx)->cudf();
                 });
  std::vector<cudf::order> column_orders(sort_infos.size());
  std::transform(sort_infos.begin(), sort_infos.end(), column_orders.begin(),
                 [](const auto &sort_info) {
                   return sort_info.order == SortInfo::Order::ASCENDING
                              ? cudf::order::ASCENDING
                              : cudf::order::DESCENDING;
                 });
  std::vector<cudf::null_order> null_orders(sort_infos.size());
  std::transform(sort_infos.begin(), sort_infos.end(), null_orders.begin(),
                 [](const auto &sort_info) {
                   return sort_info.null_order == SortInfo::NullOrder::FIRST
                              ? cudf::null_order::BEFORE
                              : cudf::null_order::AFTER;
                 });

  cudf::table_view key_table(key_columns);
  auto result = cudf::sort_by_key(fragment->cudf(), key_table, column_orders,
                                  null_orders, ctx.memory_resource->converge());

  return std::make_shared<Fragment>(schema, std::move(result));
}

} // namespace detail
#else
namespace detail {

struct SortTypeVisitor : public arrow::TypeVisitor {
  SortTypeVisitor(bool desc_, bool null_last_,
                  std::shared_ptr<arrow::Scalar> left_,
                  std::shared_ptr<arrow::Scalar> right_)
      : desc(desc_), null_last(null_last_), left(left_), right(right_) {}

  template <typename T,
            typename arrow::enable_if_primitive_ctype<T> * = nullptr>
  arrow::Status Visit(const T &type) {
    using ScalarType = typename arrow::TypeTraits<T>::ScalarType;
    auto l = std::dynamic_pointer_cast<ScalarType>(left);
    auto r = std::dynamic_pointer_cast<ScalarType>(right);
    if (!l->is_valid && !r->is_valid) {
      result = 0;
      return arrow::Status::OK();
    }
    if (!l->is_valid) {
      result = null_last ? 1 : -1;
      return arrow::Status::OK();
    }
    if (!r->is_valid) {
      result = null_last ? -1 : 1;
      return arrow::Status::OK();
    }
    result = desc ? r->value - l->value : l->value - r->value;
    return arrow::Status::OK();
  }

  template <typename T,
            std::enable_if_t<std::is_same_v<T, arrow::StringType>> * = nullptr>
  arrow::Status Visit(const T &type) {
    auto l = std::dynamic_pointer_cast<arrow::StringScalar>(left);
    auto r = std::dynamic_pointer_cast<arrow::StringScalar>(right);
    if (!l->is_valid && !r->is_valid) {
      result = 0;
      return arrow::Status::OK();
    }
    if (!l->is_valid) {
      result = null_last ? 1 : -1;
      return arrow::Status::OK();
    }
    if (!r->is_valid) {
      result = null_last ? -1 : 1;
      return arrow::Status::OK();
    }
    result = desc ? std::strcmp(r->value->ToString().data(),
                                l->value->ToString().data())
                  : std::strcmp(l->value->ToString().data(),
                                r->value->ToString().data());
    return arrow::Status::OK();
  }

  template <typename T, typename std::enable_if_t<
                            !arrow::is_primitive_ctype<T>::value &&
                            !std::is_same_v<T, arrow::StringType>> * = nullptr>
  arrow::Status Visit(const T &type) {
    CURA_FAIL("Unsupported arrow scalar");
  }

  bool desc;
  bool null_last;
  std::shared_ptr<arrow::Scalar> left, right;
  int result;
};

struct RowCompare {
  bool operator()(const Key &l, const Key &r) const {
    CURA_ASSERT(l.size() == r.size(), "Mismatched type");
    for (size_t i = 0; i < l.size(); i++) {
      CURA_ASSERT(l[i]->type->Equals(r[i]->type), "Mismatched type");
      SortTypeVisitor visitor(desc[i], null_last[i], l[i], r[i]);
      CURA_ASSERT_ARROW_OK(arrow::VisitTypeInline(*l[i]->type, &visitor),
                           "Compare row failed");
      if (visitor.result < 0) {
        return true;
      } else if (visitor.result > 0) {
        return false;
      }
    }
    return false;
  }

  std::vector<bool> desc;
  std::vector<bool> null_last;
};

using Map = std::multimap<Key, size_t, RowCompare>;

std::shared_ptr<Fragment>
doSort(const Context &ctx, const Schema &schema,
       const std::vector<PhysicalSortInfo> &sort_infos,
       std::shared_ptr<const Fragment> fragment) {
  RowCompare row_cmp{std::vector<bool>(sort_infos.size()),
                     std::vector<bool>(sort_infos.size())};
  std::transform(sort_infos.begin(), sort_infos.end(), row_cmp.desc.begin(),
                 [](const auto &sort_info) {
                   return sort_info.order == SortInfo::Order::DESCENDING;
                 });
  std::vector<bool> null_last(sort_infos.size());
  std::transform(sort_infos.begin(), sort_infos.end(),
                 row_cmp.null_last.begin(), [](const auto &sort_info) {
                   return sort_info.null_order == SortInfo::NullOrder::LAST;
                 });
  detail::Map map(row_cmp);

  /// Sort based on keys and store indices of sorted rows.
  for (size_t row = 0; row < fragment->size(); row++) {
    std::vector<std::shared_ptr<arrow::Scalar>> key(sort_infos.size());
    std::transform(
        sort_infos.begin(), sort_infos.end(), key.begin(),
        [&](const auto &sort_info) {
          return CURA_GET_ARROW_RESULT(
              fragment->column(sort_info.idx)->arrow()->GetScalar(row));
        });
    map.emplace(std::move(key), row);
  }

  /// Gather fragment based on indices.
  std::shared_ptr<arrow::Array> indices;
  auto pool = ctx.memory_resource->converge();
  arrow::compute::ExecContext context(pool);
  {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    CURA_ASSERT_ARROW_OK(arrow::MakeBuilder(pool, arrow::uint64(), &builder),
                         "Create indices builder failed");
    auto indices_builder = dynamic_cast<arrow::UInt64Builder *>(builder.get());
    CURA_ASSERT(indices_builder, "Dynamic cast of indices builder failed");

    for (const auto &entry : map) {
      CURA_ASSERT_ARROW_OK(indices_builder->Append(entry.second),
                           "Append indices failed");
    }

    CURA_ASSERT_ARROW_OK(builder->Finish(&indices), "Finish indices failed");
  }
  const auto &datum = CURA_GET_ARROW_RESULT(
      arrow::compute::Take(fragment->arrow(), indices,
                           arrow::compute::TakeOptions::Defaults(), &context));

  return std::make_shared<Fragment>(datum.record_batch());
}

} // namespace detail
#endif

void Sort::concatenate(const Context &ctx) const {
  if (pushed_fragments.empty()) {
    return;
  }

  auto fragments = std::move(pushed_fragments);
  concatenated_fragment = detail::concatFragments(
      ctx.memory_resource->concatenate(), schema, fragments);
}

void Sort::converge(const Context &ctx) const {
  if (!concatenated_fragment) {
    return;
  }

  auto concatenated = std::move(concatenated_fragment);

  converged_fragment = detail::doSort(ctx, schema, sort_infos, concatenated);
}

} // namespace cura::kernel
