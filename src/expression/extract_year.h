#pragma once

#include "cura/data/column_vector.h"
#include "cura/execution/context.h"
#include "cura/type/data_type.h"

namespace cura::expression::detail {

using cura::data::Column;
using cura::data::ColumnVector;
using cura::execution::Context;
using cura::type::DataType;

constexpr uint64_t YEAR_BIT_FIELD_OFFSET = 50;
constexpr uint64_t YEAR_BIT_FIELD_WIDTH = 14;
constexpr uint64_t YEAR_BIT_FIELD_MASK = ((1ULL << YEAR_BIT_FIELD_WIDTH) - 1ULL)
                                         << YEAR_BIT_FIELD_OFFSET;

std::shared_ptr<const Column>
extractYear(const Context &ctx, ThreadId thread_id,
            std::shared_ptr<const ColumnVector> cv,
            const DataType &result_type);

} // namespace cura::expression::detail