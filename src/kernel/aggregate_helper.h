#pragma once

#include "cura/kernel/aggregate.h"

namespace cura::kernel::detail {

std::shared_ptr<Fragment>
doAggregate(const Context &ctx, const Schema &schema,
            const std::vector<ColumnIdx> &keys,
            const std::vector<PhysicalAggregation> &aggregations,
            std::shared_ptr<const Fragment> fragment);

}
