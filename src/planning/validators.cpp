#include "validators.h"
#include "cura/expression/expression_visitor.h"

namespace cura::planning {

using cura::expression::ColumnRef;
using cura::expression::Expression;
using cura::expression::ExpressionVisitor;
using cura::relational::Schema;

namespace detail {

struct ColumnRefCollector
    : public ExpressionVisitor<ColumnRefCollector,
                               std::vector<std::shared_ptr<const ColumnRef>>> {
  std::vector<std::shared_ptr<const ColumnRef>>
  visitColumnRef(const std::shared_ptr<const ColumnRef> &column_ref) {
    return {column_ref};
  }

  std::vector<std::shared_ptr<const ColumnRef>>
  defaultVisit(const std::shared_ptr<const Expression> &,
               const std::vector<std::vector<std::shared_ptr<const ColumnRef>>>
                   &children) {
    std::vector<std::shared_ptr<const ColumnRef>> column_refs;
    for (auto &child : children) {
      column_refs.insert(column_refs.end(),
                         std::make_move_iterator(child.begin()),
                         std::make_move_iterator(child.end()));
    }
    return column_refs;
  }
};

void validateExpression(const Schema &input_schema,
                        const std::shared_ptr<const Expression> &expression) {
  auto column_refs = ColumnRefCollector().visit(expression);
  std::for_each(column_refs.begin(), column_refs.end(),
                [&input_schema](const auto &column_ref) {
                  CURA_ASSERT(column_ref->columnIdx() < input_schema.size(),
                              "Column ref out of bound");
                  CURA_ASSERT(column_ref->dataType() ==
                                  input_schema[column_ref->columnIdx()],
                              "Column ref data type mismatch with column");
                });
}

} // namespace detail

void ColumnRefValidator::visitFilter(
    const std::shared_ptr<const RelFilter> &filter) {
  detail::validateExpression(filter->inputs[0]->output(), filter->condition());
}

void ColumnRefValidator::visitHashJoin(
    const std::shared_ptr<const RelHashJoin> &hash_join) {
  auto input_schema = hash_join->left()->output();
  input_schema.insert(input_schema.end(), hash_join->right()->output().begin(),
                      hash_join->right()->output().end());
  detail::validateExpression(input_schema, hash_join->condition());
}

void ColumnRefValidator::visitProject(
    const std::shared_ptr<const RelProject> &project) {
  std::for_each(project->expressions().begin(), project->expressions().end(),
                [&](const auto &e) {
                  detail::validateExpression(project->inputs[0]->output(), e);
                });
}

void ColumnRefValidator::visitAggregate(
    const std::shared_ptr<const RelAggregate> &aggregate) {
  std::for_each(aggregate->groups().begin(), aggregate->groups().end(),
                [&](const auto &e) {
                  detail::validateExpression(aggregate->inputs[0]->output(), e);
                });
  std::for_each(aggregate->aggregations().begin(),
                aggregate->aggregations().end(), [&](const auto &e) {
                  detail::validateExpression(aggregate->inputs[0]->output(), e);
                });
}

void ColumnRefValidator::visitSort(const std::shared_ptr<const RelSort> &sort) {
  std::for_each(sort->sortInfos().begin(), sort->sortInfos().end(),
                [&](const auto &sort_info) {
                  detail::validateExpression(sort->inputs[0]->output(),
                                             sort_info.expression);
                });
}

} // namespace cura::planning
