#include "abstract_rule.hpp"

#include <memory>

#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"

namespace opossum {

void AbstractRule::_apply_to_inputs(std::shared_ptr<AbstractLQPNode> node, const std::shared_ptr<AbstractCostEstimator>& cost_estimator) const {  // NOLINT
  if (node->left_input()) apply_to(node->left_input(), cost_estimator);
  if (node->right_input()) apply_to(node->right_input(), cost_estimator);
}

}  // namespace opossum
