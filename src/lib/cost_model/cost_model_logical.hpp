#pragma once

#include "abstract_cost_estimator.hpp"

namespace opossum {

class AbstractExpression;

/**
 * Cost model for logical complexity, i.e., approximate number of tuple accesses
 */
class CostModelLogical : public AbstractCostEstimator {
 public:
  using AbstractCostEstimator::AbstractCostEstimator;

  Cost estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node, const std::shared_ptr<CostEstimationCache>& cost_estimation_cache = {},
                          const std::shared_ptr<CardinalityEstimationCache>& cardinality_estimation_cache = {}) const override;

 private:
  static float _get_expression_cost_multiplier(const std::shared_ptr<AbstractExpression>& expression);
};

}  // namespace opossum
