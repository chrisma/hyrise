#pragma once

#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <boost/container/scoped_allocator.hpp>
#include <boost/functional/hash.hpp>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "abstract_aggregate_operator.hpp"
#include "expression/aggregate_expression.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment_visitor.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/*
Operator to aggregate columns sort-based by certain functions, such as min, max, sum, average, and count. The output is a table
 with reference segments. As with most operators we do not guarantee a stable operation with regards to positions -
 i.e. your sorting order.

For implementation details, please check the wiki: https://github.com/hyrise/hyrise/wiki/Aggregate-Operator
*/

/**
 * Note: Aggregate does not support null values at the moment
 */
class AggregateSort : public AbstractAggregateOperator {
 public:
  AggregateSort(const std::shared_ptr<AbstractOperator>& in, const std::vector<AggregateColumnDefinition>& aggregates,
            const std::vector<ColumnID>& groupby_column_ids);

  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;

  // write the aggregated output for a given aggregate column
  //template <typename ColumnType, AggregateFunction function, typename AggregateKey>
  //void write_aggregate_output(ColumnID column_index);

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
          const std::shared_ptr<AbstractOperator>& copied_input_left,
          const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;
};

}  // namespace opossum