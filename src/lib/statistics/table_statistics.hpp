#pragma once

#include <atomic>
#include <iostream>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "all_type_variant.hpp"

namespace opossum {

class BaseColumnStatistics;
class Table;

/**
 * Container for all cardinality estimation statistics gathered about a Table. Also used to represent the estimation of
 * a temporary Table during Optimization.
 */
class TableStatistics {
 public:
  /**
   * Generates histograms for all columns
   */
  static std::shared_ptr<TableStatistics> from_table(const Table& table);

  TableStatistics(std::vector<std::shared_ptr<BaseColumnStatistics>>&& column_statistics, const Cardinality row_count);

  /**
   * @return column_statistics[column_id]->data_type
   */
  DataType column_data_type(const ColumnID column_id) const;

  const std::vector<std::shared_ptr<BaseColumnStatistics>> column_statistics;
  Cardinality row_count;
};

std::ostream& operator<<(std::ostream& stream, const TableStatistics& table_statistics);

}  // namespace opossum
