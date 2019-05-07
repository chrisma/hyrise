#include "mock_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "expression/lqp_column_expression.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

MockNode::MockNode(const ColumnDefinitions& column_definitions, const std::optional<std::string>& name)
    : AbstractLQPNode(LQPNodeType::Mock), name(name), _column_definitions(column_definitions) {}

LQPColumnReference MockNode::get_column(const std::string& column_name) const {
  const auto& column_definitions = this->column_definitions();

  for (auto column_id = ColumnID{0}; column_id < column_definitions.size(); ++column_id) {
    if (column_definitions[column_id].second == column_name) return LQPColumnReference{shared_from_this(), column_id};
  }

  Fail("Couldn't find column named '"s + column_name + "' in MockNode");
}

const MockNode::ColumnDefinitions& MockNode::column_definitions() const { return _column_definitions; }

const std::vector<std::shared_ptr<AbstractExpression>>& MockNode::column_expressions() const {
  // Need to initialize the expressions lazily because they will have a weak_ptr to this node and we can't obtain that
  // in the constructor
  if (!_column_expressions) {
    _column_expressions.emplace(_column_definitions.size() - _excluded_column_ids.size());

    auto excluded_column_ids_iter = _excluded_column_ids.begin();

    for (auto stored_column_id = ColumnID{0}, output_column_id = ColumnID{0}; stored_column_id < _column_definitions.size(); ++stored_column_id) {
      if (excluded_column_ids_iter != _excluded_column_ids.end() && stored_column_id == *excluded_column_ids_iter) {
        ++excluded_column_ids_iter;
        continue;
      }

      (*_column_expressions)[output_column_id] =
      std::make_shared<LQPColumnExpression>(LQPColumnReference{shared_from_this(), stored_column_id});
      ++output_column_id;
    }
  }

  return *_column_expressions;
}

bool MockNode::is_column_nullable(const ColumnID column_id) const {
  Assert(column_id < _column_definitions.size(), "ColumnID out of range");
  return false;
}

void MockNode::set_excluded_column_ids(const std::vector<ColumnID>& column_ids) {
  _excluded_column_ids = column_ids;
  std::sort(_excluded_column_ids.begin(), _excluded_column_ids.end());
  const auto last = std::unique(_excluded_column_ids.begin(), _excluded_column_ids.end());
  _excluded_column_ids.erase(last,  _excluded_column_ids.end());
  _column_expressions.reset();
}

const std::vector<ColumnID>& MockNode::excluded_column_ids() const {
  return _excluded_column_ids;
}

std::string MockNode::description() const {
  std::ostringstream stream;
  stream << "[MockNode '"s << name.value_or("Unnamed") << "']";

  if (!_excluded_column_ids.empty()) {
    stream << " Excluded columns: " << _excluded_column_ids.size() << "/" << _column_definitions.size();
  }

  return stream.str();
}

std::shared_ptr<TableStatistics> MockNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  Assert(_table_statistics, "MockNode statistics need to be explicitely set");

  if (_excluded_column_ids.empty()) {
    return _table_statistics;
  }

  // Prune _excluded_column_ids from the statistics
  auto column_statistics = std::vector<std::shared_ptr<const BaseColumnStatistics>>{_table_statistics->column_statistics().size() - _excluded_column_ids.size()};

  auto excluded_column_ids_iter = _excluded_column_ids.begin();

  for (auto stored_column_id = ColumnID{0}, output_column_id = ColumnID{0}; stored_column_id < _table_statistics->column_statistics().size(); ++stored_column_id) {
    if (excluded_column_ids_iter != _excluded_column_ids.end() && stored_column_id == *excluded_column_ids_iter) {
      ++excluded_column_ids_iter;
      continue;
    }

    column_statistics[output_column_id] = _table_statistics->column_statistics()[stored_column_id];
    ++output_column_id;
  }

  return std::make_shared<TableStatistics>(_table_statistics->table_type(), _table_statistics->row_count(), column_statistics);
}

void MockNode::set_statistics(const std::shared_ptr<TableStatistics>& statistics) { _table_statistics = statistics; }

std::shared_ptr<AbstractLQPNode> MockNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  const auto mock_node = MockNode::make(_column_definitions);
  mock_node->set_statistics(_table_statistics);
  return mock_node;
}

bool MockNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& mock_node = static_cast<const MockNode&>(rhs);
  return _column_definitions == mock_node._column_definitions && _table_statistics == mock_node._table_statistics;
}

}  // namespace opossum
