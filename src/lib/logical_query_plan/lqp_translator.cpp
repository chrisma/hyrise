#include "lqp_translator.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "aggregate_node.hpp"
#include "alias_node.hpp"
#include "create_prepared_plan_node.hpp"
#include "create_table_node.hpp"
#include "create_view_node.hpp"
#include "delete_node.hpp"
#include "drop_table_node.hpp"
#include "drop_view_node.hpp"
#include "dummy_table_node.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/value_expression.hpp"
#include "insert_node.hpp"
#include "join_node.hpp"
#include "limit_node.hpp"
#include "operators/aggregate.hpp"
#include "operators/alias_operator.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/index_scan.hpp"
#include "operators/insert.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_mpsm.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/limit.hpp"
#include "operators/maintenance/create_prepared_plan.hpp"
#include "operators/maintenance/create_table.hpp"
#include "operators/maintenance/create_view.hpp"
#include "operators/maintenance/drop_table.hpp"
#include "operators/maintenance/drop_view.hpp"
#include "operators/maintenance/show_columns.hpp"
#include "operators/maintenance/show_tables.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "operators/product.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_positions.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "predicate_node.hpp"
#include "projection_node.hpp"
#include "show_columns_node.hpp"
#include "sort_node.hpp"
#include "storage/storage_manager.hpp"
#include "stored_table_node.hpp"
#include "union_node.hpp"
#include "update_node.hpp"
#include "validate_node.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

std::shared_ptr<AbstractOperator> LQPTranslator::translate_node(const std::shared_ptr<AbstractLQPNode>& node) const {
  /**
   * Translate a node (i.e. call `_translate_by_node_type`) only if it hasn't been translated before, otherwise just
   * retrieve it from cache
   *
   * Without this caching, translating this kind of LQP
   *
   *    _____union____
   *   /              \
   *  predicate_a     predicate_b
   *  \                /
   *   \__predicate_c_/
   *          |
   *     table_int_float2
   *
   * would result in multiple operators created from predicate_c and thus in performance drops
   */

  const auto operator_iter = _operator_by_lqp_node.find(node);
  if (operator_iter != _operator_by_lqp_node.end()) {
    return operator_iter->second;
  }

  const auto pqp = _translate_by_node_type(node->type, node);
  _operator_by_lqp_node.emplace(node, pqp);
  return pqp;
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_by_node_type(
    LQPNodeType type, const std::shared_ptr<AbstractLQPNode>& node) const {
  switch (type) {
    // clang-format off
    case LQPNodeType::Alias:              return _translate_alias_node(node);
    case LQPNodeType::StoredTable:        return _translate_stored_table_node(node);
    case LQPNodeType::Predicate:          return _translate_predicate_node(node);
    case LQPNodeType::Projection:         return _translate_projection_node(node);
    case LQPNodeType::Sort:               return _translate_sort_node(node);
    case LQPNodeType::Join:               return _translate_join_node(node);
    case LQPNodeType::Aggregate:          return _translate_aggregate_node(node);
    case LQPNodeType::Limit:              return _translate_limit_node(node);
    case LQPNodeType::Insert:             return _translate_insert_node(node);
    case LQPNodeType::Delete:             return _translate_delete_node(node);
    case LQPNodeType::DummyTable:         return _translate_dummy_table_node(node);
    case LQPNodeType::Update:             return _translate_update_node(node);
    case LQPNodeType::Validate:           return _translate_validate_node(node);
    case LQPNodeType::Union:              return _translate_union_node(node);

      // Maintenance operators
    case LQPNodeType::ShowTables:         return _translate_show_tables_node(node);
    case LQPNodeType::ShowColumns:        return _translate_show_columns_node(node);
    case LQPNodeType::CreateView:         return _translate_create_view_node(node);
    case LQPNodeType::DropView:           return _translate_drop_view_node(node);
    case LQPNodeType::CreateTable:        return _translate_create_table_node(node);
    case LQPNodeType::DropTable:          return _translate_drop_table_node(node);
    case LQPNodeType::CreatePreparedPlan: return _translate_create_prepared_plan_node(node);
      // clang-format on

    default:
      Fail("Unknown node type encountered.");
  }
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_stored_table_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node);
  const auto get_table = std::make_shared<GetTable>(stored_table_node->table_name);
  get_table->set_excluded_chunk_ids(stored_table_node->excluded_chunk_ids());
  return get_table;
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_predicate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_node = node->left_input();
  const auto input_operator = translate_node(input_node);
  const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);

  switch (predicate_node->scan_type) {
    case ScanType::TableScan:
      return _translate_predicate_node_to_table_scan(predicate_node, input_operator);
    case ScanType::IndexScan:
      return _translate_predicate_node_to_index_scan(predicate_node, input_operator);
  }

  Fail("GCC thinks this is reachable");
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_predicate_node_to_index_scan(
    const std::shared_ptr<PredicateNode>& node, const std::shared_ptr<AbstractOperator>& input_operator) const {
  /**
   * Not using OperatorScanPredicate, since it splits up BETWEEN into two scans for some cases that TableScan cannot handle
   */

  auto column_id = ColumnID{0};
  auto value_variant = AllTypeVariant{NullValue{}};
  auto value2_variant = std::optional<AllTypeVariant>{};

  // Currently, we will only use IndexScans if the predicate node directly follows a StoredTableNode.
  // Our IndexScan implementation does not work on reference segments yet.
  Assert(node->left_input()->type == LQPNodeType::StoredTable, "IndexScan must follow a StoredTableNode.");

  const auto predicate = std::dynamic_pointer_cast<AbstractPredicateExpression>(node->predicate());
  Assert(predicate, "Expected predicate");
  Assert(!predicate->arguments.empty(), "Expected arguments");

  column_id = node->left_input()->get_column_id(*predicate->arguments[0]);
  if (predicate->arguments.size() > 1) {
    const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(predicate->arguments[1]);
    // This is necessary because we currently support single column indexes only
    Assert(value_expression, "Expected value as second argument for IndexScan");
    value_variant = value_expression->value;
  }
  if (predicate->arguments.size() > 2) {
    const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(predicate->arguments[2]);
    // This is necessary because we currently support single column indexes only
    Assert(value_expression, "Expected value as third argument for IndexScan");
    value2_variant = value_expression->value;
  }

  const std::vector<ColumnID> column_ids = {column_id};
  const std::vector<AllTypeVariant> right_values = {value_variant};
  std::vector<AllTypeVariant> right_values2 = {};
  if (value2_variant) right_values2.emplace_back(*value2_variant);

  auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node->left_input());
  const auto table_name = stored_table_node->table_name;
  const auto table = StorageManager::get().get_table(table_name);
  std::vector<ChunkID> indexed_chunks;

  // TODO(Sven): Each chunk could have different indices.. there needs to be an IndexScan for each occurring IndexType
  // This implementation assumes there is only a single index type per table.
  bool found_group_key_index = false;
  bool found_btree_index = false;
  for (ChunkID chunk_id{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    if (chunk->get_index(SegmentIndexType::GroupKey, column_ids)) {
      found_group_key_index = true;
      indexed_chunks.emplace_back(chunk_id);
    }
    if (chunk->get_index(SegmentIndexType::BTree, column_ids)) {
      found_btree_index = true;
      indexed_chunks.emplace_back(chunk_id);
    }
    // Potentially there could be other index types, which are not handled yet.
  }

  Assert(
      found_group_key_index != found_btree_index,
      "LQPTranslator can only handle tables that contain either BTree or GroupKey indices, but neither both nor none.");

  // All chunks that have an index on column_ids are handled by an IndexScan. All other chunks are handled by
  // TableScan(s).
  std::shared_ptr<IndexScan> index_scan;
  if (found_group_key_index) {
    index_scan = std::make_shared<IndexScan>(input_operator, SegmentIndexType::GroupKey, column_ids,
                                             predicate->predicate_condition, right_values, right_values2);
  } else if (found_btree_index) {
    index_scan = std::make_shared<IndexScan>(input_operator, SegmentIndexType::BTree, column_ids,
                                             predicate->predicate_condition, right_values, right_values2);
  }

  Assert(index_scan, "LQPTranslator can only translate IndexScan for GroupKey and BTree Indices. Did not find either.");

  const auto table_scan = _translate_predicate_node_to_table_scan(node, input_operator);

  index_scan->set_included_chunk_ids(indexed_chunks);
  table_scan->set_excluded_chunk_ids(indexed_chunks);

  return std::make_shared<UnionPositions>(index_scan, table_scan);
}

std::shared_ptr<TableScan> LQPTranslator::_translate_predicate_node_to_table_scan(
    const std::shared_ptr<PredicateNode>& node, const std::shared_ptr<AbstractOperator>& input_operator) const {
  return std::make_shared<TableScan>(input_operator, _translate_expression(node->predicate(), node->left_input()));
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_alias_node(
    const std::shared_ptr<opossum::AbstractLQPNode>& node) const {
  const auto alias_node = std::dynamic_pointer_cast<AliasNode>(node);
  const auto input_node = alias_node->left_input();
  const auto input_operator = translate_node(input_node);

  auto column_ids = std::vector<ColumnID>();
  column_ids.reserve(alias_node->column_expressions().size());

  for (const auto& expression : alias_node->column_expressions()) {
    column_ids.emplace_back(input_node->get_column_id(*expression));
  }

  return std::make_shared<AliasOperator>(input_operator, column_ids, alias_node->aliases);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_projection_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_node = node->left_input();
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(node);
  const auto input_operator = translate_node(input_node);

  return std::make_shared<Projection>(input_operator,
                                      _translate_expressions(projection_node->node_expressions, input_node));
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_sort_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto sort_node = std::dynamic_pointer_cast<SortNode>(node);
  auto input_operator = translate_node(node->left_input());

  /**
   * Go through all the order descriptions and create a sort operator for each of them.
   * Iterate in reverse because the sort operator does not support multiple columns, and instead relies on stable sort.
   * We therefore sort by the n+1-th column before sorting by the n-th column.
   */

  std::shared_ptr<AbstractOperator> current_pqp = input_operator;
  const auto& pqp_expressions = _translate_expressions(sort_node->node_expressions, node->left_input());
  if (pqp_expressions.size() > 1) {
    PerformanceWarning("Multiple ORDER BYs are executed one-by-one");
  }

  auto pqp_expression_iter = pqp_expressions.rbegin();
  auto order_by_mode_iter = sort_node->order_by_modes.rbegin();

  for (; pqp_expression_iter != pqp_expressions.rend(); ++pqp_expression_iter, ++order_by_mode_iter) {
    const auto& pqp_expression = *pqp_expression_iter;
    const auto pqp_column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(pqp_expression);
    Assert(pqp_column_expression,
           "Sort Expression '"s + pqp_expression->as_column_name() + "' must be available as column, LQP is invalid");

    current_pqp = std::make_shared<Sort>(current_pqp, pqp_column_expression->column_id, *order_by_mode_iter);
  }

  return current_pqp;
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_join_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_left_operator = translate_node(node->left_input());
  const auto input_right_operator = translate_node(node->right_input());

  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);

  if (join_node->join_mode == JoinMode::Cross) {
    PerformanceWarning("CROSS join used");
    return std::make_shared<Product>(input_left_operator, input_right_operator);
  }

  Assert(join_node->join_predicate(), "Need predicate for non Cross Join");

  /**
   * Assert that the Join Predicate is simple, e.g. of the form <column_a> <predicate> <column_b>.
   * We do not require <column_a> to be in the left input though.
   */
  const auto operator_join_predicate =
      OperatorJoinPredicate::from_expression(*join_node->join_predicate(), *node->left_input(), *node->right_input());
  Assert(operator_join_predicate,
         "Couldn't translate join predicate: "s + join_node->join_predicate()->as_column_name());

  const auto predicate_condition = operator_join_predicate->predicate_condition;

  if (join_node->join_type) {
    switch (*(join_node->join_type)) {
      case JoinType::Hash:
        return std::make_shared<JoinHash>(input_left_operator, input_right_operator, join_node->join_mode,
                                          operator_join_predicate->column_ids, predicate_condition);
      case JoinType::MPSM:
        return std::make_shared<JoinMPSM>(input_left_operator, input_right_operator, join_node->join_mode,
                                          operator_join_predicate->column_ids, predicate_condition);
      case JoinType::NestedLoop:
        return std::make_shared<JoinNestedLoop>(input_left_operator, input_right_operator, join_node->join_mode,
                                                operator_join_predicate->column_ids, predicate_condition);
      case JoinType::SortMerge:
        return std::make_shared<JoinSortMerge>(input_left_operator, input_right_operator, join_node->join_mode,
                                               operator_join_predicate->column_ids, predicate_condition);
    }
  }

  if (predicate_condition == PredicateCondition::Equals && join_node->join_mode != JoinMode::Outer) {
    return std::make_shared<JoinHash>(input_left_operator, input_right_operator, join_node->join_mode,
                                      operator_join_predicate->column_ids, predicate_condition);
  }

  return std::make_shared<JoinSortMerge>(input_left_operator, input_right_operator, join_node->join_mode,
                                         operator_join_predicate->column_ids, predicate_condition);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_aggregate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(node);

  const auto input_operator = translate_node(node->left_input());

  // Create AggregateColumnDefinitions from AggregateExpressions
  // All aggregate_pqp_expressions have to be AggregateExpressions and their argument() has to be a PQPColumnExpression
  std::vector<AggregateColumnDefinition> aggregate_column_definitions;
  aggregate_column_definitions.reserve(aggregate_node->aggregate_expressions_begin_idx);
  for (auto expression_idx = aggregate_node->aggregate_expressions_begin_idx;
       expression_idx < aggregate_node->node_expressions.size(); ++expression_idx) {
    const auto& expression = aggregate_node->node_expressions[expression_idx];

    Assert(
        expression->type == ExpressionType::Aggregate,
        "Expression '" + expression->as_column_name() + "' used as AggregateExpression is not an AggregateExpression");

    const auto& aggregate_expression = std::static_pointer_cast<AggregateExpression>(expression);

    // Always resolve the aggregate to a column, even if it is a Value. The Aggregate operator only takes columns as
    // arguments
    if (aggregate_expression->argument()) {
      const auto argument_column_id = node->left_input()->get_column_id(*aggregate_expression->argument());
      aggregate_column_definitions.emplace_back(argument_column_id, aggregate_expression->aggregate_function);

    } else {
      aggregate_column_definitions.emplace_back(std::nullopt, aggregate_expression->aggregate_function);
    }
  }

  // Create GroupByColumns from the GroupBy expressions
  std::vector<ColumnID> group_by_column_ids;
  group_by_column_ids.reserve(aggregate_node->node_expressions.size() -
                              aggregate_node->aggregate_expressions_begin_idx);

  for (auto expression_idx = size_t{0}; expression_idx < aggregate_node->aggregate_expressions_begin_idx;
       ++expression_idx) {
    const auto& expression = aggregate_node->node_expressions[expression_idx];
    const auto column_id = node->left_input()->find_column_id(*expression);
    Assert(column_id, "GroupBy expression '"s + expression->as_column_name() + "' not available as column");
    group_by_column_ids.emplace_back(*column_id);
  }

  return std::make_shared<Aggregate>(input_operator, aggregate_column_definitions, group_by_column_ids);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_limit_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_input());
  auto limit_node = std::dynamic_pointer_cast<LimitNode>(node);
  return std::make_shared<Limit>(
      input_operator, _translate_expressions({limit_node->num_rows_expression()}, node->left_input()).front());
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_insert_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_input());
  auto insert_node = std::dynamic_pointer_cast<InsertNode>(node);
  return std::make_shared<Insert>(insert_node->table_name, input_operator);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_delete_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_input());
  auto delete_node = std::dynamic_pointer_cast<DeleteNode>(node);
  return std::make_shared<Delete>(delete_node->table_name, input_operator);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_update_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  auto update_node = std::dynamic_pointer_cast<UpdateNode>(node);

  const auto input_operator_left = translate_node(node->left_input());
  const auto input_operator_right = translate_node(node->right_input());

  return std::make_shared<Update>(update_node->table_name, input_operator_left, input_operator_right);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_union_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto union_node = std::dynamic_pointer_cast<UnionNode>(node);

  const auto input_operator_left = translate_node(node->left_input());
  const auto input_operator_right = translate_node(node->right_input());

  switch (union_node->union_mode) {
    case UnionMode::Positions:
      return std::make_shared<UnionPositions>(input_operator_left, input_operator_right);
  }
  Fail("GCC thinks this is reachable");
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_validate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto input_operator = translate_node(node->left_input());
  return std::make_shared<Validate>(input_operator);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_show_tables_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  DebugAssert(node->left_input() == nullptr, "ShowTables should not have an input operator.");
  return std::make_shared<ShowTables>();
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_show_columns_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  DebugAssert(node->left_input() == nullptr, "ShowColumns should not have an input operator.");
  const auto show_columns_node = std::dynamic_pointer_cast<ShowColumnsNode>(node);
  return std::make_shared<ShowColumns>(show_columns_node->table_name);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_create_view_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto create_view_node = std::dynamic_pointer_cast<CreateViewNode>(node);
  return std::make_shared<CreateView>(create_view_node->view_name(), create_view_node->view());
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_drop_view_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto drop_view_node = std::dynamic_pointer_cast<DropViewNode>(node);
  return std::make_shared<DropView>(drop_view_node->view_name());
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_create_table_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto create_table_node = std::dynamic_pointer_cast<CreateTableNode>(node);
  return std::make_shared<CreateTable>(create_table_node->table_name, create_table_node->column_definitions);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_drop_table_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto drop_table_node = std::dynamic_pointer_cast<DropTableNode>(node);
  return std::make_shared<DropTable>(drop_table_node->table_name);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_create_prepared_plan_node(
    const std::shared_ptr<opossum::AbstractLQPNode>& node) const {
  const auto create_prepared_plan_node = std::dynamic_pointer_cast<CreatePreparedPlanNode>(node);
  return std::make_shared<CreatePreparedPlan>(create_prepared_plan_node->name,
                                              create_prepared_plan_node->prepared_plan);
}

std::shared_ptr<AbstractOperator> LQPTranslator::_translate_dummy_table_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  return std::make_shared<TableWrapper>(Projection::dummy_table());
}

std::shared_ptr<AbstractExpression> LQPTranslator::_translate_expression(
    const std::shared_ptr<AbstractExpression>& lqp_expression, const std::shared_ptr<AbstractLQPNode>& node) const {
  auto pqp_expression = lqp_expression->deep_copy();

  /**
    * Resolve Expressions to PQPColumnExpressions referencing columns from the input Operator. After this, no
    * LQPColumnExpressions remain in the pqp_expression and it is a valid PQP expression.
    */
  visit_expression(pqp_expression, [&](auto& expression) {
    // Try to resolve the Expression to a column from the input node
    const auto column_id = node->find_column_id(*expression);
    if (column_id) {
      const auto referenced_expression = node->column_expressions()[*column_id];
      expression = std::make_shared<PQPColumnExpression>(*column_id, referenced_expression->data_type(),
                                                         referenced_expression->is_nullable(),
                                                         referenced_expression->as_column_name());
      return ExpressionVisitation::DoNotVisitArguments;
    }

    // Resolve SubSelectExpression
    if (expression->type == ExpressionType::LQPSelect) {
      const auto lqp_select_expression = std::dynamic_pointer_cast<LQPSelectExpression>(expression);
      Assert(lqp_select_expression, "Expected LQPSelectExpression");

      const auto sub_select_pqp = LQPTranslator{}.translate_node(lqp_select_expression->lqp);

      auto sub_select_parameters = PQPSelectExpression::Parameters{};
      sub_select_parameters.reserve(lqp_select_expression->parameter_count());

      for (auto parameter_idx = size_t{0}; parameter_idx < lqp_select_expression->parameter_count(); ++parameter_idx) {
        const auto parameter_column_id =
            node->get_column_id(*lqp_select_expression->parameter_expression(parameter_idx));
        sub_select_parameters.emplace_back(lqp_select_expression->parameter_ids[parameter_idx], parameter_column_id);
      }

      // Only specify a type for the SubSelect if it has exactly one column. Otherwise the DataType of the Expression
      // is undefined and obtaining it will result in a runtime error.
      if (lqp_select_expression->lqp->column_expressions().size() == 1u) {
        const auto sub_select_data_type = lqp_select_expression->data_type();
        const auto sub_select_nullable = lqp_select_expression->is_nullable();

        expression = std::make_shared<PQPSelectExpression>(sub_select_pqp, sub_select_data_type, sub_select_nullable,
                                                           sub_select_parameters);
      } else {
        expression = std::make_shared<PQPSelectExpression>(sub_select_pqp, sub_select_parameters);
      }
      return ExpressionVisitation::DoNotVisitArguments;
    }

    AssertInput(expression->type != ExpressionType::LQPColumn,
                "Failed to resolve Column '"s + expression->as_column_name() + "', LQP is invalid");

    return ExpressionVisitation::VisitArguments;
  });

  return pqp_expression;
}

std::vector<std::shared_ptr<AbstractExpression>> LQPTranslator::_translate_expressions(
    const std::vector<std::shared_ptr<AbstractExpression>>& lqp_expressions,
    const std::shared_ptr<AbstractLQPNode>& node) const {
  auto pqp_expressions = std::vector<std::shared_ptr<AbstractExpression>>(lqp_expressions.size());

  for (auto expression_idx = size_t{0}; expression_idx < pqp_expressions.size(); ++expression_idx) {
    pqp_expressions[expression_idx] = _translate_expression(lqp_expressions[expression_idx], node);
  }

  return pqp_expressions;
}

}  // namespace opossum
