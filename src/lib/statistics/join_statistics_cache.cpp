#include "join_statistics_cache.hpp"

#include "logical_query_plan/lqp_utils.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/table_cardinality_estimation_statistics.hpp"
#include "statistics/horizontal_statistics_slice.hpp"

namespace {

using namespace opossum;  // NOLINT

}  // namespace

namespace opossum {

JoinStatisticsCache JoinStatisticsCache::from_join_graph(const JoinGraph& join_graph) {
  VertexIndexMap vertex_indices;
  for (auto vertex_idx = size_t{0}; vertex_idx < join_graph.vertices.size(); ++vertex_idx) {
    vertex_indices.emplace(join_graph.vertices[vertex_idx], vertex_idx);
  }

  PredicateIndexMap predicate_indices;

  auto predicate_idx = size_t{0};
  for (const auto& edge : join_graph.edges) {
    for (const auto& predicate : edge.predicates) {
      predicate_indices.emplace(predicate, predicate_idx++);
    }
  }

  return {std::move(vertex_indices), std::move(predicate_indices)};
}

JoinStatisticsCache::JoinStatisticsCache(VertexIndexMap&& vertex_indices, PredicateIndexMap&& predicate_indices)
    : _vertex_indices(std::move(vertex_indices)), _predicate_indices(std::move(predicate_indices)) {}

std::optional<JoinStatisticsCache::Bitmask> JoinStatisticsCache::bitmask(
    const std::shared_ptr<AbstractLQPNode>& lqp) const {
  auto bitmask = std::optional<Bitmask>{_vertex_indices.size() + _predicate_indices.size()};

  visit_lqp(lqp, [&](const auto& node) {
    if (!bitmask) return LQPVisitation::DoNotVisitInputs;

    if (const auto vertex_iter = _vertex_indices.find(node); vertex_iter != _vertex_indices.end()) {
      DebugAssert(vertex_iter->second < bitmask->size(), "Vertex index out of range");
      bitmask->set(vertex_iter->second);
      return LQPVisitation::DoNotVisitInputs;

    } else if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(node)) {
      if (join_node->join_mode == JoinMode::Inner) {
        const auto predicate_index_iter = _predicate_indices.find(join_node->join_predicate());
        if (predicate_index_iter == _predicate_indices.end()) {
          bitmask.reset();
          return LQPVisitation::DoNotVisitInputs;
        } else {
          Assert(predicate_index_iter->second + _vertex_indices.size() < bitmask->size(),
                 "Predicate index out of range");
          bitmask->set(predicate_index_iter->second + _vertex_indices.size());
        }
      } else if (join_node->join_mode == JoinMode::Cross) {
        return LQPVisitation::VisitInputs;
      } else {
        // Non-Inner/Cross join detected, cannot construct a bitmask from those
        bitmask.reset();
        return LQPVisitation::DoNotVisitInputs;
      }

    } else if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node)) {
      const auto predicate_index_iter = _predicate_indices.find(predicate_node->predicate());
      if (predicate_index_iter == _predicate_indices.end()) {
        bitmask.reset();
        return LQPVisitation::DoNotVisitInputs;
      } else {
        Assert(predicate_index_iter->second + _vertex_indices.size() < bitmask->size(), "Predicate index out of range");
        bitmask->set(predicate_index_iter->second + _vertex_indices.size());
      }

    } else if (node->type == LQPNodeType::Sort) {
      // ignore node type as it doesn't change the cardinality
    } else {
      bitmask.reset();
      return LQPVisitation::DoNotVisitInputs;
    }

    return LQPVisitation::VisitInputs;
  });

  return bitmask;
}

std::shared_ptr<TableCardinalityEstimationStatistics> JoinStatisticsCache::get(
    const Bitmask& bitmask, const std::vector<std::shared_ptr<AbstractExpression>>& requested_column_order) const {
  const auto cache_iter = _cache.find(bitmask);
  if (cache_iter == _cache.end()) {
    return nullptr;
  }

  /**
   * We found a matching cache_entry. Now, let's adjust its column order
   */

  const auto& cache_entry = cache_iter->second;

  DebugAssert(requested_column_order.size() == cache_entry.column_expression_order.size(),
              "Wrong number in requested column order");

  const auto cached_table_statistics = cache_entry.table_statistics;

  // Compute the mapping from result column ids to cached column ids (cached_column_ids) and the column data types
  // of the result;
  auto cached_column_ids = std::vector<ColumnID>{requested_column_order.size()};
  auto result_column_data_types = std::vector<DataType>{requested_column_order.size()};
  for (auto column_id = ColumnID{0}; column_id < requested_column_order.size(); ++column_id) {
    const auto cached_column_id_iter = cache_entry.column_expression_order.find(requested_column_order[column_id]);
    Assert(cached_column_id_iter != cache_entry.column_expression_order.end(), "Column not found in cached statistics");
    const auto cached_column_id = cached_column_id_iter->second;
    result_column_data_types[column_id] = cached_table_statistics->column_data_types[cached_column_id];
    cached_column_ids[column_id] = cached_column_id;
  }

  // Allocate the TableStatistics, ChunkStatisticsSet and ChunkStatistics to be returned
  const auto result_table_statistics = std::make_shared<TableCardinalityEstimationStatistics>(result_column_data_types);
  result_table_statistics->cardinality_estimation_slices.reserve(
      cached_table_statistics->cardinality_estimation_slices.size());
  result_table_statistics->approx_invalid_row_count = cached_table_statistics->approx_invalid_row_count.load();

  for (const auto& cached_statistics_slice : cached_table_statistics->cardinality_estimation_slices) {
    const auto result_statistics_slice = std::make_shared<HorizontalStatisticsSlice>(cached_statistics_slice->row_count);
    result_statistics_slice->vertical_slices.resize(cached_statistics_slice->vertical_slices.size());

    result_table_statistics->cardinality_estimation_slices.emplace_back(result_statistics_slice);
  }

  // Bring SegmentStatistics into the requested order for each statistics slice
  for (auto column_id = ColumnID{0}; column_id < requested_column_order.size(); ++column_id) {
    const auto cached_column_id = cached_column_ids[column_id];
    for (auto slice_idx = size_t{0}; slice_idx < cached_table_statistics->cardinality_estimation_slices.size();
         ++slice_idx) {
      const auto& cached_statistics_slice = cached_table_statistics->cardinality_estimation_slices[slice_idx];
      const auto& result_statistics_slice = result_table_statistics->cardinality_estimation_slices[slice_idx];
      result_statistics_slice->vertical_slices[column_id] =
          cached_statistics_slice->vertical_slices[cached_column_id];
    }
  }

  return result_table_statistics;
}

void JoinStatisticsCache::set(const Bitmask& bitmask,
                              const std::vector<std::shared_ptr<AbstractExpression>>& column_order,
                              const std::shared_ptr<TableCardinalityEstimationStatistics>& table_statistics) {
  auto cache_entry = CacheEntry{};
  cache_entry.table_statistics = table_statistics;

  for (auto column_id = ColumnID{0}; column_id < column_order.size(); ++column_id) {
    cache_entry.column_expression_order.emplace(column_order[column_id], column_id);
  }

  _cache.emplace(bitmask, std::move(cache_entry));
}
}  // namespace opossum
