#include "generate_pruning_statistics.hpp"

#include <atomic>
#include <iostream>
#include <thread>
#include <unordered_set>

#include "base_column_statistics.hpp"
#include "column_statistics.hpp"
#include "generate_column_statistics.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/histograms/generic_histogram_builder.hpp"
#include "statistics/histograms/histogram_utils.hpp"
#include "statistics/horizontal_statistics_slice.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/null_value_ratio.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "statistics/table_cardinality_estimation_statistics.hpp"
#include "statistics/vertical_statistics_slice.hpp"
#include "storage/table.hpp"
#include "table_statistics.hpp"

namespace {

using namespace opossum;  // NOLINT

template <typename T>
void create_pruning_filter_for_segment(VerticalStatisticsSlice<T>& vertical_slices, const pmr_vector<T>& dictionary) {
  std::shared_ptr<AbstractStatisticsObject> pruning_filter;
  if constexpr (std::is_arithmetic_v<T>) {
    if (!vertical_slices.range_filter) {
      pruning_filter = RangeFilter<T>::build_filter(dictionary);
    }
  } else {
    if (!vertical_slices.min_max_filter) {
      if (!dictionary.empty()) {
        pruning_filter = std::make_shared<MinMaxFilter<T>>(dictionary.front(), dictionary.back());
      }
    }
  }

  if (pruning_filter) {
    vertical_slices.set_statistics_object(pruning_filter);
  }
}

}  // namespace

namespace opossum {

TableStatistics generate_table_statistics(const Table& table) {
  std::vector<std::shared_ptr<const BaseColumnStatistics>> column_statistics;
  column_statistics.reserve(table.column_count());

  for (ColumnID column_id{0}; column_id < table.column_count(); ++column_id) {
    const auto column_data_type = table.column_data_types()[column_id];

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      column_statistics.emplace_back(generate_column_statistics<ColumnDataType>(table, column_id));
    });
  }

  return {table.type(), static_cast<float>(table.row_count()), column_statistics};
}

void generate_chunk_pruning_statistics(const std::shared_ptr<Table>& table) {
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);

    if (chunk->is_mutable()) {
      continue;
    }

    const auto chunk_statistics = std::make_shared<HorizontalStatisticsSlice>(chunk->size());

    for (auto column_id = ColumnID{0}; column_id < chunk->column_count(); ++column_id) {
      const auto segment = chunk->get_segment(column_id);

      resolve_data_and_segment_type(*segment, [&](auto type, auto& typed_segment) {
        using SegmentType = std::decay_t<decltype(typed_segment)>;
        using ColumnDataType = typename decltype(type)::type;

        const auto segment_statistics = std::make_shared<VerticalStatisticsSlice<ColumnDataType>>();

        if constexpr (std::is_same_v<SegmentType, DictionarySegment<ColumnDataType>>) {
          // we can use the fact that dictionary segments have an accessor for the dictionary
          const auto& dictionary = *typed_segment.dictionary();
          create_pruning_filter_for_segment(*segment_statistics, dictionary);
        } else {
          // if we have a generic segment we create the dictionary ourselves
          auto iterable = create_iterable_from_segment<ColumnDataType>(typed_segment);
          std::unordered_set<ColumnDataType> values;
          iterable.for_each([&](const auto& value) {
            // we are only interested in non-null values
            if (!value.is_null()) {
              values.insert(value.value());
            }
          });
          pmr_vector<ColumnDataType> dictionary{values.cbegin(), values.cend()};
          std::sort(dictionary.begin(), dictionary.end());
          create_pruning_filter_for_segment(*segment_statistics, dictionary);
        }

        chunk_statistics->vertical_slices.emplace_back(segment_statistics);
      });
    }

    table->get_chunk(chunk_id)->set_pruning_statistics(chunk_statistics);
  }
}

}  // namespace opossum
