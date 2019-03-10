#include "column_statistics.hpp"

#include <memory>

#include "resolve_type.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/statistics_objects/counting_quotient_filter.hpp"
#include "statistics/statistics_objects/generic_histogram.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "statistics/statistics_objects/single_bin_histogram.hpp"

namespace opossum {

template <typename T>
ColumnStatistics<T>::ColumnStatistics() : BaseColumnStatistics(data_type_from_type<T>()) {}

template <typename T>
void ColumnStatistics<T>::set_statistics_object(const std::shared_ptr<AbstractStatisticsObject>& statistics_object) {
  // We allow call patterns like `c.set_statistics_object(o.scaled(0.1f))` where `o.scaled()` might return nullptr
  // because, e.g., scaling is not possible for `o`.
  if (!statistics_object) {
    return;
  }

  if (const auto histogram_object = std::dynamic_pointer_cast<AbstractHistogram<T>>(statistics_object)) {
    histogram = histogram_object;
  } else if (const auto min_max_object = std::dynamic_pointer_cast<MinMaxFilter<T>>(statistics_object)) {
    min_max_filter = min_max_object;
  } else if (const auto counting_quotient_filter_object =
                 std::dynamic_pointer_cast<CountingQuotientFilter<T>>(statistics_object)) {
    counting_quotient_filter = counting_quotient_filter_object;
  } else if (const auto null_value_ratio_object =
                 std::dynamic_pointer_cast<NullValueRatioStatistics>(statistics_object)) {
    null_value_ratio = null_value_ratio_object;
  } else {
    if constexpr (std::is_arithmetic_v<T>) {
      if (const auto range_object = std::dynamic_pointer_cast<RangeFilter<T>>(statistics_object)) {
        range_filter = range_object;
        return;
      }
    }

    Fail("Statistics object type not yet supported.");
  }
}

template <typename T>
std::shared_ptr<BaseColumnStatistics> ColumnStatistics<T>::scaled(const Selectivity selectivity) const {
  const auto statistics = std::make_shared<ColumnStatistics<T>>();

  if (histogram) {
    statistics->set_statistics_object(histogram->scaled(selectivity));
  }

  if (null_value_ratio) {
    statistics->set_statistics_object(null_value_ratio->scaled(selectivity));
  }

  if (min_max_filter) {
    statistics->set_statistics_object(min_max_filter->scaled(selectivity));
  }

  if (counting_quotient_filter) {
    statistics->set_statistics_object(counting_quotient_filter->scaled(selectivity));
  }

  if constexpr (std::is_arithmetic_v<T>) {
    if (range_filter) {
      statistics->set_statistics_object(range_filter->scaled(selectivity));
    }
  }

  return statistics;
}

template <typename T>
std::shared_ptr<BaseColumnStatistics> ColumnStatistics<T>::sliced(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  const auto statistics = std::make_shared<ColumnStatistics<T>>();

  if (histogram) {
    statistics->set_statistics_object(histogram->sliced(predicate_type, variant_value, variant_value2));
  }

  if (null_value_ratio) {
    statistics->set_statistics_object(null_value_ratio->sliced(predicate_type, variant_value, variant_value2));
  }

  if (min_max_filter) {
    statistics->set_statistics_object(min_max_filter->sliced(predicate_type, variant_value, variant_value2));
  }
  if (counting_quotient_filter) {
    statistics->set_statistics_object(counting_quotient_filter->sliced(predicate_type, variant_value, variant_value2));
  }

  if constexpr (std::is_arithmetic_v<T>) {
    if (range_filter) {
      statistics->set_statistics_object(range_filter->sliced(predicate_type, variant_value, variant_value2));
    }
  }

  return statistics;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ColumnStatistics);

}  // namespace opossum
