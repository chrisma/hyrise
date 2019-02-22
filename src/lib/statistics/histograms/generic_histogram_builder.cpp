#include "generic_histogram_builder.hpp"

#include "utils/assert.hpp"

namespace opossum {

template <typename T>
GenericHistogramBuilder<T>::GenericHistogramBuilder(const size_t reserve_bin_count,
                                                    const std::optional<StringHistogramDomain>& string_domain) {
  constexpr auto is_string_histogram = std::is_same_v<T, std::string>;  // Cannot do this in the first Assert arg, as Assert is a macro... :(   // NOLINT
  Assert(is_string_histogram == string_domain.has_value(), "StringHistogramDomain required IFF T == std::string");

  bin_minima.reserve(reserve_bin_count);
  bin_maxima.reserve(reserve_bin_count);
  bin_heights.reserve(reserve_bin_count);
  bin_distinct_counts.reserve(reserve_bin_count);
}

template <typename T>
bool GenericHistogramBuilder<T>::empty() const {
  return bin_minima.empty();
}

template <typename T>
void GenericHistogramBuilder<T>::add_bin(const T& min, const T& max, float height, float distinct_count) {
  DebugAssert(bin_minima.empty() || min > bin_maxima.back(), "Bins must be sorted and cannot overlap");
  DebugAssert(min <= max, "Invalid bin slice");

  bin_minima.emplace_back(min);
  bin_maxima.emplace_back(max);
  bin_heights.emplace_back(static_cast<HistogramCountType>(height));
  bin_distinct_counts.emplace_back(static_cast<HistogramCountType>(distinct_count));
}

template <typename T>
void GenericHistogramBuilder<T>::add_sliced_bin(const AbstractHistogram<T>& source, const BinID bin_id,
                                                const T& slice_min, const T& slice_max) {
  DebugAssert(slice_max >= slice_min, "Invalid slice");
  DebugAssert(slice_min >= source.bin_minimum(bin_id), "Invalid slice minimum");
  DebugAssert(slice_max <= source.bin_maximum(bin_id), "Invalid slice minimum");

  const auto sliced_bin_ratio =
      source.bin_ratio_less_than_equals(bin_id, slice_max) - source.bin_ratio_less_than(bin_id, slice_min);

  auto height = source.bin_height(bin_id) * sliced_bin_ratio;
  auto distinct_count = source.bin_distinct_count(bin_id) * sliced_bin_ratio;

  add_bin(slice_min, slice_max, height, distinct_count);
}

template <typename T>
void GenericHistogramBuilder<T>::add_copied_bins(const AbstractHistogram<T>& source, const BinID begin_bin_id,
                                                 const BinID end_bin_id) {
  DebugAssert(begin_bin_id <= end_bin_id, "Invalid bin range");

  for (auto bin_id = begin_bin_id; bin_id < end_bin_id; ++bin_id) {
    const auto bin = source.bin(bin_id);
    add_bin(bin.min, bin.max, bin.height, bin.distinct_count);
  }
}

template <typename T>
std::shared_ptr<GenericHistogram<T>> GenericHistogramBuilder<T>::build() {
  return std::make_shared<GenericHistogram<T>>(std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights),
                                               std::move(bin_distinct_counts));
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(GenericHistogramBuilder);

}  // namespace opossum
