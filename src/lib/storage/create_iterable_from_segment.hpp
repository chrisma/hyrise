#pragma once

#include "storage/dictionary_segment/dictionary_segment_iterable.hpp"
#include "storage/frame_of_reference/frame_of_reference_iterable.hpp"
#include "storage/lz4/lz4_iterable.hpp"
#include "storage/run_length_segment/run_length_segment_iterable.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"

namespace opossum {

class ReferenceSegment;
template <typename T>
class ReferenceSegmentIterable;

/**
 * @defgroup Uniform interface to create an iterable from a segment
 *
 * These methods cannot be part of the segments' interfaces because
 * reference segment are not templated and thus don’t know their type.
 *
 * All iterables implement the same interface using static polymorphism
 * (i.e. the CRTP pattern, see segment_iterables/.hpp).
 *
 * In debug mode, create_iterable_from_segment returns a type erased
 * iterable, i.e., all iterators have the same type
 *
 * @{
 */

template <typename T, bool EraseSegmentType = HYRISE_DEBUG, bool>
auto create_iterable_from_segment(const ValueSegment<T>& segment) {
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
    return ValueSegmentIterable<T>{segment};
  }
}

template <typename T, bool EraseSegmentType = HYRISE_DEBUG, bool>
auto create_iterable_from_segment(const DictionarySegment<T>& segment) {
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
#if defined(HYRISE_ENCODING_ALL) || defined(HYRISE_ENCODING_DICTIONARY)
    return DictionarySegmentIterable<T, pmr_vector<T>>{segment};
#else
    PerformanceWarning("Dictionary encoding was disabled at compile time. Using slower, type-erased iterators.");
    return AnySegmentIterable<T>{DictionarySegmentIterable<T, pmr_vector<T>>{segment}};
#endif
  }
}

template <typename T, bool EraseSegmentType = HYRISE_DEBUG, bool>
auto create_iterable_from_segment(const RunLengthSegment<T>& segment) {
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
#if defined(HYRISE_ENCODING_ALL) || defined(HYRISE_ENCODING_RUN_LENGTH)
    return RunLengthSegmentIterable<T>{segment};
#else
    PerformanceWarning("Run-Length encoding was disabled at compile time. Using slower, type-erased iterators.");
    return AnySegmentIterable<T>{RunLengthSegmentIterable<T>{segment}};
#endif
  }
}

template <typename T, bool EraseSegmentType = HYRISE_DEBUG, bool>
auto create_iterable_from_segment(const FixedStringDictionarySegment<T>& segment) {
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
#if defined(HYRISE_ENCODING_ALL) || defined(HYRISE_ENCODING_FIXED_STRING_DICTIONARY)
    return DictionarySegmentIterable<T, FixedStringVector>{segment};
#else
    PerformanceWarning("Fixed-String Dictionary encoding was disabled at compile time. Using slower, type-erased iterators.");
    return AnySegmentIterable<T>{DictionarySegmentIterable<T, FixedStringVector>{segment}};
#endif
  }
}

template <typename T, bool EraseSegmentType = HYRISE_DEBUG, bool>
auto create_iterable_from_segment(const FrameOfReferenceSegment<T>& segment) {
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
#if defined(HYRISE_ENCODING_ALL) || defined(HYRISE_ENCODING_FRAME_OF_REFERENCE)
    return FrameOfReferenceIterable<T>{segment};
#else
    PerformanceWarning("Frame-of-Refernce encoding was disabled at compile time. Using slower, type-erased iterators.");
    return AnySegmentIterable<T>{FrameOfReferenceIterable<T>{segment}};
#endif
  }
}

template <typename T, bool EraseSegmentType = true, bool>
auto create_iterable_from_segment(const LZ4Segment<T>& segment) {
  // LZ4Segment always gets erased as its decoding is so slow, the virtual function calls won't make
  // a difference. If we'd allow it to not be erased we'd risk compile time increase creeping in for no benefit
  return AnySegmentIterable<T>(LZ4Iterable<T>(segment));
}

/**
 * This function must be forward-declared because ReferenceSegmentIterable
 * includes this file leading to a circular dependency
 */
template <typename T, bool EraseSegmentType = HYRISE_DEBUG, bool EraseReferencedSegmentType = HYRISE_DEBUG>
auto create_iterable_from_segment(const ReferenceSegment& segment);

/**@}*/

}  // namespace opossum

#include "create_iterable_from_segment.ipp"
