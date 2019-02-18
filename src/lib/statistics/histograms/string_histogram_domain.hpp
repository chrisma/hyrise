#pragma once

#include <string>

namespace opossum {

// To represent Strings as Histogram bounds they need to be converted to integrals.
// A StringHistogramDomain drives this conversion using a prefix length and a supported character set
class StringHistogramDomain {
 public:
  using IntegralType = uint64_t;

  // Use default character set and prefix length
  StringHistogramDomain();

  StringHistogramDomain(const std::string& supported_characters, const size_t prefix_length);

  bool contains(const std::string& string_value) const;
  bool is_valid_prefix(const std::string& string_value) const;

  std::string number_to_string(IntegralType int_value) const;
  IntegralType string_to_number(const std::string& string_value) const;

  std::string string_to_domain(const std::string& string_value) const;

  std::string string_before(const std::string& string_value, const std::string& lower_bound) const;

  std::string next_value(const std::string& string_value) const;
  std::string previous_value(const std::string& string_value) const;

  IntegralType base_number() const;

  bool operator==(const StringHistogramDomain& rhs) const;

  std::string supported_characters;
  size_t prefix_length;
};

}  // namespace opossum