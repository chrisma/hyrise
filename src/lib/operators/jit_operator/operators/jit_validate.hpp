#pragma once

#include "abstract_jittable.hpp"
#include "concurrency/transaction_context.hpp"
#include "types.hpp"

namespace opossum {

/* The JitValidate operator validates visibility of tuples
 * within the context of a given transaction
 */
class JitValidate : public AbstractJittable {
 public:
  JitValidate(const TableType input_table_type = TableType::Data);

  std::string description() const final;

  void set_input_table_type(const TableType input_table_type);

 protected:
  void _consume(JitRuntimeContext& context) const final;

 public:
  // Function not optmized due to specialization issues with atomic
  __attribute__((optnone)) static TransactionID load_atomic_value(const copyable_atomic<TransactionID>& transaction_id);

  TableType _input_table_type;
};

}  // namespace opossum
