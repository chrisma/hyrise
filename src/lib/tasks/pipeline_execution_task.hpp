#pragma once

#include <memory>
#include <string>
#include "scheduler/abstract_task.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

class SQLPipeline;

// This task encapsulates the creation and execution of the SQLPipeline for a given std::string.
class PipelineExecutionTask : public AbstractTask {
 public:
  explicit PipelineExecutionTask(const SQLPipelineBuilder builder);

  // Get executed SQLPipeline
  std::shared_ptr<SQLPipeline> get_sql_pipeline();

  // Append a callback to the last task of SQLPipeline.
  void set_query_done_callback(const std::function<void()>& done_callback);

  // Get operator tasks created by SQLPipeline.
  std::vector<std::shared_ptr<OperatorTask>> get_tasks();

 protected:
  void _on_execute() override;

 private:
  const SQLPipelineBuilder _builder;
  std::vector<std::shared_ptr<OperatorTask>> query_tasks;
  std::shared_ptr<SQLPipeline> _sql_pipeline;
  std::function<void()> _done_callback;
};
}  // namespace opossum
