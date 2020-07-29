// Copyright 2019 Nobleo Technology
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "rclcpp/executors/static_single_threaded_executor.hpp"

#include "rcpputils/event_queue.hpp"
#include <queue>
//#include "rcutils/event_queue.h"

#include <memory>

#include "rclcpp/scope_exit.hpp"

using rclcpp::executors::StaticSingleThreadedExecutor;
using rclcpp::experimental::ExecutableList;

StaticSingleThreadedExecutor::StaticSingleThreadedExecutor(
  const rclcpp::ExecutorOptions & options)
: rclcpp::Executor(options)
{
  entities_collector_ = std::make_shared<StaticExecutorEntitiesCollector>();
}

StaticSingleThreadedExecutor::~StaticSingleThreadedExecutor() {}

void
StaticSingleThreadedExecutor::spin()
{
  if (spinning.exchange(true)) {
    throw std::runtime_error("spin() called while already spinning");
  }
  RCLCPP_SCOPE_EXIT(this->spinning.store(false); );

  // Set memory_strategy_ and exec_list_ based on weak_nodes_
  // Prepare wait_set_ based on memory_strategy_
  entities_collector_->init(&wait_set_, memory_strategy_, &interrupt_guard_condition_);

  std::thread t_exec_events(&StaticSingleThreadedExecutor::execute_events, this);


  while (rclcpp::ok(this->context_) && spinning.load()) {
    entities_collector_->refresh_wait_set();
    // Refresh wait set and wait for work
    execute_ready_executables();
  }

  t_exec_events.join();
}

void
StaticSingleThreadedExecutor::add_node(
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr, bool notify)
{
  // If the node already has an executor
  std::atomic_bool & has_executor = node_ptr->get_associated_with_executor_atomic();
  if (has_executor.exchange(true)) {
    throw std::runtime_error("Node has already been added to an executor.");
  }

  if (notify) {
    // Interrupt waiting to handle new node
    if (rcl_trigger_guard_condition(&interrupt_guard_condition_) != RCL_RET_OK) {
      throw std::runtime_error(rcl_get_error_string().str);
    }
  }

  entities_collector_->add_node(node_ptr);
}

void
StaticSingleThreadedExecutor::add_node(std::shared_ptr<rclcpp::Node> node_ptr, bool notify)
{
  this->add_node(node_ptr->get_node_base_interface(), notify);
}

void
StaticSingleThreadedExecutor::remove_node(
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr, bool notify)
{
  bool node_removed = entities_collector_->remove_node(node_ptr);

  if (notify) {
    // If the node was matched and removed, interrupt waiting
    if (node_removed) {
      if (rcl_trigger_guard_condition(&interrupt_guard_condition_) != RCL_RET_OK) {
        throw std::runtime_error(rcl_get_error_string().str);
      }
    }
  }

  std::atomic_bool & has_executor = node_ptr->get_associated_with_executor_atomic();
  has_executor.store(false);
}

void
StaticSingleThreadedExecutor::remove_node(std::shared_ptr<rclcpp::Node> node_ptr, bool notify)
{
  this->remove_node(node_ptr->get_node_base_interface(), notify);
}

void
StaticSingleThreadedExecutor::execute_ready_executables()
{
  // Execute all the ready timers
  for (size_t i = 0; i < wait_set_.size_of_timers; ++i) {
    if (i < entities_collector_->get_number_of_timers()) {
      if (wait_set_.timers[i] && entities_collector_->get_timer(i)->is_ready()) {
        execute_timer(entities_collector_->get_timer(i));
      }
    }
  }
}

void
StaticSingleThreadedExecutor::execute_events()
{
  auto predicate = []() { return !rcpputils::queue_is_empty(); };

  std::queue<rcpputils::Event> local_event_queue;

  while(spinning.load())
  {
    // Scope block for the mutex, otherwise we get a deadlock
    // when trying to execute subscription
    {
      // We wait here until something has been pushed to the executable queue.
      std::unique_lock<std::mutex> lock(*execConditionMutex);
      execConditionVariable->wait(lock, predicate);

      while (!rcpputils::queue_is_empty()) {
        local_event_queue.push(rcpputils::rcpputils_get_next_event());
      }
    }

    // Process the events
    while (!local_event_queue.empty())
    {
      rcpputils::Event event = local_event_queue.front();

      switch(event.type)
      {
      case rcpputils::SUBSCRIPTION_EVENT:
        {
          execute_subscription(std::move(entities_collector_->get_subscription_by_handle(event.entity)));
        }
        break;

      case rcpputils::SERVICE_EVENT:
        std::cout << "SERVICE_EVENT: " << event.entity << std::endl;
        break;
      }

      local_event_queue.pop();
    }
  }
}
