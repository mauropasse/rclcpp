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

#include <memory>

#include "rclcpp/scope_exit.hpp"

using rclcpp::executors::StaticSingleThreadedExecutor;
using rclcpp::executor::ExecutableList;

StaticSingleThreadedExecutor::StaticSingleThreadedExecutor(
  const rclcpp::executor::ExecutorArgs & args)
: executor::Executor(args)
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

  while (rclcpp::ok(this->context_) && spinning.load()) {
    // Refresh wait set and wait for work
    entities_collector_->rclcpp_wait();
    execute_ready_executables();
  }
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
  size_t num_ready_subscribers = entities_collector_->ready_items[SUBSCRIBER];
  size_t num_ready_timers      = entities_collector_->ready_items[TIMER];
  size_t num_ready_services    = entities_collector_->ready_items[SERVICE];
  size_t num_ready_clients     = entities_collector_->ready_items[CLIENT];
  size_t num_ready_events      = entities_collector_->ready_items[EVENT]; //Not used
  size_t num_ready_gc          = entities_collector_->ready_items[GC];

  // Execute all the ready subscriptions
  for (size_t i = 0; i < num_ready_subscribers; ++i){
    size_t n = entities_collector_->ready_subscriber[i];
    execute_subscription(entities_collector_->get_subscription(n));
  }

  // Execute all the ready timers
  for (size_t i = 0; i < num_ready_timers; ++i){
    size_t n = entities_collector_->ready_timer[i];
    if (entities_collector_->get_timer(n)->is_ready()) {
      execute_timer(entities_collector_->get_timer(n));
    }
  }

  // Execute all the ready services
  for (size_t i = 0; i < num_ready_services; ++i) {
    size_t n = entities_collector_->ready_service[i];
    execute_service(entities_collector_->get_service(n));
  }

  // Execute all the ready clients
  for (size_t i = 0; i < num_ready_clients; ++i) {
    size_t n = entities_collector_->ready_client[i];
    execute_client(entities_collector_->get_client(n));
  }

  // Execute all the ready waitables
  for (size_t i = 0; i < entities_collector_->get_number_of_waitables(); ++i) {
    if (entities_collector_->get_waitable(i)->is_ready(&wait_set_)) {
      entities_collector_->get_waitable(i)->execute();
    }
  }
}
