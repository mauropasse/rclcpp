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
  // Init executor's condition variable
  cv_ = std::make_shared<std::condition_variable>();
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
    entities_collector_->refresh_wait_set();
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
  // Execute all the ready subscriptions
  for (size_t i = 0; i < wait_set_.size_of_subscriptions; ++i) {
    if (i < entities_collector_->get_number_of_subscriptions()) {
      if (wait_set_.subscriptions[i]) {
        execute_subscription(entities_collector_->get_subscription(i));
      }
    }
  }
  // Execute all the ready timers
  for (size_t i = 0; i < wait_set_.size_of_timers; ++i) {
    if (i < entities_collector_->get_number_of_timers()) {
      if (wait_set_.timers[i] && entities_collector_->get_timer(i)->is_ready()) {
        execute_timer(entities_collector_->get_timer(i));
      }
    }
  }
  // Execute all the ready services
  for (size_t i = 0; i < wait_set_.size_of_services; ++i) {
    if (i < entities_collector_->get_number_of_services()) {
      if (wait_set_.services[i]) {
        execute_service(entities_collector_->get_service(i));
      }
    }
  }
  // Execute all the ready clients
  for (size_t i = 0; i < wait_set_.size_of_clients; ++i) {
    if (i < entities_collector_->get_number_of_clients()) {
      if (wait_set_.clients[i]) {
        execute_client(entities_collector_->get_client(i));
      }
    }
  }
  // Execute all the ready waitables
  for (size_t i = 0; i < entities_collector_->get_number_of_waitables(); ++i) {
    if (entities_collector_->get_waitable(i)->is_ready(&wait_set_)) {
      entities_collector_->get_waitable(i)->execute();
    }
  }
}


void
StaticSingleThreadedExecutor::intra_process_spin()
{
  // Check if executor was already spinning
  if (spinning.exchange(true)) {
    throw std::runtime_error("spin() called while already spinning");
  }
  RCLCPP_SCOPE_EXIT(this->spinning.store(false); );

  // Init executable lists
  entities_collector_->init(&wait_set_, memory_strategy_, &interrupt_guard_condition_);

  // Add this executor condition variable to intra-process subscriptions
  provide_condition_variable();

  // Start the publisher timers
  auto timer_it = timers_map_.begin();
  while (timer_it != timers_map_.end())
  {
      timer_it++->second->start();
  }

  while (rclcpp::ok() && spinning.load()) {
    std::unique_lock<std::mutex> lock(m_);

    // Check condition variable
    cv_->wait(lock, [this]{return subscriptions_ready_to_work();});

    // Find ready subscriptions and execute them
    execute_ready_subscriptions();

    lock.unlock();
  }
}

void
StaticSingleThreadedExecutor::provide_condition_variable()
{
  // Provide executor's condition variablel to intra-process subscriptions
  for (size_t i = 0; i < entities_collector_->get_number_of_ip_waitables(); i++) {
    entities_collector_->get_ip_waitable(i)->set_condition_variable(cv_);
  }
}


bool
StaticSingleThreadedExecutor::subscriptions_ready_to_work()
{
  // Execute all the ready waitables (intra process subscriptions)
  for (size_t i = 0; i < entities_collector_->get_number_of_ip_waitables(); i++) {
    if (entities_collector_->get_ip_waitable(i)->is_ready(nullptr)) {
      return true;
    }
  }

  return false;
}

void
StaticSingleThreadedExecutor::execute_ready_subscriptions()
{
  // Execute all the ready waitables (intra process subscriptions)
  for (size_t i = 0; i < entities_collector_->get_number_of_ip_waitables(); i++) {
    if (entities_collector_->get_ip_waitable(i)->is_ready(nullptr)) {
      entities_collector_->get_ip_waitable(i)->execute();
    }
  }
}

void
StaticSingleThreadedExecutor::add_callbacks(
  std::vector<std::pair<
    std::function<void()>,
    std::chrono::microseconds>> callbacks)
{
  // Gather node callbacks with same period under same timer
  for (const auto& callback : callbacks){
    auto period = callback.second;
    //Look for existing timers with that period
    auto timer_it = timers_map_.find(period);
    if (timer_it != timers_map_.end()) {
      // There's a timer with that period, add callback to it
      timer_it->second->add_callback(std::move(callback.first));
    } else {
      // There's not a timer with that period, create one and add callback
      auto timer = this->create_std_wall_timer(std::move(callback.first), period);
      timers_map_[period] = timer;
    }
  }
}
