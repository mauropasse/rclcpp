// Copyright 2020 Open Source Robotics Foundation, Inc.
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

#ifndef RCLCPP__EXECUTORS__STATIC_EXECUTOR_WAITABLE_HPP_
#define RCLCPP__EXECUTORS__STATIC_EXECUTOR_WAITABLE_HPP_

namespace rclcpp
{
namespace executors
{

class StaticExecutorWaitable : public rclcpp::Waitable
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(StaticExecutorWaitable)

  // Constructor
  RCLCPP_PUBLIC
  StaticExecutorWaitable() = default;

  // Destructor
  ~StaticExecutorWaitable();

  RCLCPP_PUBLIC
  void
  init(rcl_wait_set_t* p_wait_set,
       memory_strategy::MemoryStrategy::SharedPtr& memory_strategy,
       StaticExecutorWaitable::SharedPtr this_shared_ptr);

  RCLCPP_PUBLIC
  void
  execute();

  RCLCPP_PUBLIC
  void
  get_memory_strategy();

  RCLCPP_PUBLIC
  void
  get_executable_list();

  /// Function to reallocate space for entities in the wait set.
  RCLCPP_PUBLIC
  void
  prepare_wait_set();

  /// Function to add_handles_to_wait_set and wait for work and
  // block until the wait set is ready or until the timeout has been exceeded.
  RCLCPP_PUBLIC
  void
  refresh_wait_set(std::chrono::nanoseconds timeout = std::chrono::nanoseconds(-1));

  RCLCPP_PUBLIC
  bool
  add_to_wait_set(rcl_wait_set_t * wait_set) override;

  RCLCPP_PUBLIC
  size_t
  get_number_of_ready_guard_conditions() override;

  RCLCPP_PUBLIC
  void
  add_node_and_guard_condition(
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr,
    rcl_guard_condition_t * node_guard_condition);

  RCLCPP_PUBLIC
  void
  remove_node_and_guard_condition(
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr);

  /// Complete all available queued work without blocking.
  /**
   * This function checks if after the guard condition was triggered
   * (or a spurious wakeup happened) we are really ready to execute
   * i.e. re-collect entities
   */
  RCLCPP_PUBLIC
  bool
  is_ready(rcl_wait_set_t * wait_set) override;

  RCLCPP_PUBLIC
  size_t
  get_number_of_timers() {return exec_list_.number_of_timers;}

  RCLCPP_PUBLIC
  size_t
  get_number_of_subscriptions() {return exec_list_.number_of_subscriptions;}

  RCLCPP_PUBLIC
  size_t
  get_number_of_services() {return exec_list_.number_of_services;}

  RCLCPP_PUBLIC
  size_t
  get_number_of_clients() {return exec_list_.number_of_clients;}

  RCLCPP_PUBLIC
  size_t
  get_number_of_waitables() {return exec_list_.number_of_waitables;}

  RCLCPP_PUBLIC
  rclcpp::SubscriptionBase::SharedPtr
  get_subscription(size_t i) {return exec_list_.subscription[i];}

  RCLCPP_PUBLIC
  rclcpp::TimerBase::SharedPtr
  get_timer(size_t i) {return exec_list_.timer[i];}

  RCLCPP_PUBLIC
  rclcpp::ServiceBase::SharedPtr
  get_service(size_t i) {return exec_list_.service[i];}

  RCLCPP_PUBLIC
  rclcpp::ClientBase::SharedPtr
  get_client(size_t i) {return exec_list_.client[i];}

  RCLCPP_PUBLIC
  rclcpp::Waitable::SharedPtr
  get_waitable(size_t i) {return exec_list_.waitable[i];}

private:
  /// Nodes guard conditions which trigger this waitable
  std::list<const rcl_guard_condition_t *> guard_conditions_;

  /// Memory strategy: an interface for handling user-defined memory allocation strategies.
  memory_strategy::MemoryStrategy::SharedPtr memory_strategy_;

  /// Executable list
  std::list<rclcpp::node_interfaces::NodeBaseInterface::WeakPtr> weak_nodes_;

  /// Wait set for managing entities that the rmw layer waits on.
  rcl_wait_set_t* p_wait_set_ = nullptr;

  /// Executable list: timers, subscribers, clients, services and waitables
  executor::ExecutableList exec_list_;

  /// Shared pointer to this StaticExecutorWaitable
  StaticExecutorWaitable::SharedPtr this_shared_ptr_;
};

}  // namespace executors
}  // namespace rclcpp

#endif  // RCLCPP__EXECUTORS__STATIC_EXECUTOR_WAITABLE_HPP_