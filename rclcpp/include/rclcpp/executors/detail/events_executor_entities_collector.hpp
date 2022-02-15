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

#ifndef RCLCPP__EXECUTORS__DETAIL__EVENTS_EXECUTOR_ENTITIES_COLLECTOR_HPP_
#define RCLCPP__EXECUTORS__DETAIL__EVENTS_EXECUTOR_ENTITIES_COLLECTOR_HPP_

#include <list>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

#include "rclcpp/executors/detail/entities_collector.hpp"
#include "rclcpp/executors/detail/events_executor_event_types.hpp"
#include "rclcpp/node_interfaces/node_base_interface.hpp"
#include "rclcpp/timers_manager.hpp"

namespace rclcpp
{
namespace executors
{
// forward declaration of EventsExecutor to avoid circular dependency
class EventsExecutor;

namespace detail
{

class EventsExecutorEntitiesCollector final
  : public rclcpp::executors::detail::EntitiesCollector,
  public std::enable_shared_from_this<EventsExecutorEntitiesCollector>
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(EventsExecutorEntitiesCollector)

  // Constructor
  RCLCPP_PUBLIC
  EventsExecutorEntitiesCollector(
    EventsExecutor * executor);

  // Destructor
  RCLCPP_PUBLIC
  ~EventsExecutorEntitiesCollector();

  // Initialize entities collector
  RCLCPP_PUBLIC
  void init();

  /// Execute the waitable.
  RCLCPP_PUBLIC
  void
  execute(std::shared_ptr<void> & data) override;

  /// Function to add_handles_to_wait_set and wait for work and
  /**
   * block until the wait set is ready or until the timeout has been exceeded.
   * \throws std::runtime_error if wait set couldn't be cleared or filled.
   * \throws any rcl errors from rcl_wait, \see rclcpp::exceptions::throw_from_rcl_error()
   */
  RCLCPP_PUBLIC
  void
  refresh_wait_set(std::chrono::nanoseconds timeout = std::chrono::nanoseconds(-1));

  /**
   * \throws std::runtime_error if it couldn't add guard condition to wait set
   */
  RCLCPP_PUBLIC
  void
  add_to_wait_set(rcl_wait_set_t * wait_set) override;

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
  std::shared_ptr<void>
  take_data() override
  {
    // This waitable doesn't handle any data
    return nullptr;
  }

  RCLCPP_PUBLIC
  std::shared_ptr<void>
  take_data_by_entity_id(int id) override
  {
    (void)id;
    return take_data();
  }

  ///
  /**
   * Get the subscription shared pointer corresponding
   * to a subscription identifier
   */
  RCLCPP_PUBLIC
  rclcpp::SubscriptionBase::SharedPtr
  get_subscription(const void * subscription_id);

  ///
  /**
   * Get the client shared pointer corresponding
   * to a client identifier
   */
  RCLCPP_PUBLIC
  rclcpp::ClientBase::SharedPtr
  get_client(const void * client_id);

  ///
  /**
   * Get the service shared pointer corresponding
   * to a service identifier
   */
  RCLCPP_PUBLIC
  rclcpp::ServiceBase::SharedPtr
  get_service(const void * service_id);

  ///
  /**
   * Get the waitable shared pointer corresponding
   * to a waitable identifier
   */
  RCLCPP_PUBLIC
  rclcpp::Waitable::SharedPtr
  get_waitable(const void * waitable_id);

  ///
  /**
   * Add a weak pointer to a waitable
   */
  RCLCPP_PUBLIC
  void
  add_waitable(rclcpp::Waitable::SharedPtr waitable);

protected:
  void
  callback_group_added_impl(
    rclcpp::CallbackGroup::SharedPtr group) override;

  void
  node_added_impl(
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node) override;

  void
  callback_group_removed_impl(
    rclcpp::CallbackGroup::SharedPtr group) override;

  void
  node_removed_impl(
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node) override;

private:
  void
  set_entities_event_callbacks_from_map(
    const WeakCallbackGroupsToNodesMap & weak_groups_to_nodes);

  void
  set_callback_group_entities_callbacks(rclcpp::CallbackGroup::SharedPtr group);

  void
  unset_callback_group_entities_callbacks(rclcpp::CallbackGroup::SharedPtr group);

  void
  set_guard_condition_callback(rclcpp::GuardCondition * guard_condition);

  void
  unset_guard_condition_callback(rclcpp::GuardCondition * guard_condition);

  std::function<void(size_t)>
  create_entity_callback(void * exec_entity_id, ExecutorEventType type);

  std::function<void(size_t, int)>
  create_waitable_callback(void * waitable_id);

  typedef std::map<rclcpp::node_interfaces::NodeBaseInterface::WeakPtr,
      const rclcpp::GuardCondition *,
      std::owner_less<rclcpp::node_interfaces::NodeBaseInterface::WeakPtr>>
    WeakNodesToGuardConditionsMap;
  WeakNodesToGuardConditionsMap weak_nodes_to_guard_conditions_;

  // Mutex to protect vector of new nodes.
  std::recursive_mutex reentrant_mutex_;

  // Maps: entity identifiers to weak pointers from the entities registered in the executor
  // so in the case of an event providing and ID, we can retrieve and own the corresponding
  // entity while it performs work
  std::unordered_map<const void *, rclcpp::SubscriptionBase::WeakPtr> weak_subscriptions_map_;
  std::unordered_map<const void *, rclcpp::ServiceBase::WeakPtr> weak_services_map_;
  std::unordered_map<const void *, rclcpp::ClientBase::WeakPtr> weak_clients_map_;
  std::unordered_map<const void *, rclcpp::Waitable::WeakPtr> weak_waitables_map_;

  /// Executor using this entities collector object
  EventsExecutor * associated_executor_ = nullptr;
  /// Instance of the timers manager used by the associated executor
  TimersManager::SharedPtr timers_manager_;
};

}  // namespace detail
}  // namespace executors
}  // namespace rclcpp

#endif  // RCLCPP__EXECUTORS__DETAIL__EVENTS_EXECUTOR_ENTITIES_COLLECTOR_HPP_
