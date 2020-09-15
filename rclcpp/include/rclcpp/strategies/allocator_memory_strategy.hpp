// Copyright 2015 Open Source Robotics Foundation, Inc.
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

#ifndef RCLCPP__STRATEGIES__ALLOCATOR_MEMORY_STRATEGY_HPP_
#define RCLCPP__STRATEGIES__ALLOCATOR_MEMORY_STRATEGY_HPP_

#include <memory>
#include <vector>

#include "rcl/allocator.h"

#include "rclcpp/allocator/allocator_common.hpp"
#include "rclcpp/memory_strategy.hpp"
#include "rclcpp/node.hpp"
#include "rclcpp/visibility_control.hpp"

#include "rcutils/logging_macros.h"

#include "rmw/types.h"

namespace rclcpp
{
namespace memory_strategies
{
namespace allocator_memory_strategy
{

/// Delegate for handling memory allocations while the Executor is executing.
/**
 * By default, the memory strategy dynamically allocates memory for structures that come in from
 * the rmw implementation after the executor waits for work, based on the number of entities that
 * come through.
 */
template<typename Alloc = std::allocator<void>>
class AllocatorMemoryStrategy : public memory_strategy::MemoryStrategy
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(AllocatorMemoryStrategy<Alloc>)

  using VoidAllocTraits = typename allocator::AllocRebind<void *, Alloc>;
  using VoidAlloc = typename VoidAllocTraits::allocator_type;

  explicit AllocatorMemoryStrategy(std::shared_ptr<Alloc> allocator)
  {
    allocator_ = std::make_shared<VoidAlloc>(*allocator.get());
  }

  AllocatorMemoryStrategy()
  {
    allocator_ = std::make_shared<VoidAlloc>();
  }

  void add_guard_condition(const rcl_guard_condition_t * guard_condition) override
  {
    for (const auto & existing_guard_condition : guard_conditions_) {
      if (existing_guard_condition == guard_condition) {
        return;
      }
    }
    guard_conditions_.push_back(guard_condition);
  }

  void remove_guard_condition(const rcl_guard_condition_t * guard_condition) override
  {
    for (auto it = guard_conditions_.begin(); it != guard_conditions_.end(); ++it) {
      if (*it == guard_condition) {
        guard_conditions_.erase(it);
        break;
      }
    }
  }

  void clear_handles() override
  {
    subscription_handles_.clear();
    subscription_hooks_.clear();
    service_handles_.clear();
    service_hooks_.clear();
    client_handles_.clear();
    client_hooks_.clear();
    guard_condition_hooks_.clear();
    timer_handles_.clear();
    waitable_handles_.clear();
    waitable_hooks_.clear();
  }

  void remove_null_handles(rcl_wait_set_t * wait_set) override
  {
    // TODO(jacobperron): Check if wait set sizes are what we expect them to be?
    //                    e.g. wait_set->size_of_clients == client_handles_.size()

    // Important to use subscription_handles_.size() instead of wait set's size since
    // there may be more subscriptions in the wait set due to Waitables added to the end.
    // The same logic applies for other entities.
    for (size_t i = 0; i < subscription_handles_.size(); ++i) {
      if (!wait_set->subscriptions[i]) {
        subscription_handles_[i].reset();
      }
    }
    for (size_t i = 0; i < service_handles_.size(); ++i) {
      if (!wait_set->services[i]) {
        service_handles_[i].reset();
      }
    }
    for (size_t i = 0; i < client_handles_.size(); ++i) {
      if (!wait_set->clients[i]) {
        client_handles_[i].reset();
      }
    }
    for (size_t i = 0; i < timer_handles_.size(); ++i) {
      if (!wait_set->timers[i]) {
        timer_handles_[i].reset();
      }
    }
    for (size_t i = 0; i < waitable_handles_.size(); ++i) {
      if (!waitable_handles_[i]->is_ready(wait_set)) {
        waitable_handles_[i].reset();
      }
    }

    subscription_handles_.erase(
      std::remove(subscription_handles_.begin(), subscription_handles_.end(), nullptr),
      subscription_handles_.end()
    );

    service_handles_.erase(
      std::remove(service_handles_.begin(), service_handles_.end(), nullptr),
      service_handles_.end()
    );

    client_handles_.erase(
      std::remove(client_handles_.begin(), client_handles_.end(), nullptr),
      client_handles_.end()
    );

    timer_handles_.erase(
      std::remove(timer_handles_.begin(), timer_handles_.end(), nullptr),
      timer_handles_.end()
    );

    waitable_handles_.erase(
      std::remove(waitable_handles_.begin(), waitable_handles_.end(), nullptr),
      waitable_handles_.end()
    );
  }

  bool collect_entities(const WeakNodeList & weak_nodes, void * exec_context, Event_callback cb)
  {
    bool has_invalid_weak_nodes = false;
    for (auto & weak_node : weak_nodes) {
      auto node = weak_node.lock();
      if (!node) {
        has_invalid_weak_nodes = true;
        continue;
      }
      for (auto & weak_group : node->get_callback_groups()) {
        auto group = weak_group.lock();
        if (!group || !group->can_be_taken_from().load()) {
          continue;
        }
        group->find_subscription_ptrs_if(
          [this, exec_context, cb](const rclcpp::SubscriptionBase::SharedPtr & subscription) {
            subscription_handles_.push_back(subscription->get_subscription_handle());
            subscription_hooks_.push_back({exec_context, subscription.get(), cb});
            return false;
          });
        group->find_service_ptrs_if(
          [this, exec_context, cb](const rclcpp::ServiceBase::SharedPtr & service) {
            service_handles_.push_back(service->get_service_handle());
            service_hooks_.push_back({exec_context, service.get(), cb});
            return false;
          });
        group->find_client_ptrs_if(
          [this, exec_context, cb](const rclcpp::ClientBase::SharedPtr & client) {
            client_handles_.push_back(client->get_client_handle());
            client_hooks_.push_back({exec_context, client.get(), cb});
            return false;
          });
        group->find_timer_ptrs_if(
          [this](const rclcpp::TimerBase::SharedPtr & timer) {
            timer_handles_.push_back(timer->get_timer_handle());
            return false;
          });
        group->find_waitable_ptrs_if(
          [this, exec_context, cb](const rclcpp::Waitable::SharedPtr & waitable) {
            waitable_handles_.push_back(waitable);
            waitable_hooks_.push_back({exec_context, waitable.get(), cb});
            return false;
          });
      }
    }
    // Add guard conditions event hooks.
    // These guard conditions are part of the memory strategy, but not part of any waitable.
    // These guard condtiions are not used by the current executors more that for interrupting
    // the rcl_wait and do nothing when it happens.
    // My proposal: All used guard conditions should belong to waitables, so the ros2 handle for
    // the event queue is the waitable handle, not the guard condition.
    // If we take that approach, the code below won't be needed anymore because it'll be done
    // by the waitables apis.
    for (auto it = guard_conditions_.begin(); it != guard_conditions_.end(); ++it) {
      guard_condition_hooks_.push_back({exec_context, static_cast<void *>(&*it), cb});
    }
    return has_invalid_weak_nodes;
  }

  void add_waitable_handle(const rclcpp::Waitable::SharedPtr & waitable, void * exec_context, Event_callback cb) override
  {
    if (nullptr == waitable) {
      throw std::runtime_error("waitable object unexpectedly nullptr");
    }
    waitable_handles_.push_back(waitable);
    waitable_hooks_.push_back({exec_context, waitable.get(), cb});
  }

  bool add_handles_to_wait_set(rcl_wait_set_t * wait_set) override
  {
    auto it_subscription = subscription_handles_.begin();
    auto it_subscription_hook = subscription_hooks_.begin();

    while(it_subscription != subscription_handles_.end() || it_subscription_hook != subscription_hooks_.end())
    {
      if(it_subscription != subscription_handles_.end() && it_subscription_hook != subscription_hooks_.end())
      {
        if (rcl_wait_set_add_subscription(wait_set, (*it_subscription).get(), static_cast<void *>(&*it_subscription_hook), NULL) != RCL_RET_OK) {
          RCUTILS_LOG_ERROR_NAMED(
            "rclcpp",
            "Couldn't add subscription to wait set: %s", rcl_get_error_string().str);
          return false;
        }

        it_subscription++;
        it_subscription_hook++;
      }
    }

    auto it_client = client_handles_.begin();
    auto it_client_hook = client_hooks_.begin();

    while(it_client != client_handles_.end() || it_client_hook != client_hooks_.end())
    {
      if(it_client != client_handles_.end() && it_client_hook != client_hooks_.end())
      {
        if (rcl_wait_set_add_client(wait_set, (*it_client).get(), static_cast<void *>(&*it_client_hook), NULL) != RCL_RET_OK) {
          RCUTILS_LOG_ERROR_NAMED(
            "rclcpp",
            "Couldn't add client to wait set: %s", rcl_get_error_string().str);
          return false;
        }

        it_client++;
        it_client_hook++;
      }
    }

    auto it_service = service_handles_.begin();
    auto it_service_hook = service_hooks_.begin();

    while(it_service != service_handles_.end() || it_service_hook != service_hooks_.end())
    {
      if(it_service != service_handles_.end() && it_service_hook != service_hooks_.end())
      {
        if (rcl_wait_set_add_service(wait_set, (*it_service).get(), static_cast<void *>(&*it_service_hook), NULL) != RCL_RET_OK) {
          RCUTILS_LOG_ERROR_NAMED(
            "rclcpp",
            "Couldn't add service to wait set: %s", rcl_get_error_string().str);
          return false;
        }

        it_service++;
        it_service_hook++;
      }
    }

    for (auto timer : timer_handles_) {
      if (rcl_wait_set_add_timer(wait_set, timer.get(), NULL) != RCL_RET_OK) {
        RCUTILS_LOG_ERROR_NAMED(
          "rclcpp",
          "Couldn't add timer to wait set: %s", rcl_get_error_string().str);
        return false;
      }
    }

    // Here are handled the guard conditions that are added to the memory strategy.
    // But the only guard conditions that are actually used, belong to waitables.
    // So in the new executor, we should only care for those.
    auto it_gc = guard_conditions_.begin();
    auto it_gc_hook = guard_condition_hooks_.begin();

    while(it_gc != guard_conditions_.end() || it_gc_hook != guard_condition_hooks_.end())
    {
      if(it_gc != guard_conditions_.end() && it_gc_hook != guard_condition_hooks_.end())
      {
        if (rcl_wait_set_add_guard_condition(wait_set, (*it_gc), static_cast<void *>(&*it_gc_hook), NULL) != RCL_RET_OK) {
          RCUTILS_LOG_ERROR_NAMED(
            "rclcpp",
            "Couldn't add service to wait set: %s", rcl_get_error_string().str);
          return false;
        }

        it_gc++;
        it_gc_hook++;
      }
    }

    auto it_w = waitable_handles_.begin();
    auto it_w_hook = waitable_hooks_.begin();

    while(it_w != waitable_handles_.end() || it_w_hook != waitable_hooks_.end())
    {
      if(it_w != waitable_handles_.end() && it_w_hook != waitable_hooks_.end())
      {
        if (!(*it_w)->add_to_wait_set(wait_set, static_cast<void *>(&*it_w_hook))) {
          RCUTILS_LOG_ERROR_NAMED(
            "rclcpp",
            "Couldn't add waitable to wait set: %s", rcl_get_error_string().str);
          return false;
        }
        it_w++;
        it_w_hook++;
      }
    }

    return true;
  }

  bool add_some_handles_to_wait_set(rcl_wait_set_t * wait_set) override
  {
    // auto it_subscription = subscription_handles_.begin();
    // auto it_ros2_handle = subscription_hooks_.begin();

    // while(it_subscription != subscription_handles_.end() || it_ros2_handle != subscription_hooks_.end())
    // {
    //   if(it_subscription != subscription_handles_.end() && it_ros2_handle != subscription_hooks_.end())
    //   {
    //     //std::cout << "allocator_mem_strategy: Add subscription pointer to rcl: " << *it_ros2_handle << std::endl;
    //     if (rcl_wait_set_add_subscription(wait_set, (*it_subscription).get(), *it_ros2_handle, NULL) != RCL_RET_OK) {
    //       RCUTILS_LOG_ERROR_NAMED(
    //         "rclcpp",
    //         "Couldn't add subscription to wait set: %s", rcl_get_error_string().str);
    //       return false;
    //     }

    //     it_subscription++;
    //     it_ros2_handle++;
    //   }
    // }

    // for (auto client : client_handles_) {
    //   if (rcl_wait_set_add_client(wait_set, client.get(), NULL) != RCL_RET_OK) {
    //     RCUTILS_LOG_ERROR_NAMED(
    //       "rclcpp",
    //       "Couldn't add client to wait set: %s", rcl_get_error_string().str);
    //     return false;
    //   }
    // }

    // for (auto service : service_handles_) {
    //   if (rcl_wait_set_add_service(wait_set, service.get(), NULL) != RCL_RET_OK) {
    //     RCUTILS_LOG_ERROR_NAMED(
    //       "rclcpp",
    //       "Couldn't add service to wait set: %s", rcl_get_error_string().str);
    //     return false;
    //   }
    // }

    for (auto timer : timer_handles_) {
      if (rcl_wait_set_add_timer(wait_set, timer.get(), NULL) != RCL_RET_OK) {
        RCUTILS_LOG_ERROR_NAMED(
          "rclcpp",
          "Couldn't add timer to wait set: %s", rcl_get_error_string().str);
        return false;
      }
    }

    // for (auto guard_condition : guard_conditions_) {
    //   if (rcl_wait_set_add_guard_condition(wait_set, guard_condition, NULL) != RCL_RET_OK) {
    //     RCUTILS_LOG_ERROR_NAMED(
    //       "rclcpp",
    //       "Couldn't add guard_condition to wait set: %s",
    //       rcl_get_error_string().str);
    //     return false;
    //   }
    // }

    // for (auto waitable : waitable_handles_) {
    //   if (!waitable->add_to_wait_set(wait_set)) {
    //     RCUTILS_LOG_ERROR_NAMED(
    //       "rclcpp",
    //       "Couldn't add waitable to wait set: %s", rcl_get_error_string().str);
    //     return false;
    //   }
    // }
    return true;
  }


  void
  get_next_subscription(
    rclcpp::AnyExecutable & any_exec,
    const WeakNodeList & weak_nodes) override
  {
    auto it = subscription_handles_.begin();
    while (it != subscription_handles_.end()) {
      auto subscription = get_subscription_by_handle(*it, weak_nodes);
      if (subscription) {
        // Find the group for this handle and see if it can be serviced
        auto group = get_group_by_subscription(subscription, weak_nodes);
        if (!group) {
          // Group was not found, meaning the subscription is not valid...
          // Remove it from the ready list and continue looking
          it = subscription_handles_.erase(it);
          continue;
        }
        if (!group->can_be_taken_from().load()) {
          // Group is mutually exclusive and is being used, so skip it for now
          // Leave it to be checked next time, but continue searching
          ++it;
          continue;
        }
        // Otherwise it is safe to set and return the any_exec
        any_exec.subscription = subscription;
        any_exec.callback_group = group;
        any_exec.node_base = get_node_by_group(group, weak_nodes);
        subscription_handles_.erase(it);
        return;
      }
      // Else, the subscription is no longer valid, remove it and continue
      it = subscription_handles_.erase(it);
    }
  }

  void
  get_next_service(
    rclcpp::AnyExecutable & any_exec,
    const WeakNodeList & weak_nodes) override
  {
    auto it = service_handles_.begin();
    while (it != service_handles_.end()) {
      auto service = get_service_by_handle(*it, weak_nodes);
      if (service) {
        // Find the group for this handle and see if it can be serviced
        auto group = get_group_by_service(service, weak_nodes);
        if (!group) {
          // Group was not found, meaning the service is not valid...
          // Remove it from the ready list and continue looking
          it = service_handles_.erase(it);
          continue;
        }
        if (!group->can_be_taken_from().load()) {
          // Group is mutually exclusive and is being used, so skip it for now
          // Leave it to be checked next time, but continue searching
          ++it;
          continue;
        }
        // Otherwise it is safe to set and return the any_exec
        any_exec.service = service;
        any_exec.callback_group = group;
        any_exec.node_base = get_node_by_group(group, weak_nodes);
        service_handles_.erase(it);
        return;
      }
      // Else, the service is no longer valid, remove it and continue
      it = service_handles_.erase(it);
    }
  }

  void
  get_next_client(rclcpp::AnyExecutable & any_exec, const WeakNodeList & weak_nodes) override
  {
    auto it = client_handles_.begin();
    while (it != client_handles_.end()) {
      auto client = get_client_by_handle(*it, weak_nodes);
      if (client) {
        // Find the group for this handle and see if it can be serviced
        auto group = get_group_by_client(client, weak_nodes);
        if (!group) {
          // Group was not found, meaning the service is not valid...
          // Remove it from the ready list and continue looking
          it = client_handles_.erase(it);
          continue;
        }
        if (!group->can_be_taken_from().load()) {
          // Group is mutually exclusive and is being used, so skip it for now
          // Leave it to be checked next time, but continue searching
          ++it;
          continue;
        }
        // Otherwise it is safe to set and return the any_exec
        any_exec.client = client;
        any_exec.callback_group = group;
        any_exec.node_base = get_node_by_group(group, weak_nodes);
        client_handles_.erase(it);
        return;
      }
      // Else, the service is no longer valid, remove it and continue
      it = client_handles_.erase(it);
    }
  }

  void
  get_next_timer(
    rclcpp::AnyExecutable & any_exec,
    const WeakNodeList & weak_nodes) override
  {
    auto it = timer_handles_.begin();
    while (it != timer_handles_.end()) {
      auto timer = get_timer_by_handle(*it, weak_nodes);
      if (timer) {
        // Find the group for this handle and see if it can be serviced
        auto group = get_group_by_timer(timer, weak_nodes);
        if (!group) {
          // Group was not found, meaning the timer is not valid...
          // Remove it from the ready list and continue looking
          it = timer_handles_.erase(it);
          continue;
        }
        if (!group->can_be_taken_from().load()) {
          // Group is mutually exclusive and is being used, so skip it for now
          // Leave it to be checked next time, but continue searching
          ++it;
          continue;
        }
        // Otherwise it is safe to set and return the any_exec
        any_exec.timer = timer;
        any_exec.callback_group = group;
        any_exec.node_base = get_node_by_group(group, weak_nodes);
        timer_handles_.erase(it);
        return;
      }
      // Else, the service is no longer valid, remove it and continue
      it = timer_handles_.erase(it);
    }
  }

  void
  get_next_waitable(rclcpp::AnyExecutable & any_exec, const WeakNodeList & weak_nodes) override
  {
    auto it = waitable_handles_.begin();
    while (it != waitable_handles_.end()) {
      auto waitable = *it;
      if (waitable) {
        // Find the group for this handle and see if it can be serviced
        auto group = get_group_by_waitable(waitable, weak_nodes);
        if (!group) {
          // Group was not found, meaning the waitable is not valid...
          // Remove it from the ready list and continue looking
          it = waitable_handles_.erase(it);
          continue;
        }
        if (!group->can_be_taken_from().load()) {
          // Group is mutually exclusive and is being used, so skip it for now
          // Leave it to be checked next time, but continue searching
          ++it;
          continue;
        }
        // Otherwise it is safe to set and return the any_exec
        any_exec.waitable = waitable;
        any_exec.callback_group = group;
        any_exec.node_base = get_node_by_group(group, weak_nodes);
        waitable_handles_.erase(it);
        return;
      }
      // Else, the waitable is no longer valid, remove it and continue
      it = waitable_handles_.erase(it);
    }
  }

  rcl_allocator_t get_allocator() override
  {
    return rclcpp::allocator::get_rcl_allocator<void *, VoidAlloc>(*allocator_.get());
  }

  size_t number_of_ready_subscriptions() const override
  {
    size_t number_of_subscriptions = subscription_handles_.size();
    for (auto waitable : waitable_handles_) {
      number_of_subscriptions += waitable->get_number_of_ready_subscriptions();
    }
    return number_of_subscriptions;
  }

  size_t number_of_ready_services() const override
  {
    size_t number_of_services = service_handles_.size();
    for (auto waitable : waitable_handles_) {
      number_of_services += waitable->get_number_of_ready_services();
    }
    return number_of_services;
  }

  size_t number_of_ready_events() const override
  {
    size_t number_of_events = 0;
    for (auto waitable : waitable_handles_) {
      number_of_events += waitable->get_number_of_ready_events();
    }
    return number_of_events;
  }

  size_t number_of_ready_clients() const override
  {
    size_t number_of_clients = client_handles_.size();
    for (auto waitable : waitable_handles_) {
      number_of_clients += waitable->get_number_of_ready_clients();
    }
    return number_of_clients;
  }

  size_t number_of_guard_conditions() const override
  {
    size_t number_of_guard_conditions = guard_conditions_.size();
    for (auto waitable : waitable_handles_) {
      number_of_guard_conditions += waitable->get_number_of_ready_guard_conditions();
    }
    return number_of_guard_conditions;
  }

  size_t number_of_ready_timers() const override
  {
    size_t number_of_timers = timer_handles_.size();
    for (auto waitable : waitable_handles_) {
      number_of_timers += waitable->get_number_of_ready_timers();
    }
    return number_of_timers;
  }

  size_t number_of_waitables() const override
  {
    return waitable_handles_.size();
  }

private:
  template<typename T>
  using VectorRebind =
    std::vector<T, typename std::allocator_traits<Alloc>::template rebind_alloc<T>>;

  VectorRebind<const rcl_guard_condition_t *> guard_conditions_;

  std::vector<EventHook> subscription_hooks_;
  std::vector<EventHook> service_hooks_;
  std::vector<EventHook> client_hooks_;
  std::vector<EventHook> guard_condition_hooks_; // Should not be used anymnore
  std::vector<EventHook> waitable_hooks_;
  VectorRebind<std::shared_ptr<const rcl_subscription_t>> subscription_handles_;
  VectorRebind<std::shared_ptr<const rcl_service_t>> service_handles_;
  VectorRebind<std::shared_ptr<const rcl_client_t>> client_handles_;
  VectorRebind<std::shared_ptr<const rcl_timer_t>> timer_handles_;
  VectorRebind<std::shared_ptr<Waitable>> waitable_handles_;

  std::shared_ptr<VoidAlloc> allocator_;
};

}  // namespace allocator_memory_strategy
}  // namespace memory_strategies
}  // namespace rclcpp

#endif  // RCLCPP__STRATEGIES__ALLOCATOR_MEMORY_STRATEGY_HPP_
