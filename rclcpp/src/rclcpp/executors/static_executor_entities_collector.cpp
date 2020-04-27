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

#include "rclcpp/executors/static_executor_entities_collector.hpp"

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>

#include "rclcpp/memory_strategy.hpp"
#include "rclcpp/executors/static_single_threaded_executor.hpp"

using rclcpp::executors::StaticExecutorEntitiesCollector;

StaticExecutorEntitiesCollector::~StaticExecutorEntitiesCollector()
{
  // Disassociate all nodes
  for (auto & weak_node : weak_nodes_) {
    auto node = weak_node.lock();
    if (node) {
      std::atomic_bool & has_executor = node->get_associated_with_executor_atomic();
      has_executor.store(false);
    }
  }
  exec_list_.clear();
  weak_nodes_.clear();
  guard_conditions_.clear();
}

void
StaticExecutorEntitiesCollector::init(
  rcl_wait_set_t * p_wait_set,
  rclcpp::memory_strategy::MemoryStrategy::SharedPtr & memory_strategy,
  rcl_guard_condition_t * executor_guard_condition)
{
  // Empty initialize executable list
  exec_list_ = executor::ExecutableList();
  // Get executor's wait_set_ pointer
  p_wait_set_ = p_wait_set;
  // Get executor's memory strategy ptr
  if (memory_strategy == nullptr) {
    throw std::runtime_error("Received NULL memory strategy in executor waitable.");
  }
  memory_strategy_ = memory_strategy;

  // Add executor's guard condition
  guard_conditions_.push_back(executor_guard_condition);

  // Get memory strategy and executable list. Prepare wait_set_
  execute();
}

void
StaticExecutorEntitiesCollector::execute()
{
  // Fill memory strategy with entities coming from weak_nodes_
  fill_memory_strategy();
  // Fill exec_list_ with entities coming from weak_nodes_ (same as memory strategy)
  fill_executable_list();
  // Resize the wait_set_ based on memory_strategy handles (rcl_wait_set_resize)
  prepare_wait_set();
}

void
StaticExecutorEntitiesCollector::fill_memory_strategy()
{
  memory_strategy_->clear_handles();
  bool has_invalid_weak_nodes = memory_strategy_->collect_entities(weak_nodes_);

  // Clean up any invalid nodes, if they were detected
  if (has_invalid_weak_nodes) {
    auto node_it = weak_nodes_.begin();
    while (node_it != weak_nodes_.end()) {
      if (node_it->expired()) {
        node_it = weak_nodes_.erase(node_it);
      } else {
        ++node_it;
      }
    }
  }

  // Add the static executor waitable to the memory strategy
  memory_strategy_->add_waitable_handle(this->shared_from_this());
}

void
StaticExecutorEntitiesCollector::fill_executable_list()
{
  exec_list_.clear();

  for (auto & weak_node : weak_nodes_) {
    auto node = weak_node.lock();
    if (!node) {
      continue;
    }
    // Check in all the callback groups
    for (auto & weak_group : node->get_callback_groups()) {
      auto group = weak_group.lock();
      if (!group || !group->can_be_taken_from().load()) {
        continue;
      }

      group->find_timer_ptrs_if(
        [this](const rclcpp::TimerBase::SharedPtr & timer) {
          if (timer) {
            exec_list_.add_timer(timer);
          }
          return false;
        });
      group->find_subscription_ptrs_if(
        [this](const rclcpp::SubscriptionBase::SharedPtr & subscription) {
          if (subscription) {
            exec_list_.add_subscription(subscription);
          }
          return false;
        });
      group->find_service_ptrs_if(
        [this](const rclcpp::ServiceBase::SharedPtr & service) {
          if (service) {
            exec_list_.add_service(service);
          }
          return false;
        });
      group->find_client_ptrs_if(
        [this](const rclcpp::ClientBase::SharedPtr & client) {
          if (client) {
            exec_list_.add_client(client);
          }
          return false;
        });
      group->find_waitable_ptrs_if(
        [this](const rclcpp::Waitable::SharedPtr & waitable) {
          if (waitable) {
            exec_list_.add_waitable(waitable);
          }
          return false;
        });
    }
  }

  // Add the executor's waitable to the executable list
  std::cout << "Add waitable of entities collector" << std::endl;
  exec_list_.add_waitable(shared_from_this());
}

void
StaticExecutorEntitiesCollector::prepare_wait_set()
{
  // clear wait set
  if (rcl_wait_set_clear(p_wait_set_) != RCL_RET_OK) {
    throw std::runtime_error("Couldn't clear wait set");
  }

  // The size of waitables are accounted for in size of the other entities
  rcl_ret_t ret = rcl_wait_set_resize(
    p_wait_set_, memory_strategy_->number_of_ready_subscriptions(),
    memory_strategy_->number_of_guard_conditions(), memory_strategy_->number_of_ready_timers(),
    memory_strategy_->number_of_ready_clients(), memory_strategy_->number_of_ready_services(),
    memory_strategy_->number_of_ready_events());

  std::cout << "\nprepare_wait_set, resize. Number of gc: " << memory_strategy_->number_of_guard_conditions() << std::endl;

  if (RCL_RET_OK != ret) {
    throw std::runtime_error(
            std::string("Couldn't resize the wait set : ") + rcl_get_error_string().str);
  }
  // clear wait set (memeset to '0' all wait_set_ entities
  // but keeps the wait_set_ number of entities)
  std::cout << "\n rcl_wait_set_clear" << std::endl;
  if (rcl_wait_set_clear(p_wait_set_) != RCL_RET_OK) {
    throw std::runtime_error("Couldn't clear wait set");
  }

  std::cout << "\n memory_strategy_->add_handles_to_wait_set" << std::endl;
  if (!memory_strategy_->add_handles_to_wait_set(p_wait_set_)) {
    throw std::runtime_error("Couldn't fill wait set");
  }
}

void
StaticExecutorEntitiesCollector::refresh_wait_set(std::chrono::nanoseconds timeout)
{
  std::cout << "\n rcl_wait" << std::endl;

  rcl_ret_t status =
    rcl_wait(p_wait_set_, std::chrono::duration_cast<std::chrono::nanoseconds>(timeout).count());

  CddsWaitset * dds_wait_set = static_cast<CddsWaitset *>(p_wait_set_->impl->rmw_wait_set->data);

  get_executable_indexes(dds_wait_set);

  if (status == RCL_RET_WAIT_SET_EMPTY) {
    RCUTILS_LOG_WARN_NAMED(
      "rclcpp",
      "empty wait set received in rcl_wait(). This should never happen.");
  } else if (status != RCL_RET_OK && status != RCL_RET_TIMEOUT) {
    using rclcpp::exceptions::throw_from_rcl_error;
    throw_from_rcl_error(status, "rcl_wait() failed");
  }
}

void
StaticExecutorEntitiesCollector::get_executable_indexes(CddsWaitset * ws)
{
  std::cout << "\n get_executable_indexes" << std::endl;
  // Now we have a list with the indexes of triggered entities.
  // We need to find out to which entity each index belong.

  // ws->trigs has the list of triggered entities indexes.
  // For example: ws->trigs = [0,3,5,9,-1]
  // The last element is always -1 (where is this used?)

  // Reset the ready items numbers for each entity.
  ready_items[SUBSCRIBER] = 0;
  ready_items[GC] = 0;
  ready_items[SERVICE] = 0;
  ready_items[CLIENT] = 0;
  ready_items[EVENT] = 0;
  ready_items[TIMER] = 0;

  // If nothing was trigered: ws->trigs = [-1]
  if (ws->trigs.size() == 1) {
    return;
  }
  
  // Debug size of trig
  std::cout << "\nCollector: ws->trig_idxs.size() = " << ws->trigs.size() << std::endl;

  // The last ws->trigs is set to '-1', so don't iterate over it
  for (size_t i = 0; i < ws->trigs.size() - 1; i++)
  {
    size_t trig_idx = static_cast<size_t>(ws->trigs[i]);

    // Debug index 
    std::cout << "    trig_idx[" << i << "] = " << trig_idx << std::endl;
      
    // Here I'm following the same order of entities as in rmw_cyclonedds_cpp->rmw_node.cpp
    // 1. SUBSCRIBER
    // 2. GUARD CONDITIONS
    // 3. SERVICE
    // 4. CLIENT
    // 5. EVENT  

    // First we check if the indexes belong to subscriptions
    if(trig_idx < get_number_of_subscriptions()) {
      std::cout << "    belongs to a subscription" << std::endl;
      ready_subscriber[ready_items[SUBSCRIBER]] = trig_idx;
      ready_items[SUBSCRIBER]++;
    }
    // If trig_idx is bigger than number of subscriptions, they could be guard conditions, services, etc.
    // Here we make sure that we manage guard_conditions
    else // if(.. complete ..)
    {
      // Following: WRONG! FIX It's different.
      // Here we need to get the guards conditions number of each waitable,
      // to know which waitable should be executed
      //     Todo: Check if happens that guard conditions don't belong to waitables
      // 
      // For now I just want the guard conditions of the entities collector, the last waitable
      // added to the executable_list (I assume that guard conditions only belong to waitables)
      //
      // *  Node1:              Waitable1 -> gc1, gc2, ...
      //                        Waitable2 -> gc1, gc2, ...
      //  
      // *  Node2:              Waitable1 -> gc1, gc2, ...
      //  
      // The last waitable should be the entities collector:
      //  
      // * entities_collector: Waitable1 -> gc1, gc2, ...

      // Example case of triggered indexes
      // ws->trigs:
      //   0, subscription
      //   3, guard_condition of some waitable
      //   5, guard condition associated with the entities collector
      //   9, service, client, event ..
      //   -1,


      // Get number of total guard conditions
      // size_t total_guard_conditions = 0;

      // for (size_t i = 0; i < get_number_of_waitables(); i++) {
      //   auto waitable = get_waitable(i);
      //   total_guard_conditions += waitable->get_number_of_ready_guard_conditions();
      // }

      // size_t entitites_collector_gcs = get_number_of_ready_guard_conditions();

      // // So far trig_idx could be a gc, service, client, event
      // if (trig_idx < get_number_of_subscriptions() + total_guard_conditions){
      //   //trig_idx is a gc
      //   std::cout << "    belongs to a guard condition" << std::endl;

      //   if (trig_idx >= get_number_of_subscriptions() + total_guard_conditions - entitites_collector_gcs){
      //     //trig_idx is a gc belonging to the entities collector
      //     std::cout << "    which belongs to the entities collector" << std::endl;
      //     size_t guard_condition_index = trig_idx - get_number_of_subscriptions() - total_guard_conditions;
      //     ready_gc[ready_items[GC]] = guard_condition_index;
      //     ready_items[GC]++;
      //   }

      // } else {
      //   std::cout << "    who belongs to?" << std::endl;
      // }
    }
  }
}

bool
StaticExecutorEntitiesCollector::add_to_wait_set(rcl_wait_set_t * wait_set)
{
  // Add waitable guard conditions (one for each registered node) into the wait set.
  for (const auto & gc : guard_conditions_) {
    std::cout << "\nEntities collector: rcl_wait_set_add_guard_condition: " << gc << std::endl;
    rcl_ret_t ret = rcl_wait_set_add_guard_condition(wait_set, gc, NULL);
    if (ret != RCL_RET_OK) {
      throw std::runtime_error("Executor waitable: couldn't add guard condition to wait set");
    }
  }
  return true;
}

size_t StaticExecutorEntitiesCollector::get_number_of_ready_guard_conditions()
{
  return guard_conditions_.size();
}

void
StaticExecutorEntitiesCollector::add_node(
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr)
{
  // Check to ensure node not already added
  for (auto & weak_node : weak_nodes_) {
    auto node = weak_node.lock();
    if (node == node_ptr) {
      // TODO(jacquelinekay): Use a different error here?
      throw std::runtime_error("Cannot add node to executor, node already added.");
    }
  }

  weak_nodes_.push_back(node_ptr);
  guard_conditions_.push_back(node_ptr->get_notify_guard_condition());
}

bool
StaticExecutorEntitiesCollector::remove_node(
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr)
{
  auto node_it = weak_nodes_.begin();

  while (node_it != weak_nodes_.end()) {
    bool matched = (node_it->lock() == node_ptr);
    if (matched) {
      // Find and remove node and its guard condition
      auto gc_it = std::find(
        guard_conditions_.begin(),
        guard_conditions_.end(),
        node_ptr->get_notify_guard_condition());

      if (gc_it != guard_conditions_.end()) {
        guard_conditions_.erase(gc_it);
        weak_nodes_.erase(node_it);
        return true;
      }

      throw std::runtime_error("Didn't find guard condition associated with node.");

    } else {
      ++node_it;
    }
  }

  return false;
}

bool
StaticExecutorEntitiesCollector::is_ready(rcl_wait_set_t * p_wait_set)
{
  // At this point, only guard conditions of the entities collector
  // are present in ready_items[GC];
  (void)p_wait_set;
  size_t num_triggered_gc = ready_items[GC];
  
  if(num_triggered_gc){
    // return true; This is failing
  }

  // for (size_t i = 0; i < num_triggered_gc; ++i) {
  //   size_t gc_idx = ready_gc[i]; // do stuff ..
  // }

  // None of the guard conditions trig_idxgered belong to a registered node
  return false;
}
