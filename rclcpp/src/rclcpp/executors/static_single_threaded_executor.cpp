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
#include <iomanip>

#include "rclcpp/scope_exit.hpp"

using rclcpp::executors::StaticSingleThreadedExecutor;
using rclcpp::experimental::ExecutableList;

StaticSingleThreadedExecutor::StaticSingleThreadedExecutor(
  const rclcpp::ExecutorOptions & options)
: rclcpp::Executor(options)
{
  entities_collector_ = std::make_shared<StaticExecutorEntitiesCollector>();
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
  entities_collector_->init(
    &wait_set_,
    memory_strategy_,
    &interrupt_guard_condition_,
    this,
    &StaticSingleThreadedExecutor::push_event,
    &m_exec_list_mutex_);

  std::thread t_exec_events(&StaticSingleThreadedExecutor::execute_events, this);

  pthread_setname_np(t_exec_events.native_handle(), "execute_events_thread");

  while (rclcpp::ok(this->context_) && spinning.load()) {
    // Refresh wait set and wait for work
    // entities_collector_->refresh_wait_set();
    execute_ready_executables();
  }

  // Print max elapsed time between push/pop into the queue
  // during the whole benchmark
  static std::mutex mutex_executor_;
  {
    std::unique_lock<std::mutex> lock(mutex_executor_);
    std::cout << "Event queue push/pop [min, max, avg] us = "
              << std::setw(8) << min_elapsed.count() << ", "
              << std::setw(8) << max_elapsed.count() << ", "
              << std::setw(8) << (total_elapsed.count() / num_pops)
              << "\n";

    std::cout << "Execute subscriptions max time us = " << sub_max_time.count()
              << std::endl;
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

  // Check in all the callback groups
  for (auto & weak_group : node_ptr->get_callback_groups()) {

    auto group = weak_group.lock();

    if (!group || !group->can_be_taken_from().load()) {
      continue;
    }

    group->find_timer_ptrs_if(
      [this](const rclcpp::TimerBase::SharedPtr & timer) {
        if (timer) {
        timers.add_timer(timer);
      }
      return false;
    });

  }
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
  // for (size_t i = 0; i < wait_set_.size_of_timers; ++i) {
  //   if (i < entities_collector_->get_number_of_timers()) {
  //     if (wait_set_.timers[i] && entities_collector_->get_timer(i)->is_ready()) {
  //       execute_timer(entities_collector_->get_timer(i));
  //     }
  //   }
  // }

  auto wait_timeout = timers.get_head_timeout();
  std::unique_lock<std::mutex> lock = std::unique_lock<std::mutex>(m_);
  // No need to check a predicate here, as it would be checked again right after.
  std::cv_status wait_status = cv_->wait_for(lock, wait_timeout);

  if (wait_status == std::cv_status::timeout) {
    timers.execute_ready_timers();
  }
}

void
StaticSingleThreadedExecutor::execute_events()
{
  // When condition variable is notified, check this predicate to proceed
  auto predicate = [this]() { return !event_queue.empty(); };

  // std::queue<EventQ> local_event_queue;
  std::queue<std::pair<TimePoint, EventQ>> local_event_queue;

  while(spinning.load())
  {
    // Scope block for the mutex
    {
      // We wait here until something has been pushed to the event queue
      std::unique_lock<std::mutex> lock(mutex_q_);
      cond_var_q_.wait(lock, predicate);

      // Swap queues
      swap(local_event_queue, event_queue);
    }

    // Mutex to protect the executable list from being
    // cleared while we still have events to process
    std::unique_lock<std::mutex> lock(m_exec_list_mutex_);

    // Execute events
    do {
      //EventQ event = local_event_queue.front();
      auto event = local_event_queue.front();

      // Compute delta time between push/pop to queue
      auto now = std::chrono::high_resolution_clock::now();

      auto push_pop_elapsed_time = now - event.first;

      // Store max push/pop elapsed time
      if(push_pop_elapsed_time > max_elapsed) {
        max_elapsed = push_pop_elapsed_time;
      }

      // Store min push/pop elapsed time
      if(push_pop_elapsed_time < min_elapsed) {
        min_elapsed = push_pop_elapsed_time;
      }

      // Store total push/pop elapsed time
      total_elapsed += push_pop_elapsed_time;
      num_pops++;

      local_event_queue.pop();

      switch(event.second.type)
      {
      case SUBSCRIPTION_EVENT:
        {
          auto now = std::chrono::high_resolution_clock::now();

          execute_subscription(std::move(entities_collector_->get_subscription_by_handle(event.second.entity)));

          auto then = std::chrono::high_resolution_clock::now();

          auto sub_elapsed_time = then - now;

          if(sub_elapsed_time > sub_max_time){
            sub_max_time = sub_elapsed_time;
          }

          break;
        }

      case SERVICE_EVENT:
        {
          execute_service(std::move(entities_collector_->get_service_by_handle(event.second.entity)));
          break;
        }

      case CLIENT_EVENT:
        {
          execute_client(std::move(entities_collector_->get_client_by_handle(event.second.entity)));
          break;
        }

       case GUARD_CONDITION_EVENT:
        {
          // Todo: Here we should get the waitable associated to the guard condition, check if ready and execute
          for (size_t i = 0; i < entities_collector_->get_number_of_waitables(); ++i) {
            if (entities_collector_->get_waitable(i)->is_ready(&wait_set_)) {
              entities_collector_->get_waitable(i)->execute();
            }
          }
          break;
        }

      }
    } while (!local_event_queue.empty());
  }
}
