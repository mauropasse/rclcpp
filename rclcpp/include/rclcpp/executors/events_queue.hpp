// Copyright 2021 Open Source Robotics Foundation, Inc.
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

#ifndef RCLCPP__EXECUTORS__EVENTS_QUEUE_HPP_
#define RCLCPP__EXECUTORS__EVENTS_QUEUE_HPP_

#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "rclcpp/executor_options.hpp"
#include "rclcpp/executors/events_executor_entities_collector.hpp"
#include "rclcpp/macros.hpp"

#include "rmw/listener_event_types.h"

namespace rclcpp
{
namespace executors
{

/**
 * @brief This class provides a queue implementation which supports
 * different strategies for queueing and pruning events in case of
 * queue overruns.
 *
 */
class EventsQueue
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(EventsQueue)

  using EventQueue = std::deque<rmw_listener_event_t>;

  /**
   * @brief Construct a new EventsQueue object
   * @param collector The entities collector associated to this queue
   * @param options The event queue options
   */
  EventsQueue(
    EventsExecutorEntitiesCollector::SharedPtr collector,
    QueueOptions options);

  /**
   * @brief Destruct the object.
   */
  ~EventsQueue();

  /**
   * @brief swap EventQueue with another
   * @param event_queue The queue to swap with member queue
   */
  void swap(EventQueue & event_queue);

  /**
   * @brief Waits for an event to happen or timeout
   * @param timeout Time to wait and exit if no events received
   */
  void wait_for_event(std::chrono::nanoseconds timeout);

  /**
   * @brief Waits for an event and swap queues
   * @param event_queue The queue where the events will be swapped to
   */
  void wait_for_event_and_swap(EventQueue & event_queue);

  /**
   * @brief Waits for an event or timeout and swap queues
   * @param event_queue The queue where the events will be swapped to
   * @param timeout Time to wait and swap if no events received
   */
  void wait_for_event_and_swap(
    EventQueue & event_queue,
    std::chrono::nanoseconds timeout);

  /**
   * @brief Waits for an event or timeout and get first (oldest) event
   * @param event Where the event will be stored
   * @param timeout Time to wait and exit if no events received
   * @return true if there was an event, false if timeout time elapsed
   */
  bool wait_and_get_first_event(
    rmw_listener_event_t & event,
    std::chrono::nanoseconds timeout);

  /**
   * @brief push event into the queue and trigger cv
   * @param event The event to push into the queue
   */
  void push_and_notify(rmw_listener_event_t event);

private:
  RCLCPP_DISABLE_COPY(EventsQueue)

  // Function pointers to implement different queue strategies
  using PushFunctionPtr = std::function<void (rmw_listener_event_t)>;
  using ClearStatsFunctionPtr = std::function<void (void)>;

  PushFunctionPtr push_and_notify_implem_;
  ClearStatsFunctionPtr clear_stats_implem_;

  //
  // QueueStrategy::CPU_PERFORMANCE
  //

  /**
   * @brief push event into the queue and trigger cv
   * @param event The event to push into the queue
   */
  void simple_push(rmw_listener_event_t event);

  //
  // QueueStrategy::LIMITED_EVENTS_WITH_TIME_ORDERING
  //

  /**
   * @brief Follows policy in how to push to the queue
   * Before pushing, counts how many events came from the entity
   * and compares it with its QoS depth. It removes the old and
   * adds a new event if one has expired, se we keep time ordering
   * @param event The event to push into the queue
   */
  void limited_events_push(rmw_listener_event_t event);

  /**
   * @brief Remove oldest event and pushes new one in the back
   * @param event The event to remove and push back into the queue
   */
  void remove_oldest_event_and_push_back_new(rmw_listener_event_t event);

  /**
   * @brief Informs if the amount of subscriptions events reached the limit
   * @param subscription_id The subscription_id to obtain information
   * @return true if reached limit
   */
  bool subscription_events_reached_limit(const void * subscription_id);

  /**
   * @brief Informs if the amount of waitable events reached the limit
   * @param waitable_id The waitable_id to obtain information
   * @return true if reached limit
   */
  bool waitable_events_reached_limit(const void * waitable_id);

  /**
   * @brief Clears events queue stats
   */
  void clear_stats();

  //
  // QueueStrategy::QOS_BOUNDED
  //

  /**
   * @brief push event into the queue and trigger cv
   * @param event The event to push into the queue
   */
  void bounded_push(rmw_listener_event_t event);

  /**
   * @brief prune mechanism for qos bounded queue
   */
  void qos_bounded_prune();

  size_t queue_limit_;

  // The underlying queue
  EventQueue event_queue_;

  // Mutex to protect the insertion of events in the queue
  std::mutex push_mutex_;

  // Variable used to notify when an event is added to the queue
  std::condition_variable event_queue_cv_;

  // Entities collector associated with executor and events queue
  EventsExecutorEntitiesCollector::SharedPtr entities_collector_;

  // Maps: entity identifiers to number of events in the queue
  using EventsInQueueMap = std::unordered_map<const void *, size_t>;
  EventsInQueueMap subscription_events_in_queue_map_;
  EventsInQueueMap waitable_events_in_queue_map_;
};

}  // namespace executors
}  // namespace rclcpp

#endif  // RCLCPP__EXECUTORS__EVENTS_QUEUE_HPP_
