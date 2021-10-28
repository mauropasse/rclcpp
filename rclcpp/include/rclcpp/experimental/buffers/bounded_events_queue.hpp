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

#ifndef RCLCPP__EXPERIMENTAL__BUFFERS__BOUNDED_EVENTS_QUEUE_HPP_
#define RCLCPP__EXPERIMENTAL__BUFFERS__BOUNDED_EVENTS_QUEUE_HPP_

#include <deque>
#include <utility>

#include "rclcpp/executors/events_queue.hpp"

namespace rclcpp
{
namespace experimental
{
namespace buffers
{

/**
 * @brief This class provides a bounded queue implementation
 * based on a std::queue. Before pushing events into the queue
 * checks the queue size. In case of exceeding the size it performs
 * a prune of the queue.
 */
class BoundedEventsQueue : public EventsQueue
{
public:
  RCLCPP_PUBLIC
  ~BoundedEventsQueue() = default;

  /**
   * @brief push event into the queue
   * @param event The event to push into the queue
   */
  RCLCPP_PUBLIC
  virtual
  void
  push(const rmw_listener_event_t & event)
  {
    if (event_queue_.size() >= queue_size_limit_) {
      // Option 1:
      // Uncommenting this would prune the queue by removing all elements
      // this->init();
      // Option 2:
      bounded_prune();
    }
    event_queue_.push_back(event);
  }

  /**
   * @brief removes front element from the queue
   * The element removed is the "oldest" element in the queue whose
   * value can be retrieved by calling member front().
   */
  RCLCPP_PUBLIC
  virtual
  void
  pop()
  {
    event_queue_.pop_front();
  }

  /**
   * @brief gets the front event from the queue
   * @return the front event
   */
  RCLCPP_PUBLIC
  virtual
  rmw_listener_event_t
  front() const
  {
    return event_queue_.front();
  }

  /**
   * @brief Test whether queue is empty
   * @return true if the queue's size is 0, false otherwise.
   */
  RCLCPP_PUBLIC
  virtual
  bool
  empty() const
  {
    return event_queue_.empty();
  }

  /**
   * @brief Initializes the queue
   */
  RCLCPP_PUBLIC
  virtual
  void
  init()
  {
    // Make sure the queue is empty when we start
    std::queue<rmw_listener_event_t> local_queue;
    std::swap(event_queue_, local_queue);
  }


  /**
   * @brief gets a queue with all events accumulated on it since
   * the last call. The member queue is empty when the call returns.
   * @return std::queue with events
   */
  RCLCPP_PUBLIC
  virtual
  std::queue<rmw_listener_event_t>
  get_all_events()
  {
    std::queue<rmw_listener_event_t> local_queue;
    std::swap(event_queue_, local_queue);
    return local_queue;
  }

  /**
   * @brief sets the queue size limit
   * @param limit The queue size limit
   */
  RCLCPP_PUBLIC
  void
  set_queue_size_limit(size_t queue_size_limit)
  {
    queue_size_limit_ = queue_size_limit;
  }

private:
  void bounded_prune()
  {
    // Clear maps
    qos_depth_waitables_map_.clear();
    qos_depth_subscriptions_map_.clear();
    waitable_events_in_queue_map_.clear();
    subscription_events_in_queue_map_.clear();

    // Prune queue:
    // For each entity, we get its QoS depth and use it as its events limit.
    // Starting from the newer events (backward iterating the queue) we
    // count events from each entity. If there are more events than the limit,
    // we discard the oldest events. The
    // For example, subscription A has depth = 1 / B has depth = 2
    //                           C has depth = 1 / D has depth = 1
    // If the queue is:
    //  Older Events (front of the queue)
    //    | D |
    //    | A | -> Should be removed, the msg has expired and overriden.
    //    | A | -> Should be removed
    //    | B | -> Should be removed        | D |
    //    | C |                             | C |
    //    | B |                             | B |
    //    | A |           --->              | A |
    //    | B |                             | B |
    //  Newer Events                    After pruning
    //
    EventQueue::reverse_iterator rit = event_queue_.rbegin();

    while (rit != event_queue_.rend()) {
      auto event = *rit;

      switch (event.type) {
        case SUBSCRIPTION_EVENT:
          {
            bool limit_reached = subscription_events_reached_limit(event.entity);

            if (limit_reached) {
              // Remove oldest events
              rit = decltype(rit)(event_queue_.erase(std::next(rit).base()));
            } else {
              rit++;
            }
            break;
          }

        case SERVICE_EVENT:
        case CLIENT_EVENT:
          {
            break;
          }

        case WAITABLE_EVENT:
          {
            bool limit_reached = waitable_events_reached_limit(event.entity);

            if (limit_reached) {
              // Remove oldest events
              rit = decltype(rit)(event_queue_.erase(std::next(rit).base()));
            } else {
              rit++;
            }
            break;
          }
      }
    }
  }

  bool subscription_events_reached_limit(const void * subscription_id)
  {
    auto limit = get_subscription_qos_depth(subscription_id);

    // If there's no limit, return false
    if (!limit) {
      throw std::runtime_error("BoundedEventsQueue requires a QoS depth with a limit.");
    }

    auto it = subscription_events_in_queue_map_.find(subscription_id);

    if (it != subscription_events_in_queue_map_.end()) {
      size_t & subscription_events_in_queue = it->second;

      if (subscription_events_in_queue < limit) {
        // Add one event as we still didn't reach the limit
        subscription_events_in_queue++;
        return false;
      } else {
        return true;
      }
    }

    // If the subscription_id is not present in the map, add it with one event in the counter
    subscription_events_in_queue_map_.emplace(subscription_id, 1);
    return false;
  }

  size_t get_subscription_qos_depth(const void * subscription_id)
  {
    // API YET TO BE DEFINED
    return 0;
  }

  bool waitable_events_reached_limit(const void * waitable_id)
  {
    auto limit = get_waitable_qos_depth(waitable_id);

    // If there's no limit, return false
    if (!limit) {
      throw std::runtime_error("BoundedEventsQueue requires a QoS depth with a limit.");
    }

    auto it = waitable_events_in_queue_map_.find(waitable_id);

    if (it != waitable_events_in_queue_map_.end()) {
      size_t & waitable_events_in_queue = it->second;

      if (waitable_events_in_queue < limit) {
        // Add one event as we still didn't reach the limit
        waitable_events_in_queue++;
        return false;
      } else {
        return true;
      }
    }

    // If the waitable_id is not present in the map, add it with one event in the counter
    waitable_events_in_queue_map_.emplace(waitable_id, 1);
    return false;
  }

  size_t get_waitable_qos_depth(const void * subscription_id)
  {
    // API YET TO BE DEFINED
    return 0;
  }

  std::deque<rmw_listener_event_t> event_queue_;

  size_t queue_size_limit_ = 1000;

  // Maps: entity identifiers to number of events in the queue
  using EventsInQueueMap = std::unordered_map<const void *, size_t>;
  EventsInQueueMap subscription_events_in_queue_map_;
  EventsInQueueMap waitable_events_in_queue_map_;

  // Maps: entity identifiers to qos->depth from the entities registered in the executor
  using QosDepthMap = std::unordered_map<const void *, size_t>;
  QosDepthMap qos_depth_subscriptions_map_;
  QosDepthMap qos_depth_waitables_map_;
};

}  // namespace buffers
}  // namespace experimental
}  // namespace rclcpp


#endif  // RCLCPP__EXPERIMENTAL__BUFFERS__BOUNDED_EVENTS_QUEUE_HPP_
