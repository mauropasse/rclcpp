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

#include <mutex>
#include <queue>
#include <utility>

#include "rclcpp/experimental/buffers/events_queue.hpp"
#include "rclcpp/executors/events_executor_entities_collector.hpp"

namespace rclcpp
{
namespace experimental
{
namespace buffers
{

/**
 * @brief This class implements an EventsQueue as a wrapper around a std::vector.
 * It keeps track of amount of events in the queue for each entity.
 * Each entity has a limit of events which makes sense to have in the queue,
 * for example a subscription with QoS.depth = 5 shoud have as max 5 events in the queue.
 * If a 6th event for that subscription arrives, the 1st event in the queue is removed
 * and a new one is added at the back, to maintain correct ordering of events.
 * This way the queue doesn't grow un-bounded.
 */
class BoundedEventsQueue : public EventsQueue
{
public:
  RCLCPP_PUBLIC
  ~BoundedEventsQueue() override
  {
    event_queue_.clear();
    entity_id_to_events_map.clear();
  }

  /**
   * @brief enqueue event into the queue
   * If the limit of events per entity is surpassed, the 1st
   * event is removed and a new one is added at the back of the queue.
   * Thread safe
   * @param event The event to enqueue into the queue
   */
  RCLCPP_PUBLIC
  void
  enqueue(const rclcpp::executors::ExecutorEvent & event) override
  {
    rclcpp::executors::ExecutorEvent single_event = event;
    single_event.num_events = 1;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      for (size_t ev = 0; ev < event.num_events; ev++) {
        if (!max_events_limit_reached(event)) {
          event_queue_.push_back(single_event);
        } else {
          remove_first_and_push_back(event);
        }
      }
    }
    events_queue_cv_.notify_one();
  }

  /**
   * @brief waits for an event until timeout, gets a single event
   * (if any) and decreases the event count for the event entity.
   * Thread safe
   * @return true if event, false if timeout
   */
  RCLCPP_PUBLIC
  bool
  dequeue(
    rclcpp::executors::ExecutorEvent & event,
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds::max()) override
  {
    std::unique_lock<std::mutex> lock(mutex_);

    // Initialize to true because it's only needed if we have a valid timeout
    bool has_data = true;
    if (timeout != std::chrono::nanoseconds::max()) {
      has_data =
        events_queue_cv_.wait_for(lock, timeout, [this]() {return !event_queue_.empty();});
    } else {
      events_queue_cv_.wait(lock, [this]() {return !event_queue_.empty();});
    }

    if (has_data) {
      event = event_queue_.front();
      // Decrease the counter of events from this entity
      decrease_entity_events_count(event.exec_entity_id);
      // Remove first element from queue
      event_queue_.erase(event_queue_.begin());
      return true;
    }

    return false;
  }

  /**
   * @brief Test whether queue is empty
   * Thread safe
   * @return true if the queue's size is 0, false otherwise.
   */
  RCLCPP_PUBLIC
  bool
  empty() const override
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return event_queue_.empty();
  }

  /**
   * @brief Returns the number of elements in the queue.
   * Thread safe
   * @return the number of elements in the queue.
   */
  RCLCPP_PUBLIC
  size_t
  size() const override
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return event_queue_.size();
  }

  /**
   * @brief Initializes the queue
   */
  RCLCPP_PUBLIC
  virtual
  void
  init(rclcpp::executors::EventsExecutorEntitiesCollector::SharedPtr entities_collector)
  {
    entities_collector_ = entities_collector;
    // Make sure the queue is empty when we start
    event_queue_.clear();
  }

private:
  /**
   * @brief Compares current amount of events in the queue of a particular entity
   * with the maximum allowed number of events for it.
   * @return true if the limit of events in the queue has been reached for
   * the entity.
  */
  bool max_events_limit_reached(const rclcpp::executors::ExecutorEvent & event)
  {
    auto it = entity_id_to_events_map.find(event.exec_entity_id);

    if (it != entity_id_to_events_map.end()) {
      // Events from this entity ID were found in queue. Get number of them
      auto & entity_current_events_in_queue = it->second.first;
      auto & entity_id_limit_events_in_queue = it->second.second;

      if (entity_current_events_in_queue >= entity_id_limit_events_in_queue) {
        return true;
      } else {
        // Update count in map
        entity_current_events_in_queue++;
      }
    } else {
      // If event is not present in queue, add it to the map to track count of events
      if (entities_collector_) {
        size_t entity_limit_events = entities_collector_->get_entity_depth(event);
        auto entity_id_events = std::make_pair(1, entity_limit_events);
        entity_id_to_events_map[event.exec_entity_id] = entity_id_events;
      } else {
        throw std::runtime_error(
          "BoundedEventsQueue has to be initialized with entities collector.");
      }
    }
    // If event is not present in queue or hasn't reached limit, return false
    return false;
  }

  void decrease_entity_events_count(const void * entity_id)
  {
    auto it = entity_id_to_events_map.find(entity_id);

    if (it != entity_id_to_events_map.end()) {
      // Events from this entity ID were found in queue. Get number of them
      auto & entity_current_events_in_queue = it->second.first;

      if (entity_current_events_in_queue == 1) {
        // If we only have one event, remove entity ID from map.
        entity_id_to_events_map.erase(it);
      } else {
        entity_current_events_in_queue--;
      }
    }
  }

  void
  remove_first_and_push_back(const rclcpp::executors::ExecutorEvent & event)
  {
    for (auto it = event_queue_.begin(); it != event_queue_.end(); ++it) {
      if (it->exec_entity_id == event.exec_entity_id) {
        event_queue_.erase(it);
        break;
      }
    }

    event_queue_.push_back(event);
  }

private:
  // The underlying queue implementation
  std::vector<rclcpp::executors::ExecutorEvent> event_queue_;
  // Mutex to protect the insertion/extraction of events in the queue
  mutable std::mutex mutex_;
  // Variable used to notify when an event is added to the queue
  std::condition_variable events_queue_cv_;

  typedef std::pair<size_t /*Current num events*/, size_t /*Max limit of events*/> EntityEvents;
  std::unordered_map<const void *, EntityEvents> entity_id_to_events_map;
  rclcpp::executors::EventsExecutorEntitiesCollector::SharedPtr entities_collector_;

};

}  // namespace buffers
}  // namespace experimental
}  // namespace rclcpp


#endif  // RCLCPP__EXPERIMENTAL__BUFFERS__BOUNDED_EVENTS_QUEUE_HPP_
