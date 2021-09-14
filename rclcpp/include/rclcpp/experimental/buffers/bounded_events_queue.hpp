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

#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rclcpp/executors/events_executor_entities_collector.hpp"
#include "rclcpp/experimental/buffers/events_queue.hpp"

namespace rclcpp
{
namespace experimental
{
namespace buffers
{

/**
 * @brief This class implements an EventsQueue as ...
 */
class BoundedEventsQueue : public EventsQueue
{
public:
  RCLCPP_PUBLIC
  BoundedEventsQueue() = default;

  RCLCPP_PUBLIC
  ~BoundedEventsQueue()
  {
    event_queue_.clear();
    entity_id_to_events_map.clear();
  }

  /**
   * @brief push event into the queue
   * @param event The event to push into the queue
   */
  RCLCPP_PUBLIC
  virtual
  void
  push(const rclcpp::executors::ExecutorEvent & event)
  {
    if (!max_events_limit_reached(event)) {
      event_queue_.push_back(event);
    } else {
      remove_first_and_push_back(event);
    }
  }

  /**
   * @brief removes front event from the queue.
   */
  RCLCPP_PUBLIC
  virtual
  void
  pop()
  {
    auto & event = event_queue_.front();
    decrease_entity_events_count(event.exec_entity_id);
    event_queue_.erase(event_queue_.begin());
  }

  /**
   * @brief gets the front event from the queue
   * @return the front event
   */
  RCLCPP_PUBLIC
  virtual
  rclcpp::executors::ExecutorEvent
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
   * @brief Returns the number of elements in the queue.
   * @return the number of elements in the queue.
   */
  RCLCPP_PUBLIC
  virtual
  size_t
  size() const
  {
    return event_queue_.size();
  }

  /**
   * @brief Initializes the queue
   */
  RCLCPP_PUBLIC
  virtual
  void
  init()
  {
    std::runtime_error("Bounded events queue needs to be initialized with the entities collector.");
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

  /**
   * @brief gets a queue with all events accumulated on it since
   * the last call. The member queue is empty when the call returns.
   * @return std::queue with events
   */
  RCLCPP_PUBLIC
  virtual
  std::queue<rclcpp::executors::ExecutorEvent>
  pop_all_events()
  {
    std::queue<rclcpp::executors::ExecutorEvent> std_queue;
    for (auto & event : event_queue_) {
      std_queue.push(event);
    }
    event_queue_.clear();
    entity_id_to_events_map.clear();
    return std_queue;
  }

  /**
   * @brief gets a single entity event from the queue
   * and decrements the event counter.
   * @return a single event
   */
  RCLCPP_PUBLIC
  virtual
  rclcpp::executors::ExecutorEvent
  get_single_event()
  {
    rclcpp::executors::ExecutorEvent & front_event = event_queue_.front();

    if (front_event.num_events > 1) {
      // We have more than a single event for the entity.
      // Decrement the counter by one, keeping the event in the front.
      front_event.num_events--;
    } else {
      // We have a single event, remove it from queue.
      decrease_entity_events_count(front_event.exec_entity_id);
      event_queue_.erase(event_queue_.begin());
    }

    // Make sure we return a single event for the entity.
    rclcpp::executors::ExecutorEvent single_event = front_event;
    single_event.num_events = 1;

    return single_event;
  }

private:
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
      size_t entity_limit_events = entities_collector_->get_entity_qos_depth(event);
      auto entity_id_events = std::make_pair(1, entity_limit_events);
      entity_id_to_events_map[event.exec_entity_id] = entity_id_events;
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
  std::vector<rclcpp::executors::ExecutorEvent> event_queue_;
  typedef std::pair<size_t /*Current num events*/, size_t /*Max limit of events*/> EntityEvents;
  std::unordered_map<const void *, EntityEvents> entity_id_to_events_map;
  rclcpp::executors::EventsExecutorEntitiesCollector::SharedPtr entities_collector_;
};

}  // namespace buffers
}  // namespace experimental
}  // namespace rclcpp


#endif  // RCLCPP__EXPERIMENTAL__BUFFERS__BOUNDED_EVENTS_QUEUE_HPP_
