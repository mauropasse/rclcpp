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

#ifndef RCLCPP__EXPERIMENTAL__BUFFERS__SIMPLE_EVENTS_QUEUE_HPP_
#define RCLCPP__EXPERIMENTAL__BUFFERS__SIMPLE_EVENTS_QUEUE_HPP_

#include <mutex>
#include <queue>
#include <utility>

#include "rclcpp/experimental/buffers/events_queue.hpp"

namespace rclcpp
{
namespace experimental
{
namespace buffers
{

/**
 * @brief This class implements an EventsQueue as a simple wrapper around a std::queue.
 * It does not perform any checks about the size of queue, which can grow
 * unbounded without being pruned.
 * The simplicity of this implementation makes it suitable for optimizing CPU usage.
 */
class SimpleEventsQueue : public EventsQueue
{
public:
  RCLCPP_PUBLIC
  ~SimpleEventsQueue() override = default;

  /**
   * @brief push event into the queue
   * @param event The event to push into the queue
   */
  RCLCPP_PUBLIC
  void
  push(const rclcpp::executors::ExecutorEvent & event) override
  {
    {
      std::unique_lock<std::mutex> lock(this->push_mutex_);
      event_queue_.push(event);
    }
    events_queue_cv_.notify_one();
  }

  /**
   * @brief removes front event from the queue.
   */
  RCLCPP_PUBLIC
  void
  pop() override
  {
    std::unique_lock<std::mutex> lock(this->push_mutex_);
    event_queue_.pop();
  }

  /**
   * @brief gets the front event from the queue
   * @return the front event
   */
  RCLCPP_PUBLIC
  rclcpp::executors::ExecutorEvent
  front() override
  {
    std::unique_lock<std::mutex> lock(this->push_mutex_);
    return event_queue_.front();
  }

  /**
   * @brief Test whether queue is empty
   * @return true if the queue's size is 0, false otherwise.
   */
  RCLCPP_PUBLIC
  bool
  empty() const override
  {
    std::unique_lock<std::mutex> lock(this->push_mutex_);
    return event_queue_.empty();
  }

  /**
   * @brief Returns the number of elements in the queue.
   * @return the number of elements in the queue.
   */
  RCLCPP_PUBLIC
  size_t
  size() const override
  {
    std::unique_lock<std::mutex> lock(this->push_mutex_);
    return event_queue_.size();
  }

  /**
   * @brief Initializes the queue
   */
  RCLCPP_PUBLIC
  void
  init() override
  {
    // Make sure the queue is empty when we start
    std::unique_lock<std::mutex> lock(this->push_mutex_);
    std::queue<rclcpp::executors::ExecutorEvent> local_queue;
    std::swap(event_queue_, local_queue);
  }

  /**
   * @brief gets a queue with all events accumulated on it since
   * the last call. The member queue is empty when the call returns.
   * @return std::queue with events
   */
  RCLCPP_PUBLIC
  std::queue<rclcpp::executors::ExecutorEvent>
  pop_all_events() override
  {
    // When condition variable is notified, check this predicate to proceed
    auto has_event_predicate = [this]() {return !event_queue_.empty();};

    std::unique_lock<std::mutex> lock(this->push_mutex_);
    // We wait here until something has been pushed to the event queue
    events_queue_cv_.wait(lock, has_event_predicate);

    std::queue<rclcpp::executors::ExecutorEvent> local_queue;
    std::swap(event_queue_, local_queue);
    return local_queue;
  }

  /**
   * @brief gets a single entity event from the queue
   * and decrements the event counter.
   * @return a single event
   */
  RCLCPP_PUBLIC
  rclcpp::executors::ExecutorEvent
  get_single_event() override
  {
    std::unique_lock<std::mutex> lock(this->push_mutex_);
    return get_single_event_unsafe();
  }

  /**
   * @brief waits for an event until timeout, gets a single event
   * @return true if event, false if timeout
   */
  RCLCPP_PUBLIC
  bool
  wait_for_event(
    rclcpp::executors::ExecutorEvent & event,
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds::max()) override
  {
    auto has_event_predicate = [this]() {return !event_queue_.empty();};

    std::unique_lock<std::mutex> lock(this->push_mutex_);

    // We wait here until something has been pushed to the event queue or timeout
    if (timeout != std::chrono::nanoseconds::max()) {
      events_queue_cv_.wait_for(lock, timeout, has_event_predicate);
      if (event_queue_.empty()) {
        return false;
      } else {
        event = get_single_event_unsafe();
        return true;
      }
    } else {
      events_queue_cv_.wait(lock, has_event_predicate);
      event = get_single_event_unsafe();
      return true;
    }
  }

  /**
   * @brief Checks if the queue is lock free
   * @return true if the queue is lock free
   */
  RCLCPP_PUBLIC
  virtual
  bool
  is_lock_free() const override { return false; }

private:
  /**
   * @brief gets a single entity event from the queue
   * and decrements the event counter. This API is tied to the
   * ExecutorEvent
   * @return a single event
   */
  rclcpp::executors::ExecutorEvent
  get_single_event_unsafe()
  {
    rclcpp::executors::ExecutorEvent & front_event = event_queue_.front();
    if (front_event.num_events > 1) {
      // We have more than a single event for the entity.
      // Decrement the counter by one, keeping the event in the front.
      front_event.num_events--;
    } else {
      // We have a single event, pop it from queue.
      event_queue_.pop();
    }

    // Make sure we return a single event for the entity.
    rclcpp::executors::ExecutorEvent single_event = front_event;
    single_event.num_events = 1;

    return single_event;
  }

private:
  std::queue<rclcpp::executors::ExecutorEvent> event_queue_;
  // Mutex to protect the insertion/extraction of events in the queue
  mutable std::mutex push_mutex_;
  // Variable used to notify when an event is added to the queue
  std::condition_variable events_queue_cv_;

};

}  // namespace buffers
}  // namespace experimental
}  // namespace rclcpp


#endif  // RCLCPP__EXPERIMENTAL__BUFFERS__SIMPLE_EVENTS_QUEUE_HPP_
