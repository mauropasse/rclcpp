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

#ifndef RCLCPP__EXPERIMENTAL__BUFFERS__BLOCKING_CONCURRENT_QUEUE_HPP_
#define RCLCPP__EXPERIMENTAL__BUFFERS__BLOCKING_CONCURRENT_QUEUE_HPP_

#include <queue>

#include "rclcpp/experimental/buffers/concurrent_queue/blockingconcurrentqueue.h"
#include "rclcpp/experimental/buffers/events_queue.hpp"

namespace rclcpp
{
namespace experimental
{
namespace buffers
{

/**
 * @brief This class implements an EventsQueue as a simple wrapper around
 * the blockingconcurrentqueue.h
 * See https://github.com/cameron314/concurrentqueue
 * It does not perform any checks about the size of queue, which can grow
 * unbounded without being pruned. (there are options about this, read the docs).
 * This implementation is lock free, producers and consumers can use the queue
 * concurrently without the need for synchronization mechanisms. The use of this
 * queue aims to fix the issue of publishers being blocked by the executor extracting
 * events from the queue in a different thread, causing expensive mutex contention.
 */
class BlockingConcurrentQueue : public EventsQueue
{
public:
  RCLCPP_PUBLIC
  ~BlockingConcurrentQueue() override
  {
    // It's important that all threads have finished using the queue
    // and the memory effects have fully propagated, before it is destructed.
    // Consume all events
    rclcpp::executors::ExecutorEvent event;
    while (event_queue_.try_dequeue(event)) {}
  }

  /**
   * @brief push event into the queue
   * @param event The event to push into the queue
   */
  RCLCPP_PUBLIC
  void
  push(const rclcpp::executors::ExecutorEvent & event) override
  {
    // The concurrent queue doesn't support front/pop operations.
    // It does both at once with try_dequeue or wait_dequeue.
    // This means we can't get an event and modify it while
    // it's still in the queue, as we do on get_single_event of the
    // SimpleEventsQueue. Due this, we need to push events separately,
    // allowing to take single events without the need to modify values
    // of the front event.
    rclcpp::executors::ExecutorEvent single_event = event;
    single_event.num_events = 1;
    for (size_t ev = 1; ev <= event.num_events; ev++ ) {
      event_queue_.enqueue(single_event);
    }
  }

  /**
   * @brief Stub for pop, as front method already pops it.
   */
  RCLCPP_PUBLIC
  void
  pop() override {}

  /**
   * @brief gets and pops the front event from the queue.
   * The event is removed from the queue after this operation.
   * Callers should make sure there's an event in the queue before calling.
   *
   * @return the front event
   */
  RCLCPP_PUBLIC
  rclcpp::executors::ExecutorEvent
  front() override
  {
    rclcpp::executors::ExecutorEvent event;
    bool has_event = event_queue_.try_dequeue(event);
    if (!has_event) {
      throw std::runtime_error("Called front() on empty queue");
    }
    return event;
  }

  /**
   * @brief Test whether queue is empty
   * @return true if the queue's size is 0, false otherwise.
   */
  RCLCPP_PUBLIC
  bool
  empty() const override
  {
    return event_queue_.size_approx() == 0;
  }

  /**
   * @brief Returns the number of elements in the queue.
   * This estimate is only accurate if the queue has completely
   * stabilized before it is called
   * @return the number of elements in the queue.
   */
  RCLCPP_PUBLIC
  size_t
  size() const override
  {
    return event_queue_.size_approx();
  }

  /**
   * @brief Initializes the queue
   */
  RCLCPP_PUBLIC
  void
  init() override
  {
    // Make sure the queue is empty when we start
    moodycamel::BlockingConcurrentQueue<rclcpp::executors::ExecutorEvent> local_queue;
    event_queue_.swap(local_queue);
  }

  /**
   * @brief gets a std queue with all events accumulated on it since
   * the last call. The member queue is empty when the call returns.
   * The use of this API should be avoided as makes a copy of events
   * in the queue, when there's no need for it since is a lock free
   * queue and other more efficient methods could be used
   * @return std::queue with events
   */
  RCLCPP_PUBLIC
  std::queue<rclcpp::executors::ExecutorEvent>
  pop_all_events() override
  {
    std::queue<rclcpp::executors::ExecutorEvent> local_queue;
    while (event_queue_.size_approx() != 0)
    {
      rclcpp::executors::ExecutorEvent single_event;
      event_queue_.wait_dequeue(single_event);
      local_queue.push(single_event);
    }
    return local_queue;
  }

  /**
   * @brief Get front event. Non blocking
   * @return a single event
   */
  RCLCPP_PUBLIC
  rclcpp::executors::ExecutorEvent
  get_single_event() override
  {
    // No need to decrement event counter, as this queue
    // only holds single events (event.num_events = 1)
    return front();
  }

  /**
   * @brief Checks if the underlying atomic variables used by
   * the queue are lock-free (they should be on most platforms).
   * @return true if the queue is lock free
   */
  RCLCPP_PUBLIC
  bool
  is_lock_free() const override
  {
    return event_queue_.is_lock_free();
  }

  /**
   * @brief waits for an event until timeout
   * @return true if event, false if timeout
   */
  RCLCPP_PUBLIC
  bool
  wait_for_event(
    rclcpp::executors::ExecutorEvent & event,
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds::max()) override
  {
    if (timeout != std::chrono::nanoseconds::max()) {
      return event_queue_.wait_dequeue_timed(event, timeout);
    }

    // If no timeout specified, just wait for an event to arrive
    event_queue_.wait_dequeue(event);
    return true;
  }

private:
  moodycamel::BlockingConcurrentQueue<rclcpp::executors::ExecutorEvent> event_queue_;
};

}  // namespace buffers
}  // namespace experimental
}  // namespace rclcpp


#endif  // RCLCPP__EXPERIMENTAL__BUFFERS__BLOCKING_CONCURRENT_QUEUE_HPP_
