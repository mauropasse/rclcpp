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

#include <utility>

#include "rclcpp/executors/events_queue.hpp"

using rclcpp::executors::EventsQueue;
using namespace std::placeholders;

EventsQueue::EventsQueue(
  EventsExecutorEntitiesCollector::SharedPtr entities_collector,
  QueueOptions queue_options)
: entities_collector_(entities_collector)
{
  switch (queue_options.queue_strategy) {
    case QueueStrategy::CPU_PERFORMANCE:
      {
        push_and_notify_implem_ = std::bind(
          &EventsQueue::simple_push, this, std::placeholders::_1);
        clear_stats_implem_ = []() {};
        break;
      }

    case QueueStrategy::LIMITED_EVENTS_WITH_TIME_ORDERING:
      {
        push_and_notify_implem_ = std::bind(
          &EventsQueue::limited_events_push, this, std::placeholders::_1);
        clear_stats_implem_ = std::bind(&EventsQueue::clear_stats, this);
        break;
      }

    case QueueStrategy::BOUNDED:
      {
        push_and_notify_implem_ = std::bind(
          &EventsQueue::bounded_push, this, std::placeholders::_1);
        clear_stats_implem_ = []() {};
        queue_limit_ = queue_options.max_events;
        break;
      }
  }
}

EventsQueue::~EventsQueue()
{
  subscription_events_in_queue_map_.clear();
  waitable_events_in_queue_map_.clear();
}

void EventsQueue::swap(EventQueue & event_queue)
{
  std::unique_lock<std::mutex> push_lock(push_mutex_);
  std::swap(event_queue, event_queue_);
  clear_stats_implem_();
}

void EventsQueue::wait_for_event(std::chrono::nanoseconds timeout)
{
  // When condition variable is notified, check this predicate to proceed
  auto has_event_predicate = [this]() {return !event_queue_.empty();};

  std::unique_lock<std::mutex> push_lock(push_mutex_);
  event_queue_cv_.wait_for(push_lock, timeout, has_event_predicate);
}

void EventsQueue::wait_for_event_and_swap(EventQueue & event_queue)
{
  // When condition variable is notified, check this predicate to proceed
  auto has_event_predicate = [this]() {return !event_queue_.empty();};

  std::unique_lock<std::mutex> push_lock(push_mutex_);
  // We wait here until something has been pushed to the event queue
  event_queue_cv_.wait(push_lock, has_event_predicate);
  // We got an event! Swap queues and clear maps
  std::swap(event_queue, event_queue_);
  clear_stats_implem_();
}

void EventsQueue::wait_for_event_and_swap(
  EventQueue & event_queue,
  std::chrono::nanoseconds timeout)
{
  // When condition variable is notified, check this predicate to proceed
  auto has_event_predicate = [this]() {return !event_queue_.empty();};

  std::unique_lock<std::mutex> push_lock(push_mutex_);
  // We wait here until something has been pushed to the queue
  // or if the timeout expired
  event_queue_cv_.wait_for(push_lock, timeout, has_event_predicate);
  // Time to swap queues as the wait is over
  std::swap(event_queue, event_queue_);
  clear_stats_implem_();
}

bool EventsQueue::wait_and_get_first_event(
  rmw_listener_event_t & event,
  std::chrono::nanoseconds timeout)
{
  // When condition variable is notified, check this predicate to proceed
  auto has_event_predicate = [this]() {return !event_queue_.empty();};

  // Wait until timeout or event arrives
  std::unique_lock<std::mutex> lock(push_mutex_);

  event_queue_cv_.wait_for(lock, timeout, has_event_predicate);

  // Grab first event (oldest) from queue if it exists
  bool has_event = !event_queue_.empty();

  if (has_event) {
    event = event_queue_.front();
    event_queue_.pop_front();
    return true;
  }

  // If the timeout expired without new events, return false
  return false;
}

//
// Queue Policies/Strategies implementations
//

void EventsQueue::push_and_notify(rmw_listener_event_t event)
{
  // This function depends on the queue policy selected
  push_and_notify_implem_(event);

  // Notify that we have an event in the queue
  event_queue_cv_.notify_one();
}

//
// QueueStrategy::CPU_PERFORMANCE
//

void EventsQueue::simple_push(rmw_listener_event_t event)
{
  std::unique_lock<std::mutex> lock(push_mutex_);
  event_queue_.push_back(event);
}

//
// QueueStrategy::LIMITED_EVENTS_WITH_TIME_ORDERING
//

void EventsQueue::limited_events_push(rmw_listener_event_t event)
{
  std::unique_lock<std::mutex> lock(push_mutex_);

  switch (event.type) {
    case SUBSCRIPTION_EVENT:
      {
        bool limit_reached = subscription_events_reached_limit(event.entity);

        if (limit_reached) {
          remove_oldest_event_and_push_back_new(event);
        } else {
          event_queue_.push_back(event);
          break;
        }
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
          remove_oldest_event_and_push_back_new(event);
        } else {
          event_queue_.push_back(event);
          break;
        }
      }
  }
}

bool EventsQueue::subscription_events_reached_limit(const void * subscription_id)
{
  auto limit = entities_collector_->get_subscription_qos_depth(subscription_id);

  // If there's no limit, return false
  if (!limit) {
    return false;
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

bool EventsQueue::waitable_events_reached_limit(const void * waitable_id)
{
  auto limit = entities_collector_->get_waitable_qos_depth(waitable_id);

  // If there's no limit, return false
  if (!limit) {
    return false;
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

void EventsQueue::remove_oldest_event_and_push_back_new(rmw_listener_event_t event)
{
  // Remove first event (oldest) associated with this entity from the queue
  EventQueue::iterator it;

  for (it = event_queue_.begin(); it != event_queue_.end(); it++) {
    if ((*it).entity == event.entity) {
      event_queue_.erase(it);
      break;
    }
  }

  // Push event to the back of the queue (where newest messages are located)
  event_queue_.push_back(event);
}

void EventsQueue::clear_stats()
{
  subscription_events_in_queue_map_.clear();
  waitable_events_in_queue_map_.clear();
}

//
// QueueStrategy::BOUNDED
//

void EventsQueue::bounded_push(rmw_listener_event_t event)
{
  std::unique_lock<std::mutex> lock(push_mutex_);

  if (event_queue_.size() >= queue_limit_) {
    bounded_prune();
  }
  event_queue_.push_back(event);
}

void EventsQueue::bounded_prune()
{
  // Start with fresh stats
  clear_stats();

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
