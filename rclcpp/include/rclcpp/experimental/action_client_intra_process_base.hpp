// Copyright 2022 Open Source Robotics Foundation, Inc.
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

#ifndef RCLCPP__EXPERIMENTAL__ACTION_CLIENT_INTRA_PROCESS_BASE_HPP_
#define RCLCPP__EXPERIMENTAL__ACTION_CLIENT_INTRA_PROCESS_BASE_HPP_

#include <algorithm>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "rmw/impl/cpp/demangle.hpp"

#include "rclcpp/context.hpp"
#include "rclcpp/guard_condition.hpp"
#include "rclcpp/logging.hpp"
#include "rclcpp/qos.hpp"
#include "rclcpp/type_support_decl.hpp"
#include "rclcpp/waitable.hpp"

namespace rclcpp
{
namespace experimental
{

class ActionClientIntraProcessBase : public rclcpp::Waitable
{
public:
  RCLCPP_SMART_PTR_ALIASES_ONLY(ActionClientIntraProcessBase)

  // The action client event types
  enum class EventType : std::size_t
  {
    ResultResponse,
    CancelResponse,
    GoalResponse,
    FeedbackReady,
    StatusReady,
  };

  // Alias for the type used for the server responses callback
  using ResponseCallback = std::function<void (std::shared_ptr<void> /*server response*/)>;

  // Define a structure to hold event information
  struct EventInfo {
    // The event type
    EventType event_type;
    // The callback to be called with the responses from the server
    ResponseCallback response_callback;
    // Flag to determine if there was an event but "response_callback" was not set
    // Only applies to ResultResponse: The ActionServer can respond faster than the client
    // setting the "response_callback"
    bool has_event;
    // Counter of events received before the "on_ready" callback was set
    size_t unread_count;
  };

  // Map the Goal ID with EventInfo
  std::unordered_multimap<size_t /*Goal ID*/, EventInfo> event_info_multi_map_;

  RCLCPP_PUBLIC
  ActionClientIntraProcessBase(
    rclcpp::Context::SharedPtr context,
    const std::string & action_name,
    const rclcpp::QoS & qos_profile)
  : gc_(context),
    action_name_(action_name),
    qos_profile_(qos_profile)
  {}

  virtual ~ActionClientIntraProcessBase() = default;

  RCLCPP_PUBLIC
  size_t
  get_number_of_ready_guard_conditions() override {return 1;}

  RCLCPP_PUBLIC
  void
  add_to_wait_set(rcl_wait_set_t * wait_set) override;

  bool
  is_ready(rcl_wait_set_t * wait_set) override = 0;

  std::shared_ptr<void>
  take_data() override = 0;

  std::shared_ptr<void>
  take_data_by_entity_id(size_t id) override = 0;

  void
  execute(std::shared_ptr<void> & data) override = 0;

  RCLCPP_PUBLIC
  const char *
  get_action_name() const;

  RCLCPP_PUBLIC
  QoS
  get_actual_qos() const;

  /// Set a callback to be called when each new response arrives.
  /**
   * The callback receives a size_t which is the number of responses received
   * since the last time this callback was called.
   * Normally this is 1, but can be > 1 if responses were received before any
   * callback was set.
   *
   * The callback also receives an int identifier argument.
   * This is needed because a Waitable may be composed of several distinct entities,
   * such as subscriptions, services, etc. In this case they identify event types.
   * The application should provide a generic callback function that will be then
   * forwarded by the waitable to all of its entities.
   * Before forwarding, a different value for the identifier argument will be
   * bound to the function.
   * This implies that the provided callback can use the identifier to behave
   * differently depending on which entity triggered the waitable to become ready.
   *
   * Calling it again will clear any previously set callback.
   *
   * An exception will be thrown if the callback is not callable.
   *
   * This function is thread-safe.
   *
   * If you want more information available in the callback, like the subscription
   * or other information, you may use a lambda with captures or std::bind.
   *
   * \param[in] callback functor to be called when a new request is received.
   */
  void
  set_on_ready_callback(std::function<void(size_t, int)> callback) override
  {
    if (!callback) {
      throw std::invalid_argument(
              "The callback passed to set_on_ready_callback "
              "is not callable.");
    }
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);

    on_ready_callback_ = callback;

    // If we had events happened before the "on_ready" callback was set,
    // call the callback now with the events counter "unread_count".
    // But we first have to verify that also the "response_callback" was set:
    // Otherwise we'll push events in the queue, that when processed, will try to
    // use a not-yet set "reponse_callback" (this problem actually can only happen for
    // ResultResponse events, if the server finishes the goal
    // (on_terminal_state called) before async_get_result is called)
    for (auto& pair : event_info_multi_map_) {
      auto & unread_count = pair.second.unread_count;
      auto & event_type = pair.second.event_type;
      auto & response_callback = pair.second.response_callback;
      if (unread_count > 0) {
        if (response_callback) {
          // The "response_callback" was set, so we can safely generate the
          // events
          on_ready_callback_(unread_count, static_cast<int>(event_type));
          unread_count = 0;
        } else {
          // The "response_callback" was not set!
          // Mark that we have events, so when the "response_callback" is set,
          // we call the "on_ready_callback"
          pair.second.has_event = true;
        }
      }
    }
  }

  void
  clear_on_ready_callback() override
  {
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);
    on_ready_callback_ = nullptr;
  }

  void clear_expired_goals(size_t goal_id)
  {
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);
    event_info_multi_map_.erase(goal_id);
  }

protected:
  std::recursive_mutex reentrant_mutex_;
  rclcpp::GuardCondition gc_;

  // Generic events callback
  std::function<void(size_t, int)> on_ready_callback_{nullptr};

  // Function to set callback to event type
  void set_response_callback_to_event_type(
    EventType event_type,
    ResponseCallback response_callback,
    size_t goal_id = 0)
  {
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);

    // Get the range of EventInfo matching the goal_id
    auto range = event_info_multi_map_.equal_range(goal_id);

    for (auto it = range.first; it != range.second; ++it) {
      if (it->second.event_type == event_type) {
        it->second.response_callback = response_callback;
        return;
      }
    }

    // If no entry found, create a new one.
    EventInfo event_info{event_type, response_callback, false, 0};
    event_info_multi_map_.emplace(goal_id, event_info);
  }

  // Function to invoke callback on ready.
  // If the callback was not set at the time of calling, increase the unread count.
  void invoke_on_ready_callback(
    EventType event_type,
    size_t goal_id = 0)
  {
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);

    auto range = event_info_multi_map_.equal_range(goal_id);
    for (auto it = range.first; it != range.second; ++it) {
      if (it->second.event_type == event_type) {
        if (on_ready_callback_) {
          on_ready_callback_(1, static_cast<int>(event_type));
        } else {
          it->second.unread_count++;
        }
        return;
      }
    }

    // If no entry found, create a new one with unread_count = 1
    EventInfo event_info{event_type, nullptr, false, 1};
    event_info_multi_map_.emplace(goal_id, event_info);
  }

  // Function to retrieve a reference to an EventInfo matching the hashed_guuid and EventType
  EventInfo& get_event_info(size_t goal_id, EventType event_type)
  {
    auto range = event_info_multi_map_.equal_range(goal_id);
    for (auto it = range.first; it != range.second; ++it) {
      if (it->second.event_type == event_type) {
        return it->second;
      }
    }

    // If no matching EventInfo found, create a new one
    EventInfo event_info{event_type, nullptr, false, 0};
    auto it = event_info_multi_map_.emplace(goal_id, event_info);
    return it->second;
  }

  // Function to return a copy to the callback for a particular goal_id and event_type
  ResponseCallback get_callback_for_event_type(size_t goal_id, EventType event_type)
  {
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);

    auto range = event_info_multi_map_.equal_range(goal_id);
    for (auto it = range.first; it != range.second; ++it) {
      if (it->second.event_type == event_type) {
        return it->second.response_callback;
      }
    }

    // If no callback found, return a default-constructed std::function
    return ResponseCallback{};
  }

  // Function to remove an entry from event_info_multi_map_ for a particular goal_id and EventType
  void remove_entry_from_event_info_multi_map_(size_t goal_id, EventType event_type)
  {
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);

    auto range = event_info_multi_map_.equal_range(goal_id);
    for (auto it = range.first; it != range.second; ++it) {
      if (it->second.event_type == event_type) {
        event_info_multi_map_.erase(it);
        return;
      }
    }
  }

private:
  std::string action_name_;
  QoS qos_profile_;
};

}  // namespace experimental
}  // namespace rclcpp

#endif  // RCLCPP__EXPERIMENTAL__ACTION_CLIENT_INTRA_PROCESS_BASE_HPP_
