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

#ifndef RCLCPP__EXPERIMENTAL__ACTION_CLIENT_INTRA_PROCESS_HPP_
#define RCLCPP__EXPERIMENTAL__ACTION_CLIENT_INTRA_PROCESS_HPP_

#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>
#include <variant>  // NOLINT, cpplint doesn't think this is a cpp std header

#include "rcutils/logging_macros.h"
#include "rclcpp/experimental/buffers/intra_process_buffer.hpp"
#include "rclcpp/experimental/create_intra_process_buffer.hpp"
#include "rclcpp/experimental/action_client_intra_process_base.hpp"

typedef struct rcl_action_client_depth_s
{
  size_t goal_service_depth;
  size_t result_service_depth;
  size_t cancel_service_depth;
  size_t feedback_topic_depth;
  size_t status_topic_depth;
} rcl_action_client_depth_t;

namespace rclcpp
{
namespace experimental
{

template<typename ActionT>
class ActionClientIntraProcess : public ActionClientIntraProcessBase
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(ActionClientIntraProcess)

  // Useful aliases for the action client data types
  using ResponseCallback = std::function<void (std::shared_ptr<void>)>;
  using GoalResponse = typename ActionT::Impl::SendGoalService::Response;
  using GoalResponseSharedPtr = typename std::shared_ptr<GoalResponse>;
  using ResultResponse = typename ActionT::Impl::GetResultService::Response;
  using ResultResponseSharedPtr = typename std::shared_ptr<ResultResponse>;
  using FeedbackMessage = typename ActionT::Impl::FeedbackMessage;
  using FeedbackSharedPtr = typename std::shared_ptr<FeedbackMessage>;
  using CancelGoalSharedPtr = typename std::shared_ptr<void>;
  using GoalStatusSharedPtr = typename std::shared_ptr<void>;

  ActionClientIntraProcess(
    rclcpp::Context::SharedPtr context,
    const std::string & action_name,
    const rcl_action_client_depth_t & qos_history,
    ResponseCallback goal_status_callback,
    ResponseCallback feedback_callback)
  : goal_status_callback_(goal_status_callback),
    feedback_callback_(feedback_callback),
    ActionClientIntraProcessBase(
      context,
      action_name,
      QoS(qos_history.goal_service_depth))
  {
    // Create the intra-process buffers
    goal_response_buffer_ =
      rclcpp::experimental::create_service_intra_process_buffer<GoalResponseSharedPtr>(
      QoS(qos_history.goal_service_depth));

    result_response_buffer_ =
      rclcpp::experimental::create_service_intra_process_buffer<ResultResponseSharedPtr>(
      QoS(qos_history.result_service_depth));

    status_buffer_ =
      rclcpp::experimental::create_service_intra_process_buffer<GoalStatusSharedPtr>(
      QoS(qos_history.status_topic_depth));

    feedback_buffer_ =
      rclcpp::experimental::create_service_intra_process_buffer<FeedbackSharedPtr>(
      QoS(qos_history.feedback_topic_depth));

    cancel_response_buffer_ =
      rclcpp::experimental::create_service_intra_process_buffer<CancelGoalSharedPtr>(
      QoS(qos_history.cancel_service_depth));
  }

  virtual ~ActionClientIntraProcess() = default;

  bool is_ready(rcl_wait_set_t * wait_set)
  {
    (void) wait_set;

    is_goal_response_ready_ = goal_response_buffer_->has_data();
    is_result_response_ready_ = result_response_buffer_->has_data();
    is_cancel_response_ready_ = cancel_response_buffer_->has_data();
    is_feedback_ready_ = feedback_buffer_->has_data();
    is_status_ready_ = status_buffer_->has_data();

    return is_feedback_ready_ ||
           is_status_ready_ ||
           is_goal_response_ready_ ||
           is_cancel_response_ready_ ||
           is_result_response_ready_;
  }

  void store_goal_response_callback(ResponseCallback callback)
  {
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);
    goal_response_callback_ = callback;
    if (events_callbacks_set()) {
      set_callback_to_event_type(EventType::GoalResponse, generic_callback_);
    }
  }

  void store_cancel_goal_callback(ResponseCallback callback)
  {
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);
    cancel_goal_callback_ = callback;
    if (events_callbacks_set()) {
      set_callback_to_event_type(EventType::CancelResponse, generic_callback_);
    }
  }

  void store_result_response_callback(ResponseCallback callback)
  {
    std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);
    result_response_callback_ = callback;
    if (events_callbacks_set()) {
      set_callback_to_event_type(EventType::ResultResponse, generic_callback_);
    }
  }

  // Store responses from server
  void store_ipc_action_goal_response(GoalResponseSharedPtr goal_response)
  {
    goal_response_buffer_->add(std::move(goal_response));
    gc_.trigger();
    invoke_on_ready_callback(EventType::GoalResponse);
  }

  void store_ipc_action_result_response(ResultResponseSharedPtr result_response)
  {
    result_response_buffer_->add(std::move(result_response));
    gc_.trigger();
    invoke_on_ready_callback(EventType::ResultResponse);
  }

  void store_ipc_action_cancel_response(CancelGoalSharedPtr cancel_response)
  {
    cancel_response_buffer_->add(std::move(cancel_response));
    gc_.trigger();
    invoke_on_ready_callback(EventType::CancelResponse);
  }

  void store_ipc_action_feedback(FeedbackSharedPtr feedback)
  {
    feedback_buffer_->add(std::move(feedback));
    gc_.trigger();
    invoke_on_ready_callback(EventType::FeedbackReady);
  }

  void store_ipc_action_goal_status(GoalStatusSharedPtr status)
  {
    status_buffer_->add(std::move(status));
    gc_.trigger();
    invoke_on_ready_callback(EventType::StatusReady);
  }

  std::shared_ptr<void>
  take_data() override
  {
    if (is_goal_response_ready_) {
      auto data = std::move(goal_response_buffer_->consume());
      return std::static_pointer_cast<void>(data);
    } else if (is_result_response_ready_) {
      auto data = std::move(result_response_buffer_->consume());
      return std::static_pointer_cast<void>(data);
    } else if (is_cancel_response_ready_) {
      auto data = std::move(cancel_response_buffer_->consume());
      return std::static_pointer_cast<void>(data);
    } else if (is_feedback_ready_) {
      auto data = std::move(feedback_buffer_->consume());
      return std::static_pointer_cast<void>(data);
    } else if (is_status_ready_) {
      auto data = std::move(status_buffer_->consume());
      return std::static_pointer_cast<void>(data);
    } else {
      throw std::runtime_error("Taking data from intra-process action client but nothing is ready");
    }
  }

  std::shared_ptr<void>
  take_data_by_entity_id(size_t id) override
  {
    // Mark as ready the event type from which we want to take data
    switch (static_cast<EventType>(id)) {
      case EventType::ResultResponse:
        is_result_response_ready_ = true;
        break;
      case EventType::CancelResponse:
        is_cancel_response_ready_ = true;
        break;
      case EventType::GoalResponse:
        is_goal_response_ready_ = true;
        break;
      case EventType::FeedbackReady:
        is_feedback_ready_ = true;
        break;
      case EventType::StatusReady:
        is_status_ready_ = true;
        break;
    }

    return take_data();
  }


  void execute(std::shared_ptr<void> & data)
  {
    if (!data) {
      // This can happen when there were more events than elements in the ring buffer
      return;
    }

    if (is_goal_response_ready_) {
      std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);
      is_goal_response_ready_ = false;
      goal_response_callback_(std::move(data));
      // Unset the callback after use, since it's keeping in scope shared pointers
      goal_response_callback_ = nullptr;
      unset_callback_to_event_type(EventType::GoalResponse);
    } else if (is_result_response_ready_) {
      std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);
      is_result_response_ready_ = false;
      result_response_callback_(std::move(data));
      // Unset the callback after use, since it's keeping in scope shared pointers
      result_response_callback_ = nullptr;
      unset_callback_to_event_type(EventType::ResultResponse);
    } else if (is_cancel_response_ready_) {
      std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);
      is_cancel_response_ready_ = false;
      cancel_goal_callback_(std::move(data));
      // Unset the callback after use, since it's keeping in scope shared pointers
      cancel_goal_callback_ = nullptr;
      unset_callback_to_event_type(EventType::CancelResponse);
    } else if (is_feedback_ready_) {
      is_feedback_ready_ = false;
      feedback_callback_(std::move(data));
    } else if (is_status_ready_) {
      is_status_ready_ = false;
      goal_status_callback_(std::move(data));
    } else {
      throw std::runtime_error("Executing intra-process action client but nothing is ready");
    }
  }

protected:
  // Mutex to proctect callbacks
  std::recursive_mutex reentrant_mutex_;

  ResponseCallback goal_status_callback_;
  ResponseCallback feedback_callback_;

  // Create buffers to store data coming from server
  typename rclcpp::experimental::buffers::ServiceIntraProcessBuffer<
    GoalResponseSharedPtr>::UniquePtr goal_response_buffer_;

  typename rclcpp::experimental::buffers::ServiceIntraProcessBuffer<
    ResultResponseSharedPtr>::UniquePtr result_response_buffer_;

  typename rclcpp::experimental::buffers::ServiceIntraProcessBuffer<
    FeedbackSharedPtr>::UniquePtr feedback_buffer_;

  rclcpp::experimental::buffers::ServiceIntraProcessBuffer<
    GoalStatusSharedPtr>::UniquePtr status_buffer_;

  rclcpp::experimental::buffers::ServiceIntraProcessBuffer<
    CancelGoalSharedPtr>::UniquePtr cancel_response_buffer_;

  std::atomic<bool> is_feedback_ready_{false};
  std::atomic<bool> is_status_ready_{false};
  std::atomic<bool> is_goal_response_ready_{false};
  std::atomic<bool> is_cancel_response_ready_{false};
  std::atomic<bool> is_result_response_ready_{false};
};

}  // namespace experimental
}  // namespace rclcpp

#endif  // RCLCPP__EXPERIMENTAL__ACTION_CLIENT_INTRA_PROCESS_HPP_
