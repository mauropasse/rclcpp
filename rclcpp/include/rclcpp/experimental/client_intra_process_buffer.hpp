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

#ifndef RCLCPP__EXPERIMENTAL__CLIENT_INTRA_PROCESS_BUFFER_HPP_
#define RCLCPP__EXPERIMENTAL__CLIENT_INTRA_PROCESS_BUFFER_HPP_

#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <string>
#include <tuple>
#include <utility>
#include <variant>  // NOLINT, cpplint doesn't think this is a cpp std header

#include "rcutils/logging_macros.h"
#include "rclcpp/experimental/client_intra_process_base.hpp"

namespace rclcpp
{
namespace experimental
{

template<typename ServiceT>
class ClientIntraProcessBuffer : public ClientIntraProcessBase
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(ClientIntraProcessBuffer)

  ClientIntraProcessBuffer(
    rclcpp::Context::SharedPtr context,
    const std::string & service_name,
    const rmw_qos_profile_t & qos_profile)
  : ClientIntraProcessBase(context, service_name, qos_profile)
  {}

  virtual ~ClientIntraProcessBuffer() = default;

  bool
  is_ready(rcl_wait_set_t * wait_set)
  {
    (void) wait_set;
    return !queue_.empty();
  }

  using Request = typename ServiceT::Request;
  using Response = typename ServiceT::Response;

  using SharedRequest = typename ServiceT::Request::SharedPtr;
  using SharedResponse = typename ServiceT::Response::SharedPtr;

  using Promise = std::promise<SharedResponse>;
  using PromiseWithRequest = std::promise<std::pair<SharedRequest, SharedResponse>>;

  using SharedPromise = std::shared_ptr<Promise>;
  using SharedPromiseWithRequest = std::shared_ptr<PromiseWithRequest>;

  using Future = std::future<SharedResponse>;
  using SharedFuture = std::shared_future<SharedResponse>;
  using SharedFutureWithRequest = std::shared_future<std::pair<SharedRequest, SharedResponse>>;

  using CallbackType = std::function<void (SharedFuture)>;
  using CallbackWithRequestType = std::function<void (SharedFutureWithRequest)>;

  using CallbackTypeValueVariant = std::tuple<CallbackType, SharedFuture, Promise>;
  using CallbackWithRequestTypeValueVariant = std::tuple<
    CallbackWithRequestType, SharedRequest, SharedFutureWithRequest, PromiseWithRequest>;

  using CallbackInfoVariant = std::variant<
    std::promise<SharedResponse>,
    CallbackTypeValueVariant,
    CallbackWithRequestTypeValueVariant>;

  struct ClientResponse
  {
    SharedResponse response;
    CallbackInfoVariant value;

    ClientResponse(
      SharedResponse resp,
      CallbackInfoVariant val)
    : response(resp), value(std::move(val)) {}
  };

  std::shared_ptr<void>
  take_data() override
  {
    auto data = std::make_shared<ClientResponse>(std::move(queue_.front()));
    queue_.pop();
    return std::static_pointer_cast<void>(data);
  }

  void execute(std::shared_ptr<void> & data)
  {
    if (!data) {
      throw std::runtime_error("'data' is empty");
    }

    auto data_ptr = std::static_pointer_cast<ClientResponse>(data);
    auto & value = data_ptr->value;
    auto & typed_response = data_ptr->response;

    if (std::holds_alternative<Promise>(value)) {
      auto & promise = std::get<Promise>(value);
      promise.set_value(std::move(typed_response));
    } else if (std::holds_alternative<CallbackTypeValueVariant>(value)) {
      auto & inner = std::get<CallbackTypeValueVariant>(value);
      const auto & callback = std::get<CallbackType>(inner);
      auto & promise = std::get<Promise>(inner);
      auto & future = std::get<SharedFuture>(inner);
      promise.set_value(std::move(typed_response));
      callback(std::move(future));
    } else if (std::holds_alternative<CallbackWithRequestTypeValueVariant>(value)) {
      auto & inner = std::get<CallbackWithRequestTypeValueVariant>(value);
      const auto & callback = std::get<CallbackWithRequestType>(inner);
      auto & promise = std::get<PromiseWithRequest>(inner);
      auto & future = std::get<SharedFutureWithRequest>(inner);
      auto & request = std::get<SharedRequest>(inner);
      promise.set_value(std::make_pair(std::move(request), std::move(typed_response)));
      callback(std::move(future));
    }
  }

  void
  store_intra_process_response(
    SharedResponse response,
    CallbackInfoVariant value)
  {
    // Mauro: There is a copy here, why not using RequestSharedPtr?
    // I have to modify client.hpp, but can be done.
    queue_.emplace(std::move(response), std::move(value));
    trigger_guard_condition();
  }

protected:
  void
  trigger_guard_condition()
  {
    gc_.trigger();
  }

  std::queue<ClientResponse> queue_;
};

}  // namespace experimental
}  // namespace rclcpp

#endif  // RCLCPP__EXPERIMENTAL__CLIENT_INTRA_PROCESS_BUFFER_HPP_
