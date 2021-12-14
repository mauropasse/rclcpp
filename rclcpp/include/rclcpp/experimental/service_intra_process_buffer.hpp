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

#ifndef RCLCPP__EXPERIMENTAL__SERVICE_INTRA_PROCESS_BUFFER_HPP_
#define RCLCPP__EXPERIMENTAL__SERVICE_INTRA_PROCESS_BUFFER_HPP_

#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <string>
#include <tuple>
#include <utility>

#include "rcl/error_handling.h"
#include "rcutils/logging_macros.h"

#include "rclcpp/any_service_callback.hpp"
#include "rclcpp/experimental/client_intra_process_buffer.hpp"
#include "rclcpp/experimental/service_intra_process_base.hpp"
#include "rclcpp/logging.hpp"
#include "rclcpp/qos.hpp"
#include "rclcpp/type_support_decl.hpp"

namespace rclcpp
{
namespace experimental
{

template<typename ServiceT>
class ServiceIntraProcessBuffer : public ServiceIntraProcessBase
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(ServiceIntraProcessBuffer)

  ServiceIntraProcessBuffer(
    AnyServiceCallback<ServiceT> callback,
    rclcpp::Context::SharedPtr context,
    const std::string & service_name,
    const rmw_qos_profile_t & qos_profile)
  : ServiceIntraProcessBase(context, service_name, qos_profile), any_callback_(callback)
  {}

  virtual ~ServiceIntraProcessBuffer() = default;

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

  void
  store_intra_process_request(
    uint64_t intra_process_client_id,
    SharedRequest request,
    CallbackInfoVariant value)
  {
    queue_.emplace(
      intra_process_client_id,
      std::make_pair(std::move(request), std::move(value)));
    trigger_guard_condition();
  }

  std::shared_ptr<void>
  take_data()
  {
    // Mauro: Check if this move is correct
    auto data = std::make_shared<ClientIDtoRequestMap>(std::move(queue_.front()));
    queue_.pop();
    return std::static_pointer_cast<void>(data);
  }

  void execute(std::shared_ptr<void> & data)
  {
    auto ptr = std::static_pointer_cast<ClientIDtoRequestMap>(data);

    SharedRequest & typed_request = ptr->second.first;
    CallbackInfoVariant & value = ptr->second.second;

    SharedResponse response = any_callback_.dispatch(nullptr, nullptr, std::move(typed_request));

    uint64_t intra_process_client_id = ptr->first;

    if (response) {
      std::unique_lock<std::recursive_mutex> lock(reentrant_mutex_);

      auto client_it = clients_.find(intra_process_client_id);

      if (client_it == clients_.end()) {
        // Publisher is either invalid or no longer exists.
        RCLCPP_WARN(
          rclcpp::get_logger("rclcpp"),
          "Calling intra_process_service_send_response for invalid or no "
          "longer existing client id");
        return;
      }

      auto client_intra_process_base = client_it->second.lock();

      if (client_intra_process_base) {
        auto client = std::dynamic_pointer_cast<
          rclcpp::experimental::ClientIntraProcessBuffer<ServiceT>>(
          client_intra_process_base);
        client->store_intra_process_response(response, std::move(value));
      } else {
        clients_.erase(client_it);
      }
    }
  }

protected:
  void
  trigger_guard_condition()
  {
    gc_.trigger();
  }

  typedef std::pair<SharedRequest, CallbackInfoVariant> RequestCallbackMap;
  typedef std::pair<uint64_t, RequestCallbackMap> ClientIDtoRequestMap;

  std::queue<ClientIDtoRequestMap> queue_;

  AnyServiceCallback<ServiceT> any_callback_;
};

}  // namespace experimental
}  // namespace rclcpp

#endif  // RCLCPP__EXPERIMENTAL__SERVICE_INTRA_PROCESS_BUFFER_HPP_
