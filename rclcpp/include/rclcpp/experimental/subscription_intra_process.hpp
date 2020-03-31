// Copyright 2019 Open Source Robotics Foundation, Inc.
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

#ifndef RCLCPP__EXPERIMENTAL__SUBSCRIPTION_INTRA_PROCESS_HPP_
#define RCLCPP__EXPERIMENTAL__SUBSCRIPTION_INTRA_PROCESS_HPP_

#include <rmw/rmw.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "rcl/error_handling.h"

#include "rclcpp/any_subscription_callback.hpp"
#include "rclcpp/experimental/buffers/intra_process_buffer.hpp"
#include "rclcpp/experimental/create_intra_process_buffer.hpp"
#include "rclcpp/experimental/subscription_intra_process_base.hpp"
#include "rclcpp/type_support_decl.hpp"
#include "rclcpp/waitable.hpp"
#include "tracetools/tracetools.h"

namespace rclcpp
{
namespace experimental
{

template<
  typename MessageT,
  typename Alloc = std::allocator<void>,
  typename Deleter = std::default_delete<MessageT>,
  typename CallbackMessageT = MessageT>
class SubscriptionIntraProcess : public SubscriptionIntraProcessBase
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(SubscriptionIntraProcess)

  using MessageAllocTraits = allocator::AllocRebind<MessageT, Alloc>;
  using MessageAlloc = typename MessageAllocTraits::allocator_type;
  using ConstMessageSharedPtr = std::shared_ptr<const MessageT>;
  using MessageUniquePtr = std::unique_ptr<MessageT, Deleter>;

  using BufferUniquePtr = typename rclcpp::experimental::buffers::IntraProcessBuffer<
    MessageT,
    Alloc,
    Deleter
    >::UniquePtr;

  SubscriptionIntraProcess(
    AnySubscriptionCallback<CallbackMessageT, Alloc> callback,
    std::shared_ptr<Alloc> allocator,
    rclcpp::Context::SharedPtr context,
    const std::string & topic_name,
    rmw_qos_profile_t qos_profile,
    rclcpp::IntraProcessBufferType buffer_type)
  : SubscriptionIntraProcessBase(topic_name, qos_profile),
    any_callback_(callback)
  {
    if (!std::is_same<MessageT, CallbackMessageT>::value) {
      throw std::runtime_error("SubscriptionIntraProcess wrong callback type");
    }

    (void)context;

    // Create the intra-process buffer.
    buffer_ = rclcpp::experimental::create_intra_process_buffer<MessageT, Alloc, Deleter>(
      buffer_type,
      qos_profile,
      allocator);

    TRACEPOINT(
      rclcpp_subscription_callback_added,
      (const void *)this,
      (const void *)&any_callback_);
    // The callback object gets copied, so if registration is done too early/before this point
    // (e.g. in `AnySubscriptionCallback::set()`), its address won't match any address used later
    // in subsequent tracepoints.
#ifndef TRACETOOLS_DISABLED
    any_callback_.register_callback_for_tracing();
#endif
  }

  void execute()
  {
    execute_impl<CallbackMessageT>();
  }

  void
  provide_intra_process_message(ConstMessageSharedPtr message)
  {
    buffer_->add_shared(std::move(message));
    trigger_condition_variable();
  }

  void
  provide_intra_process_message(MessageUniquePtr message)
  {
    buffer_->add_unique(std::move(message));
    trigger_condition_variable();
  }

  bool
  use_take_shared_method() const
  {
    return buffer_->use_take_shared_method();
  }

  void consume_messages_task() override
  {
    while(rclcpp::ok()){
      std::unique_lock<std::mutex> lock(m_);

      // Check condition variable, if triggered check buffer for data
      cv_.wait(lock, [this]{return buffer_->has_data();});
      lock.unlock();

      // Process the message
      execute();
    }
  }

private:
  void
  trigger_condition_variable()
  {
    // Publisher pushed message into the buffer.
    // Notify subscription thread
    cv_.notify_one();
  }

  template<typename T>
  typename std::enable_if<std::is_same<T, rcl_serialized_message_t>::value, void>::type
  execute_impl()
  {
    throw std::runtime_error("Subscription intra-process can't handle serialized messages");
  }

  template<class T>
  typename std::enable_if<!std::is_same<T, rcl_serialized_message_t>::value, void>::type
  execute_impl()
  {
    rmw_message_info_t msg_info;
    msg_info.publisher_gid = {0, {0}};
    msg_info.from_intra_process = true;

    if (any_callback_.use_take_shared_method()) {
      ConstMessageSharedPtr msg = buffer_->consume_shared();
      any_callback_.dispatch_intra_process(msg, msg_info);
    } else {
      MessageUniquePtr msg = buffer_->consume_unique();
      any_callback_.dispatch_intra_process(std::move(msg), msg_info);
    }
  }

  AnySubscriptionCallback<CallbackMessageT, Alloc> any_callback_;
  BufferUniquePtr buffer_;
};

}  // namespace experimental
}  // namespace rclcpp

#endif  // RCLCPP__EXPERIMENTAL__SUBSCRIPTION_INTRA_PROCESS_HPP_
