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

#include "rclcpp/experimental/subscription_intra_process_base.hpp"

using rclcpp::experimental::SubscriptionIntraProcessBase;

bool
SubscriptionIntraProcessBase::add_to_wait_set(rcl_wait_set_t * wait_set)
{
  std::lock_guard<std::recursive_mutex> lock(reentrant_mutex_);

  return gc_->add_to_wait_set(wait_set);
}

const char *
SubscriptionIntraProcessBase::get_topic_name() const
{
  return topic_name_.c_str();
}

rmw_qos_profile_t
SubscriptionIntraProcessBase::get_actual_qos() const
{
  return qos_profile_;
}

void
SubscriptionIntraProcessBase::set_listener_callback(
  rmw_listener_callback_t callback,
  const void * user_data)
{
  std::lock_guard<std::mutex> lock(callback_mutex_);

  user_data_ = user_data;
  callback_ = callback;

  if (callback && unread_count_) {
    // Push events arrived before setting the callback
    callback(user_data, unread_count_);
    unread_count_ = 0;
  }
}
