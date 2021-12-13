// Copyright 2015 Open Source Robotics Foundation, Inc.
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

#include "rclcpp/service.hpp"

#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "rclcpp/any_service_callback.hpp"
#include "rclcpp/node_interfaces/node_base_interface.hpp"
#include "rclcpp/macros.hpp"
#include "rmw/error_handling.h"
#include "rmw/rmw.h"

using rclcpp::ServiceBase;

ServiceBase::ServiceBase(std::shared_ptr<rclcpp::node_interfaces::NodeBaseInterface> node_base)
: node_handle_(node_base->get_shared_rcl_node_handle()),
  context_(node_base->get_context())
{}

ServiceBase::~ServiceBase()
{}

bool
ServiceBase::take_type_erased_request(void * request_out, rmw_request_id_t & request_id_out)
{
  rcl_ret_t ret = rcl_take_request(
    this->get_service_handle().get(),
    &request_id_out,
    request_out);
  if (RCL_RET_SERVICE_TAKE_FAILED == ret) {
    return false;
  } else if (RCL_RET_OK != ret) {
    rclcpp::exceptions::throw_from_rcl_error(ret);
  }
  return true;
}

const char *
ServiceBase::get_service_name()
{
  return rcl_service_get_service_name(this->get_service_handle().get());
}

std::shared_ptr<rcl_service_t>
ServiceBase::get_service_handle()
{
  return service_handle_;
}

std::shared_ptr<const rcl_service_t>
ServiceBase::get_service_handle() const
{
  return service_handle_;
}

rcl_node_t *
ServiceBase::get_rcl_node_handle()
{
  return node_handle_.get();
}

const rcl_node_t *
ServiceBase::get_rcl_node_handle() const
{
  return node_handle_.get();
}

bool
ServiceBase::exchange_in_use_by_wait_set_state(bool in_use_state)
{
  return in_use_by_wait_set_.exchange(in_use_state);
}

void
ServiceBase::setup_intra_process(
  uint64_t intra_process_service_id,
  IntraProcessManagerWeakPtr weak_ipm)
{
  intra_process_service_id_ = intra_process_service_id;
  weak_ipm_ = weak_ipm;
  use_intra_process_ = true;
}

rclcpp::Waitable::SharedPtr
ServiceBase::get_intra_process_waitable() const
{
  // If not using intra process, shortcut to nullptr.
  if (!use_intra_process_) {
    return nullptr;
  }
  // Get the intra process manager.
  auto ipm = weak_ipm_.lock();
  if (!ipm) {
    throw std::runtime_error(
            "ClientBase::get_intra_process_waitable() called "
            "after destruction of intra process manager");
  }

  // Use the id to retrieve the subscription intra-process from the intra-process manager.
  return ipm->get_service_intra_process(intra_process_service_id_);
}
