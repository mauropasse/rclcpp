// Copyright 2016 Open Source Robotics Foundation, Inc.
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

#include "rclcpp/node_interfaces/node_services.hpp"

#include <string>

using rclcpp::node_interfaces::NodeServices;

NodeServices::NodeServices(rclcpp::node_interfaces::NodeBaseInterface * node_base)
: node_base_(node_base)
{}

NodeServices::~NodeServices()
{}

void
NodeServices::add_service(
  rclcpp::ServiceBase::SharedPtr service_base_ptr,
  rclcpp::CallbackGroup::SharedPtr group)
{
  if (group) {
    if (!node_base_->callback_group_in_node(group)) {
      // TODO(jacquelinekay): use custom exception
      throw std::runtime_error("Cannot create service, group not in node.");
    }
    group->add_service(service_base_ptr);
  } else {
    node_base_->get_default_callback_group()->add_service(service_base_ptr);
  }

  auto intra_process_waitable = service_base_ptr->get_intra_process_waitable();
  if (nullptr != intra_process_waitable) {
    // Add to the callback group to be notified about intra-process msgs.
    node_base_->get_default_callback_group()->add_waitable(intra_process_waitable);
  }

  // Notify the executor that a new service was created using the parent Node.
  auto & node_gc = node_base_->get_notify_guard_condition();
  try {
    node_gc.trigger();
  } catch (const rclcpp::exceptions::RCLError & ex) {
    throw std::runtime_error(
            std::string("failed to notify wait set on service creation: ") + ex.what());
  }
}

void
NodeServices::add_client(
  rclcpp::ClientBase::SharedPtr client_base_ptr,
  rclcpp::CallbackGroup::SharedPtr group)
{
  if (group) {
    if (!node_base_->callback_group_in_node(group)) {
      // TODO(jacquelinekay): use custom exception
      throw std::runtime_error("Cannot create client, group not in node.");
    }
    group->add_client(client_base_ptr);
  } else {
    node_base_->get_default_callback_group()->add_client(client_base_ptr);
  }

  auto intra_process_waitable = client_base_ptr->get_intra_process_waitable();
  if (nullptr != intra_process_waitable) {
    // Add to the callback group to be notified about intra-process msgs.
    node_base_->get_default_callback_group()->add_waitable(intra_process_waitable);
  }

  // Notify the executor that a new client was created using the parent Node.
  auto & node_gc = node_base_->get_notify_guard_condition();
  try {
    node_gc.trigger();
  } catch (const rclcpp::exceptions::RCLError & ex) {
    throw std::runtime_error(
            std::string("failed to notify wait set on client creation: ") + ex.what());
  }
}

std::string
NodeServices::resolve_service_name(const std::string & name, bool only_expand) const
{
  return node_base_->resolve_topic_or_service_name(name, true, only_expand);
}
