// Copyright 2019 Nobleo Technology
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

#include "rclcpp/executable_list.hpp"

using rclcpp::executor::ExecutableList;

ExecutableList::ExecutableList()
: number_of_subscriptions(0),
  number_of_timers(0),
  number_of_services(0),
  number_of_clients(0),
  number_of_waitables(0)
{}

ExecutableList::~ExecutableList()
{}

void
ExecutableList::clear()
{
  this->timer.clear();
  this->number_of_timers = 0;

  this->subscription.clear();
  this->number_of_subscriptions = 0;

  this->service.clear();
  this->number_of_services = 0;

  this->client.clear();
  this->number_of_clients = 0;

  this->waitable.clear();
  this->number_of_waitables = 0;

  elem = 0;
}

void
ExecutableList::add_subscription(rclcpp::SubscriptionBase::SharedPtr subscription)
{
  std::cout << "add_subscription. elem: " << elem++ << std::endl;

  this->subscription.push_back(std::move(subscription));
  this->number_of_subscriptions++;
}

void
ExecutableList::add_timer(rclcpp::TimerBase::SharedPtr timer)
{
  std::cout << "add_timer" << std::endl;
  this->timer.push_back(std::move(timer));
  this->number_of_timers++;
}

void
ExecutableList::add_service(rclcpp::ServiceBase::SharedPtr service)
{
  std::cout << "add_service. elem: " << elem++ << std::endl;
  this->service.push_back(std::move(service));
  this->number_of_services++;
}

void
ExecutableList::add_client(rclcpp::ClientBase::SharedPtr client)
{
  std::cout << "add_client. elem: " << elem++ << std::endl;
  this->client.push_back(std::move(client));
  this->number_of_clients++;
}

void
ExecutableList::add_waitable(rclcpp::Waitable::SharedPtr waitable)
{
  std::cout << "add_waitable. GCs: " << waitable->get_number_of_ready_guard_conditions() << std::endl;
  for (size_t i = 0; i < waitable->get_number_of_ready_guard_conditions(); i++){
      std::cout << "    gc " << i << ". elem: " << elem++ << std::endl;
  }

  this->waitable.push_back(std::move(waitable));
  this->number_of_waitables++;
}
