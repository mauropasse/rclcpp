// Copyright 2020 Mauro Technology
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

#ifndef RCLCPP__EXECUTORS__DEPENDENCIES_HPP_
#define RCLCPP__EXECUTORS__DEPENDENCIES_HPP_

#include "rmw/event.h"
#include "dds/dds.h"

typedef struct rcl_wait_set_impl_t
{
  // number of subscriptions that have been added to the wait set
  size_t subscription_index;
  rmw_subscriptions_t rmw_subscriptions;
  // number of guard_conditions that have been added to the wait set
  size_t guard_condition_index;
  rmw_guard_conditions_t rmw_guard_conditions;
  // number of clients that have been added to the wait set
  size_t client_index;
  rmw_clients_t rmw_clients;
  // number of services that have been added to the wait set
  size_t service_index;
  rmw_services_t rmw_services;
  // number of events that have been added to the wait set
  size_t event_index;
  rmw_events_t rmw_events;

  rmw_wait_set_t * rmw_wait_set;
  // number of timers that have been added to the wait set
  size_t timer_index;
  // context with which the wait set is associated
  rcl_context_t * context;
  // allocator used in the wait set
  rcl_allocator_t allocator;
} rcl_wait_set_impl_t;

struct CddsEntity
{
  dds_entity_t enth;
};

struct CddsSubscription : CddsEntity
{
  dds_entity_t rdcondh;
};

struct CddsGuardCondition
{
  dds_entity_t gcondh;
};

struct CddsPublisher : CddsEntity
{
  dds_instance_handle_t pubiid;
  struct ddsi_sertopic * sertopic;
};

struct CddsCS
{
  CddsPublisher * pub;
  CddsSubscription * sub;
};

struct CddsService
{
  CddsCS service;
};

struct CddsEvent : CddsEntity
{
  rmw_event_type_t event_type;
};

struct CddsClient
{
  CddsCS client;

#if REPORT_BLOCKED_REQUESTS
  std::mutex lock;
  dds_time_t lastcheck;
  std::map<int64_t, dds_time_t> reqtime;
#endif
};

typedef struct CddsWaitset
{
  dds_entity_t waitseth;

  std::vector<dds_attach_t> trigs;
  size_t nelems;

  std::mutex lock;
  bool inuse;
  std::vector<CddsSubscription *> subs;
  std::vector<CddsGuardCondition *> gcs;
  std::vector<CddsClient *> cls;
  std::vector<CddsService *> srvs;
  std::vector<CddsEvent> evs;
} CddsWaitset;


#endif  // RCLCPP__EXECUTORS__DEPENDENCIES_HPP_