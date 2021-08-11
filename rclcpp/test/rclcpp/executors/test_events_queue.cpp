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
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <queue>

#include "rclcpp/experimental/buffers/simple_events_queue.hpp"

using namespace std::chrono_literals;

TEST(TestEventsQueue, SimpleQueueTest)
{
  // Create a SimpleEventsQueue and a local queue
  auto simple_queue = std::make_unique<rclcpp::experimental::buffers::SimpleEventsQueue>();
  std::queue<rclcpp::executors::ExecutorEvent> local_events_queue;

  // Make sure the queue is empty after init
  simple_queue->init();
  EXPECT_TRUE(simple_queue->empty());

  // Push 11 messages
  for (int i = 0; i < 11; i++) {
    rclcpp::executors::ExecutorEvent stub_event;
    simple_queue->push(stub_event);
  }

  // Pop one message
  simple_queue->pop();

  local_events_queue = simple_queue->pop_all_events();

  // We should have (11 - 1) events in the local queue
  size_t local_queue_size = local_events_queue.size();

  // The local queue size should be 10
  EXPECT_EQ(10u, local_queue_size);

  // The simple queue should be empty after taking all events
  EXPECT_TRUE(simple_queue->empty());

  // Lets push an event into the queue and get it back
  rclcpp::executors::ExecutorEvent push_event = {simple_queue.get(), 0,
    rclcpp::executors::ExecutorEventType::SUBSCRIPTION_EVENT, 1};

  simple_queue->push(push_event);

  rclcpp::executors::ExecutorEvent front_event = simple_queue->front();

  // The events should be equal
  EXPECT_EQ(push_event.exec_entity_id, front_event.exec_entity_id);
  EXPECT_EQ(push_event.type, front_event.type);
}
