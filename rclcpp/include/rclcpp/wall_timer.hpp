// Copyright 2020 Open Source Robotics Foundation, Inc.
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

#pragma once

#include <condition_variable>

namespace rclcpp
{
namespace executors
{

/// Timer based on std::chrono. Periodically executes user-specified callbacks.
template<typename FunctorT>
class WallTimer
{
public:
  WallTimer(
    std::chrono::microseconds period,
    FunctorT && callback)
     : period_(period)
  {
    callbacks_.push_back(callback);
  }

  void
  execute_callbacks()
  {
    for (const auto& callback : callbacks_){
      callback();
    }
  }

  void start()
  {
    std::thread([this]()
    {
      while(rclcpp::ok())
      {
        auto wake_up_time = std::chrono::steady_clock::now() + period_;
        execute_callbacks();
        std::this_thread::sleep_until(wake_up_time);
      }
    }).detach();
  }

  void
  add_callback(FunctorT callback)
  {
    callbacks_.push_back(callback);
  }

protected:
  RCLCPP_DISABLE_COPY(WallTimer)

  std::chrono::microseconds period_;
  std::vector<FunctorT> callbacks_;
};

} // namespace executors
} // namespace rclcpp
