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

#include <memory>
#include <stdexcept>

#include "rclcpp/executors/timers_manager.hpp"

using rclcpp::executors::TimersManager;

TimersManager::TimersManager(std::shared_ptr<rclcpp::Context> context)
{
  context_ = context;
}

TimersManager::~TimersManager()
{
  // Remove all timers
  this->clear();

  // Make sure timers thread is stopped before destroying this object
  this->stop();
}

void TimersManager::add_timer(rclcpp::TimerBase::SharedPtr timer)
{
  if (!timer) {
    throw std::invalid_argument("TimersManager::add_timer() trying to add nullptr timer");
  }

  bool added = false;
  {
    std::unique_lock<std::mutex> lock(timers_mutex_);
    added = weak_timers_heap_.add_timer(timer);
    timers_updated_ = timers_updated_ || added;
  }

  if (added) {
    // Notify that a timer has been added
    timers_cv_.notify_one();
  }
}

#define HANDLE_ERROR(en, msg) \
  do {                           \
    errno = en;                  \
    perror(msg);                 \
    exit(EXIT_FAILURE);          \
  } while (0)

static void display_pthread_attr(pthread_attr_t *attr, char *prefix) {
  int s, i;
  size_t v;
  void *stkaddr;
  struct sched_param sp;

  s = pthread_attr_getdetachstate(attr, &i);
  if (s != 0) HANDLE_ERROR(s, "pthread_attr_getdetachstate");
  printf(
      "%sDetach state        = %s\n", prefix,
      (i == PTHREAD_CREATE_DETACHED)
          ? "PTHREAD_CREATE_DETACHED"
          : (i == PTHREAD_CREATE_JOINABLE) ? "PTHREAD_CREATE_JOINABLE" : "???");

  s = pthread_attr_getscope(attr, &i);
  if (s != 0) HANDLE_ERROR(s, "pthread_attr_getscope");
  printf("%sScope               = %s\n", prefix,
         (i == PTHREAD_SCOPE_SYSTEM)
             ? "PTHREAD_SCOPE_SYSTEM"
             : (i == PTHREAD_SCOPE_PROCESS) ? "PTHREAD_SCOPE_PROCESS" : "???");

  s = pthread_attr_getinheritsched(attr, &i);
  if (s != 0) HANDLE_ERROR(s, "pthread_attr_getinheritsched");
  printf(
      "%sInherit scheduler   = %s\n", prefix,
      (i == PTHREAD_INHERIT_SCHED)
          ? "PTHREAD_INHERIT_SCHED"
          : (i == PTHREAD_EXPLICIT_SCHED) ? "PTHREAD_EXPLICIT_SCHED" : "???");

  s = pthread_attr_getschedpolicy(attr, &i);
  if (s != 0) HANDLE_ERROR(s, "pthread_attr_getschedpolicy");
  printf("%sScheduling policy   = %s\n", prefix,
         (i == SCHED_OTHER)
             ? "SCHED_OTHER"
             : (i == SCHED_FIFO) ? "SCHED_FIFO"
                                 : (i == SCHED_RR) ? "SCHED_RR" : "???");

  s = pthread_attr_getschedparam(attr, &sp);
  if (s != 0) HANDLE_ERROR(s, "pthread_attr_getschedparam");
  printf("%sScheduling priority = %d\n", prefix, sp.sched_priority);

  s = pthread_attr_getguardsize(attr, &v);
  if (s != 0) HANDLE_ERROR(s, "pthread_attr_getguardsize");
  printf("%sGuard size          = %zu bytes\n", prefix, v);

  s = pthread_attr_getstack(attr, &stkaddr, &v);
  if (s != 0) HANDLE_ERROR(s, "pthread_attr_getstack");
  printf("%sStack address       = %p\n", prefix, stkaddr);
  printf("%sStack size          = %#zx bytes\n", prefix, v);
}

void TimersManager::start()
{
  // Make sure that the thread is not already running
  if (running_.exchange(true)) {
    throw std::runtime_error("TimersManager::start() can't start timers thread as already running");
  }

  timers_thread_ = std::thread(&TimersManager::run_timers, this);

  int s;
  pthread_attr_t gattr;
  s = pthread_getattr_np(timers_thread_.native_handle(), &gattr);
  if (s != 0) {
    HANDLE_ERROR(s, "pthread_getattr_np");
  }

  printf("TimersManager Thread attributes:\n");
  display_pthread_attr(&gattr, "\t");
}

void TimersManager::stop()
{
  // Nothing to do if the timers thread is not running
  // or if another thread already signaled to stop.
  if (!running_.exchange(false)) {
    return;
  }

  // Notify the timers manager thread to wake up
  {
    std::unique_lock<std::mutex> lock(timers_mutex_);
    timers_updated_ = true;
  }
  timers_cv_.notify_one();

  // Join timers thread if it's running
  if (timers_thread_.joinable()) {
    timers_thread_.join();
  }
}

std::chrono::nanoseconds TimersManager::get_head_timeout()
{
  // Do not allow to interfere with the thread running
  if (running_) {
    throw std::runtime_error(
            "TimersManager::get_head_timeout() can't be used while timers thread is running");
  }

  std::unique_lock<std::mutex> lock(timers_mutex_);
  return this->get_head_timeout_unsafe();
}

size_t TimersManager::get_number_ready_timers()
{
  // Do not allow to interfere with the thread running
  if (running_) {
    throw std::runtime_error(
            "TimersManager::get_number_ready_timers() can't be used while timers thread is running");
  }

  std::unique_lock<std::mutex> lock(timers_mutex_);
  TimersHeap locked_heap = weak_timers_heap_.validate_and_lock();
  return locked_heap.get_number_ready_timers();
}

void TimersManager::execute_ready_timers()
{
  // Do not allow to interfere with the thread running
  if (running_) {
    throw std::runtime_error(
            "TimersManager::execute_ready_timers() can't be used while timers thread is running");
  }

  std::unique_lock<std::mutex> lock(timers_mutex_);
  this->execute_ready_timers_unsafe();
}

bool TimersManager::execute_head_timer()
{
  // Do not allow to interfere with the thread running
  if (running_) {
    throw std::runtime_error(
            "TimersManager::execute_head_timer() can't be used while timers thread is running");
  }

  std::unique_lock<std::mutex> lock(timers_mutex_);

  TimersHeap timers_heap = weak_timers_heap_.validate_and_lock();

  // Nothing to do if we don't have any timer
  if (timers_heap.empty()) {
    return false;
  }

  TimerPtr head_timer = timers_heap.front();

  const bool timer_ready = head_timer->is_ready();
  if (timer_ready) {
    // Invoke the timer callback
    head_timer->execute_callback();
    timers_heap.heapify_root();
    weak_timers_heap_.store(timers_heap);
  }

  return timer_ready;
}

std::chrono::nanoseconds TimersManager::get_head_timeout_unsafe()
{
  // If we don't have any weak pointer, then we just return maximum timeout
  if (weak_timers_heap_.empty()) {
    return std::chrono::nanoseconds::max();;
  }
  // Weak heap is not empty, so try to lock the first element.
  // If it is still a valid pointer, it is guaranteed to be the correct head
  TimerPtr head_timer = weak_timers_heap_.front().lock();

  if (!head_timer) {
    // The first element has expired, we can't make other assumptions on the heap
    // and we need to entirely validate it.
    TimersHeap locked_heap = weak_timers_heap_.validate_and_lock();
    // NOTE: the following operations will not modify any element in the heap, so we
    // don't have to call `weak_timers_heap_.store(locked_heap)` at the end.

    if (locked_heap.empty()) {
      return std::chrono::nanoseconds::max();;
    }
    head_timer = locked_heap.front();
  }

  return head_timer->time_until_trigger();
}

void TimersManager::execute_ready_timers_unsafe()
{
  // We start by locking the timers
  TimersHeap locked_heap = weak_timers_heap_.validate_and_lock();

  // Nothing to do if we don't have any timer
  if (locked_heap.empty()) {
    return;
  }

  // Keep executing timers until they are ready and they were already ready when we started.
  // The two checks prevent this function from blocking indefinitely if the
  // time required for executing the timers is longer than their period.

  TimerPtr head_timer = locked_heap.front();
  const size_t number_ready_timers = locked_heap.get_number_ready_timers();
  size_t executed_timers = 0;
  while (executed_timers < number_ready_timers && head_timer->is_ready()) {
    // Execute head timer
    head_timer->execute_callback();
    executed_timers++;
    // Executing a timer will result in updating its time_until_trigger, so re-heapify
    locked_heap.heapify_root();
    // Get new head timer
    head_timer = locked_heap.front();
  }

  // After having performed work on the locked heap we reflect the changes to weak one.
  // Timers will be already sorted the next time we need them if none went out of scope.
  weak_timers_heap_.store(locked_heap);
}

void TimersManager::run_timers()
{
  while (rclcpp::ok(context_) && running_) {
    // Lock mutex
    std::unique_lock<std::mutex> lock(timers_mutex_);

    std::chrono::nanoseconds time_to_sleep = get_head_timeout_unsafe();

    // No need to wait if a timer is already available
    if (time_to_sleep > std::chrono::nanoseconds::zero()) {
      if (time_to_sleep != std::chrono::nanoseconds::max()) {
        // Wait until timeout or notification that timers have been updated
        timers_cv_.wait_for(lock, time_to_sleep, [this]() {return timers_updated_;});
      } else {
        // Wait until notification that timers have been updated
        timers_cv_.wait(lock, [this]() {return timers_updated_;});  
      }
    }

    // Reset timers updated flag
    timers_updated_ = false;

    // Execute timers
    this->execute_ready_timers_unsafe();
  }

  // Make sure the running flag is set to false when we exit from this function
  // to allow restarting the timers thread.
  running_ = false;
}

void TimersManager::clear()
{
  {
    // Lock mutex and then clear all data structures
    std::unique_lock<std::mutex> lock(timers_mutex_);
    weak_timers_heap_.clear();

    timers_updated_ = true;
  }

  // Notify timers thread such that it can re-compute its timeout
  timers_cv_.notify_one();
}

void TimersManager::remove_timer(TimerPtr timer)
{
  bool removed = false;
  {
    std::unique_lock<std::mutex> lock(timers_mutex_);
    removed = weak_timers_heap_.remove_timer(timer);

    timers_updated_ = timers_updated_ || removed;
  }

  if (removed) {
    // Notify timers thread such that it can re-compute its timeout
    timers_cv_.notify_one();
  }
}
