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

#ifndef RCLCPP__EXPERIMENTAL__INTRA_PROCESS_MANAGER_HPP_
#define RCLCPP__EXPERIMENTAL__INTRA_PROCESS_MANAGER_HPP_

#include <rmw/types.h>

#include <shared_mutex>

#include <iterator>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>
#include <typeinfo>

#include "rclcpp/allocator/allocator_deleter.hpp"
#include "rclcpp/experimental/action_client_intra_process.hpp"
#include "rclcpp/experimental/action_client_intra_process_base.hpp"
#include "rclcpp/experimental/action_server_intra_process.hpp"
#include "rclcpp/experimental/action_server_intra_process_base.hpp"
#include "rclcpp/experimental/client_intra_process.hpp"
#include "rclcpp/experimental/client_intra_process_base.hpp"
#include "rclcpp/experimental/buffers/intra_process_buffer.hpp"
#include "rclcpp/experimental/ros_message_intra_process_buffer.hpp"
#include "rclcpp/experimental/service_intra_process.hpp"
#include "rclcpp/experimental/service_intra_process_base.hpp"
#include "rclcpp/experimental/subscription_intra_process.hpp"
#include "rclcpp/experimental/subscription_intra_process_base.hpp"
#include "rclcpp/experimental/subscription_intra_process_buffer.hpp"
#include "rclcpp/logger.hpp"
#include "rclcpp/logging.hpp"
#include "rclcpp/macros.hpp"
#include "rclcpp/publisher_base.hpp"
#include "rclcpp/type_adapter.hpp"
#include "rclcpp/visibility_control.hpp"

namespace rclcpp
{

namespace experimental
{

/// This class performs intra process communication between nodes.
/**
 * This class is used in the creation of publishers and subscriptions.
 * A singleton instance of this class is owned by a rclcpp::Context and a
 * rclcpp::Node can use an associated Context to get an instance of this class.
 * Nodes which do not have a common Context will not exchange intra process
 * messages because they do not share access to the same instance of this class.
 *
 * When a Node creates a subscription, it can also create a helper class,
 * called SubscriptionIntraProcess, meant to receive intra process messages.
 * It can be registered with this class.
 * It is also allocated an id which is unique among all publishers
 * and subscriptions in this process and that is associated to the subscription.
 *
 * When a Node creates a publisher, as with subscriptions, a helper class can
 * be registered with this class.
 * This is required in order to publish intra-process messages.
 * It is also allocated an id which is unique among all publishers
 * and subscriptions in this process and that is associated to the publisher.
 *
 * When a publisher or a subscription are registered, this class checks to see
 * which other subscriptions or publishers it will communicate with,
 * i.e. they have the same topic and compatible QoS.
 *
 * When the user publishes a message, if intra-process communication is enabled
 * on the publisher, the message is given to this class.
 * Using the publisher id, a list of recipients for the message is selected.
 * For each subscription in the list, this class stores the message, whether
 * sharing ownership or making a copy, in a buffer associated with the
 * subscription helper class.
 *
 * The subscription helper class contains a buffer where published
 * intra-process messages are stored until they are taken from the subscription.
 * Depending on the data type stored in the buffer, the subscription helper
 * class can request either shared or exclusive ownership on the message.
 *
 * Thus, when an intra-process message is published, this class knows how many
 * intra-process subscriptions needs it and how many require ownership.
 * This information allows this class to operate efficiently by performing the
 * fewest number of copies of the message required.
 *
 * This class is neither CopyConstructable nor CopyAssignable.
 */
class IntraProcessManager
{
private:
  RCLCPP_DISABLE_COPY(IntraProcessManager)

public:
  RCLCPP_SMART_PTR_DEFINITIONS(IntraProcessManager)

  RCLCPP_PUBLIC
  IntraProcessManager() = default;

  RCLCPP_PUBLIC
  virtual ~IntraProcessManager() = default;

  /// Register a subscription with the manager, returns subscriptions unique id.
  /**
   * This method stores the subscription intra process object, together with
   * the information of its wrapped subscription (i.e. topic name and QoS).
   *
   * In addition this generates a unique intra process id for the subscription.
   *
   * \param subscription the SubscriptionIntraProcess to register.
   * \return an unsigned 64-bit integer which is the subscription's unique id.
   */
  template<
    typename ROSMessageType,
    typename Alloc = std::allocator<ROSMessageType>
  >
  RCLCPP_PUBLIC
  uint64_t
  add_subscription(rclcpp::experimental::SubscriptionIntraProcessBase::SharedPtr subscription)
  {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);

    uint64_t sub_id = IntraProcessManager::get_next_unique_id();

    subscriptions_[sub_id] = subscription;

    // adds the subscription id to all the matchable publishers
    for (auto & pair : publishers_) {
      auto publisher = pair.second.lock();
      if (!publisher) {
        continue;
      }
      if (can_communicate(publisher, subscription)) {
        uint64_t pub_id = pair.first;
        insert_sub_id_for_pub(sub_id, pub_id, subscription->use_take_shared_method());
        if (publisher->is_durability_transient_local() &&
          subscription->is_durability_transient_local())
        {
          do_transient_local_publish<ROSMessageType, Alloc>(
            pub_id, sub_id,
            subscription->use_take_shared_method());
        }
      }
    }

    return sub_id;
  }

  /// Register an intra-process client with the manager, returns the client unique id.
  /**
   * \param client the ClientIntraProcessBase to register.
   * \return an unsigned 64-bit integer which is the client's unique id.
   */
  RCLCPP_PUBLIC
  uint64_t
  add_intra_process_client(rclcpp::experimental::ClientIntraProcessBase::SharedPtr client);

  /// Register an intra-process service with the manager, returns the service unique id.
  /**
   * \param service the Service IntraProcessBase to register.
   * \return an unsigned 64-bit integer which is the service's unique id.
   */
  RCLCPP_PUBLIC
  uint64_t
  add_intra_process_service(rclcpp::experimental::ServiceIntraProcessBase::SharedPtr service);

  /// Register an intra-process action client with the manager,
  /// returns the action client unique id.
  /**
   * \param client the ActionClientIntraProcessBase to register.
   * \return an unsigned 64-bit integer which is the action client's unique id.
   */
  RCLCPP_PUBLIC
  uint64_t
  add_intra_process_action_client(
    rclcpp::experimental::ActionClientIntraProcessBase::SharedPtr client);

  /// Register an intra-process action service with the manager,
  /// returns the action service unique id.
  /**
   * \param service the ActionServerIntraProcessBase to register.
   * \return an unsigned 64-bit integer which is the action service's unique id.
   */
  RCLCPP_PUBLIC
  uint64_t
  add_intra_process_action_server(
    rclcpp::experimental::ActionServerIntraProcessBase::SharedPtr service);

  /// Unregister a subscription using the subscription's unique id.
  /**
   * This method does not allocate memory.
   *
   * \param intra_process_subscription_id id of the subscription to remove.
   */
  RCLCPP_PUBLIC
  void
  remove_subscription(uint64_t intra_process_subscription_id);

  /// Unregister a client using the client's unique id.
  /**
   * \param intra_process_client_id id of the client to remove.
   */
  RCLCPP_PUBLIC
  void
  remove_client(uint64_t intra_process_client_id);

  /// Unregister a service using the service's unique id.
  /**
   * \param intra_process_service_id id of the service to remove.
   */
  RCLCPP_PUBLIC
  void
  remove_service(uint64_t intra_process_service_id);

  /// Unregister an action client using the action client's unique id.
  /**
   * \param ipc_action_client_id id of the client to remove.
   */
  RCLCPP_PUBLIC
  void
  remove_action_client(uint64_t ipc_action_client_id);

  /// Unregister an action server using the action server's unique id.
  /**
   * \param ipc_action_server_id id of the service to remove.
   */
  RCLCPP_PUBLIC
  void
  remove_action_server(uint64_t ipc_action_server_id);

  /// Register a publisher with the manager, returns the publisher unique id.
  /**
   * This method stores the publisher intra process object, together with
   * the information of its wrapped publisher (i.e. topic name and QoS).
   *
   * In addition this generates a unique intra process id for the publisher.
   *
   * \param publisher publisher to be registered with the manager.
   * \return an unsigned 64-bit integer which is the publisher's unique id.
   */
  RCLCPP_PUBLIC
  uint64_t
  add_publisher(
    rclcpp::PublisherBase::SharedPtr publisher,
    rclcpp::experimental::buffers::IntraProcessBufferBase::SharedPtr buffer =
    rclcpp::experimental::buffers::IntraProcessBufferBase::SharedPtr());

  /// Unregister a publisher using the publisher's unique id.
  /**
   * This method does not allocate memory.
   *
   * \param intra_process_publisher_id id of the publisher to remove.
   */
  RCLCPP_PUBLIC
  void
  remove_publisher(uint64_t intra_process_publisher_id);

  // Store an intra-process action client ID along its current
  // goal UUID, since later when the server process a request
  // it'll use the goal UUID to retrieve the client which asked for
  // the result.
  RCLCPP_PUBLIC
  void
  store_intra_process_action_client_goal_uuid(
    uint64_t ipc_action_client_id,
    size_t uuid);

  // Remove an action client goal UUID entry
  RCLCPP_PUBLIC
  void
  remove_intra_process_action_client_goal_uuid(size_t uuid);

  RCLCPP_PUBLIC
  uint64_t
  get_action_client_id_from_goal_uuid(size_t uuid);

  /// Publishes an intra-process message, passed as a unique pointer.
  /**
   * This is one of the two methods for publishing intra-process.
   *
   * Using the intra-process publisher id, a list of recipients is obtained.
   * This list is split in half, depending whether they require ownership or not.
   *
   * This particular method takes a unique pointer as input.
   * The pointer can be promoted to a shared pointer and passed to all the subscriptions
   * that do not require ownership.
   * In case of subscriptions requiring ownership, the message will be copied for all of
   * them except the last one, when ownership can be transferred.
   *
   * This method can save an additional copy compared to the shared pointer one.
   *
   * This method can throw an exception if the publisher id is not found or
   * if the publisher shared_ptr given to add_publisher has gone out of scope.
   *
   * This method does allocate memory.
   *
   * \param intra_process_publisher_id the id of the publisher of this message.
   * \param message the message that is being stored.
   * \param allocator for allocations when buffering messages.
   */
  template<
    typename MessageT,
    typename ROSMessageType,
    typename Alloc,
    typename Deleter = std::default_delete<MessageT>
  >
  void
  do_intra_process_publish(
    uint64_t intra_process_publisher_id,
    std::unique_ptr<MessageT, Deleter> message,
    typename allocator::AllocRebind<MessageT, Alloc>::allocator_type & allocator)
  {
    using MessageAllocTraits = allocator::AllocRebind<MessageT, Alloc>;
    using MessageAllocatorT = typename MessageAllocTraits::allocator_type;

    std::shared_lock<std::shared_timed_mutex> lock(mutex_);

    auto publisher_it = pub_to_subs_.find(intra_process_publisher_id);
    if (publisher_it == pub_to_subs_.end()) {
      // Publisher is either invalid or no longer exists.
      RCLCPP_WARN(
        rclcpp::get_logger("rclcpp"),
        "Calling do_intra_process_publish for invalid or no longer existing publisher id");
      return;
    }
    const auto & sub_ids = publisher_it->second;

    if (sub_ids.take_ownership_subscriptions.empty()) {
      // None of the buffers require ownership, so we promote the pointer
      std::shared_ptr<MessageT> msg = std::move(message);

      this->template add_shared_msg_to_buffers<MessageT, Alloc, Deleter, ROSMessageType>(
        msg, sub_ids.take_shared_subscriptions);
    } else if (!sub_ids.take_ownership_subscriptions.empty() && // NOLINT
      sub_ids.take_shared_subscriptions.size() <= 1)
    {
      // There is at maximum 1 buffer that does not require ownership.
      // So this case is equivalent to all the buffers requiring ownership

      // Merge the two vector of ids into a unique one
      std::vector<uint64_t> concatenated_vector(sub_ids.take_shared_subscriptions);
      concatenated_vector.insert(
        concatenated_vector.end(),
        sub_ids.take_ownership_subscriptions.begin(),
        sub_ids.take_ownership_subscriptions.end());
      this->template add_owned_msg_to_buffers<MessageT, Alloc, Deleter, ROSMessageType>(
        std::move(message),
        concatenated_vector,
        allocator);
    } else if (!sub_ids.take_ownership_subscriptions.empty() && // NOLINT
      sub_ids.take_shared_subscriptions.size() > 1)
    {
      // Construct a new shared pointer from the message
      // for the buffers that do not require ownership
      auto shared_msg = std::allocate_shared<MessageT, MessageAllocatorT>(allocator, *message);

      this->template add_shared_msg_to_buffers<MessageT, Alloc, Deleter, ROSMessageType>(
        shared_msg, sub_ids.take_shared_subscriptions);
      this->template add_owned_msg_to_buffers<MessageT, Alloc, Deleter, ROSMessageType>(
        std::move(message), sub_ids.take_ownership_subscriptions, allocator);
    }
  }

  /// Send an intra-process client request
  /**
   * Using the intra-process client id, a matching intra-process service is retrieved
   * which will store the request to process it asynchronously.
   *
   * \param intra_process_client_id the id of the client sending the request
   * \param request the client's request plus callbacks if any.
   */
  template<typename ServiceT, typename RequestT>
  void
  send_intra_process_client_request(
    uint64_t intra_process_client_id,
    RequestT request)
  {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);

    auto client_it = clients_to_services_.find(intra_process_client_id);

    if (client_it == clients_to_services_.end()) {
      RCLCPP_WARN(
        rclcpp::get_logger("rclcpp"),
        "Calling send_intra_process_client_request for invalid or no "
        "longer existing client id");
      return;
    }
    uint64_t service_id = client_it->second;

    auto service_it = services_.find(service_id);
    if (service_it == services_.end()) {
      auto cli = get_client_intra_process(intra_process_client_id);
      auto warning_msg =
        "Intra-process service gone out of scope. "
        "Do inter-process requests.\n"
        "Client service name: " + std::string(cli->get_service_name());
      RCLCPP_WARN(rclcpp::get_logger("rclcpp"), warning_msg.c_str());
      return;
    }
    auto service_intra_process_base = service_it->second.lock();
    if (service_intra_process_base) {
      auto service = std::dynamic_pointer_cast<
        rclcpp::experimental::ServiceIntraProcess<ServiceT>>(service_intra_process_base);
      if (service) {
        service->store_intra_process_request(
          intra_process_client_id, std::move(request));
      }
    } else {
      services_.erase(service_it);
    }
  }

  template<
    typename MessageT,
    typename ROSMessageType,
    typename Alloc,
    typename Deleter = std::default_delete<MessageT>
  >
  std::shared_ptr<const MessageT>
  do_intra_process_publish_and_return_shared(
    uint64_t intra_process_publisher_id,
    std::unique_ptr<MessageT, Deleter> message,
    typename allocator::AllocRebind<MessageT, Alloc>::allocator_type & allocator)
  {
    using MessageAllocTraits = allocator::AllocRebind<MessageT, Alloc>;
    using MessageAllocatorT = typename MessageAllocTraits::allocator_type;

    std::shared_lock<std::shared_timed_mutex> lock(mutex_);

    auto publisher_it = pub_to_subs_.find(intra_process_publisher_id);
    if (publisher_it == pub_to_subs_.end()) {
      // Publisher is either invalid or no longer exists.
      RCLCPP_WARN(
        rclcpp::get_logger("rclcpp"),
        "Calling do_intra_process_publish for invalid or no longer existing publisher id");
      return nullptr;
    }
    const auto & sub_ids = publisher_it->second;

    if (sub_ids.take_ownership_subscriptions.empty()) {
      // If there are no owning, just convert to shared.
      std::shared_ptr<MessageT> shared_msg = std::move(message);
      if (!sub_ids.take_shared_subscriptions.empty()) {
        this->template add_shared_msg_to_buffers<MessageT, Alloc, Deleter, ROSMessageType>(
          shared_msg, sub_ids.take_shared_subscriptions);
      }
      return shared_msg;
    } else {
      // Construct a new shared pointer from the message for the buffers that
      // do not require ownership and to return.
      auto shared_msg = std::allocate_shared<MessageT, MessageAllocatorT>(allocator, *message);

      if (!sub_ids.take_shared_subscriptions.empty()) {
        this->template add_shared_msg_to_buffers<MessageT, Alloc, Deleter, ROSMessageType>(
          shared_msg,
          sub_ids.take_shared_subscriptions);
      }
      if (!sub_ids.take_ownership_subscriptions.empty()) {
        this->template add_owned_msg_to_buffers<MessageT, Alloc, Deleter, ROSMessageType>(
          std::move(message),
          sub_ids.take_ownership_subscriptions,
          allocator);
      }
      return shared_msg;
    }
  }

  /// Gets an ActionClientIntraProcess<ActionT> from its ID
  /**
   * Using the intra-process client id, gets the correspoding
   * ActionClientIntraProcess<ActionT> if exists.
   *
   * \param ipc_action_client_id the id of the client
   * \return and ActionClientIntraProcess<ActionT> or nullptr if no clients match the ID
   */
  template<typename ActionT>
  std::shared_ptr<rclcpp::experimental::ActionClientIntraProcess<ActionT>>
  get_intra_process_action_client(uint64_t ipc_action_client_id)
  {
    auto action_client = get_action_client_intra_process(ipc_action_client_id);

    if (action_client) {
      auto ipc_action_client = std::dynamic_pointer_cast<
        rclcpp::experimental::ActionClientIntraProcess<ActionT>>(
        action_client);
      if (ipc_action_client) {
        return ipc_action_client;
      }
    }

    throw std::runtime_error("No action clients match the specified ID.");
  }

  /// Gets an ActionServerIntraProcess<ActionT> matching an intra-process action client ID
  /**
   * Using the intra-process client id, a matching intra-process action service is retrieved
   * if exists.
   *
   * \param ipc_action_client_id the id of the client matching a server
   * \return and ActionServerIntraProcess<ActionT> or nullptr if no servers match the client ID
   */
  template<typename ActionT>
  std::shared_ptr<rclcpp::experimental::ActionServerIntraProcess<ActionT>>
  get_matching_intra_process_action_server(uint64_t ipc_action_client_id)
  {
    auto action_client_it = action_clients_to_servers_.find(ipc_action_client_id);

    if (action_client_it == action_clients_to_servers_.end()) {
      throw std::runtime_error("No action clients match the specified ID.");
    }

    uint64_t action_service_id = action_client_it->second;

    auto service_it = action_servers_.find(action_service_id);
    if (service_it == action_servers_.end()) {
      throw std::runtime_error(
              "There are no servers matching the intra-process action client ID.");
    }
    auto action_server_intra_process_base = service_it->second.lock();
    if (action_server_intra_process_base) {
      auto ipc_action_service = std::dynamic_pointer_cast<
        rclcpp::experimental::ActionServerIntraProcess<ActionT>>(
        action_server_intra_process_base);
      if (ipc_action_service) {
        return ipc_action_service;
      }
    } else {
      action_servers_.erase(service_it);
    }

    throw std::runtime_error("No action servers match the specified ID.");
  }

  /// Send an intra-process action client goal request
  /**
   * Using the intra-process action client id, a matching intra-process action
   * server is retrieved which will store the goal request to process it asynchronously.
   *
   * \param ipc_action_client_id the id of the action client sending the goal request
   * \param goal_request the action client's goal request data.
   * \param callback the callback to be called when the server sends the goal response
   */
  template<typename ActionT, typename RequestT>
  void
  intra_process_action_send_goal_request(
    uint64_t ipc_action_client_id,
    RequestT goal_request,
    std::function<void(std::shared_ptr<void>)> callback)
  {
    // First, lets store the client callback to be called when the
    // server sends the goal response
    auto client = get_intra_process_action_client<ActionT>(ipc_action_client_id);

    if (client) {
      client->store_goal_response_callback(callback);
    }

    // Now lets send the goal request
    auto service = get_matching_intra_process_action_server<ActionT>(ipc_action_client_id);

    if (service) {
      service->store_ipc_action_goal_request(
        ipc_action_client_id, std::move(goal_request));
    }
  }

  /// Send an intra-process action client cancel request
  /**
   * Using the intra-process action client id, a matching intra-process action
   * server is retrieved which will store the cancel request to process it asynchronously.
   *
   * \param ipc_action_client_id the id of the action client sending the cancel request
   * \param cancel_request the action client's cancel request data.
   * \param callback the callback to be called when the server sends the cancel response
   */
  template<typename ActionT, typename CancelT>
  void
  intra_process_action_send_cancel_request(
    uint64_t ipc_action_client_id,
    CancelT cancel_request,
    std::function<void(std::shared_ptr<void>)> callback)
  {
    // First, lets store the client callback to be called when the
    // server sends the cancel response
    auto client = get_intra_process_action_client<ActionT>(ipc_action_client_id);

    if (client) {
      client->store_cancel_goal_callback(callback);
    }

    // Now lets send the cancel request
    auto service = get_matching_intra_process_action_server<ActionT>(ipc_action_client_id);

    if (service) {
      service->store_ipc_action_cancel_request(
        ipc_action_client_id, std::move(cancel_request));
    }
  }

  /// Send an intra-process action client result request
  /**
   * Using the intra-process action client id, a matching intra-process action
   * server is retrieved which will store the result request to process it asynchronously.
   *
   * \param ipc_action_client_id the id of the action client sending the result request
   * \param result_request the action client's result request data.
   */
  template<typename ActionT, typename RequestT>
  void
  intra_process_action_send_result_request(
    uint64_t ipc_action_client_id,
    RequestT result_request,
    std::function<void(std::shared_ptr<void>)> callback)
  {
    // First, lets store the client callback to be called when the
    // server sends the result response
    auto client = get_intra_process_action_client<ActionT>(ipc_action_client_id);

    if (client) {
      client->store_result_response_callback(callback);
    }

    // Now lets send the result request to the server
    auto service = get_matching_intra_process_action_server<ActionT>(ipc_action_client_id);

    if (service) {
      service->store_ipc_action_result_request(
        ipc_action_client_id, std::move(result_request));
    }
  }

  /// Send an intra-process action server goal response
  /**
   * Using the intra-process action client id, an action client is found which
   * will get the response of a goal request.
   *
   * \param ipc_action_client_id the id of the action client receiving the response
   * \param goal_response the action server's goal response data.
   */
  template<typename ActionT, typename ResponseT>
  void
  intra_process_action_send_goal_response(
    uint64_t ipc_action_client_id,
    ResponseT goal_response)
  {
    auto client = get_intra_process_action_client<ActionT>(ipc_action_client_id);

    if (client) {
      client->store_ipc_action_goal_response(std::move(goal_response));
    }
  }

  /// Send an intra-process action server cancel response
  /**
   * Using the intra-process action client id, an action client is found which
   * will get the response of the cancel request.
   *
   * \param ipc_action_client_id the id of the action client receiving the response
   * \param cancel_response the action server's cancel response data.
   */
  template<typename ActionT, typename ResponseT>
  void
  intra_process_action_send_cancel_response(
    uint64_t ipc_action_client_id,
    ResponseT cancel_response)
  {
    auto client = get_intra_process_action_client<ActionT>(ipc_action_client_id);

    if (client) {
      client->store_ipc_action_cancel_response(std::move(cancel_response));
    }
  }

  /// Send an intra-process action server result response
  /**
   * Using the intra-process action client id, an action client is found which
   * will get the response of a result request.
   *
   * \param ipc_action_client_id the id of the action client receiving the response
   * \param result_response the action server's result response data.
   */
  template<typename ActionT, typename ResponseT>
  void
  intra_process_action_send_result_response(
    uint64_t ipc_action_client_id,
    ResponseT result_response)
  {
    auto client = get_intra_process_action_client<ActionT>(ipc_action_client_id);

    if (client) {
      client->store_ipc_action_result_response(std::move(result_response));
    }
  }

  /// Intra-process publish an action goal status
  /**
   * Using the intra-process action client id, an action client is found which
   * will get the goal status.
   *
   * \param ipc_action_client_id the id of the action client receiving the goal status
   * \param status_msg the status of the goal, sent by the server
   */
  template<typename ActionT, typename StatusT>
  void
  intra_process_action_publish_status(
    uint64_t ipc_action_client_id,
    StatusT status_msg)
  {
    auto client = get_intra_process_action_client<ActionT>(ipc_action_client_id);

    if (client) {
      client->store_ipc_action_goal_status(std::move(status_msg));
    }
  }

  /// Intra-process publish an action goal feedback
  /**
   * Using the intra-process action client id, an action client is found which
   * will get the goal feedback.
   *
   * \param ipc_action_client_id the id of the action client receiving the goal status
   * \param feedback the feedback of the goal, sent by the server
   */
  template<typename ActionT, typename FeedbackT>
  void
  intra_process_action_publish_feedback(
    uint64_t ipc_action_client_id,
    FeedbackT feedback)
  {
    auto client = get_intra_process_action_client<ActionT>(ipc_action_client_id);

    if (client) {
      client->store_ipc_action_feedback(std::move(feedback));
    }
  }

  template<
    typename MessageT,
    typename Alloc,
    typename Deleter,
    typename ROSMessageType>
  void
  add_shared_msg_to_buffer(
    std::shared_ptr<const MessageT> message,
    uint64_t subscription_id)
  {
    add_shared_msg_to_buffers<MessageT, Alloc, Deleter, ROSMessageType>(message, {subscription_id});
  }

  template<
    typename MessageT,
    typename Alloc,
    typename Deleter,
    typename ROSMessageType>
  void
  add_owned_msg_to_buffer(
    std::unique_ptr<MessageT, Deleter> message,
    uint64_t subscription_id,
    typename allocator::AllocRebind<MessageT, Alloc>::allocator_type & allocator)
  {
    add_owned_msg_to_buffers<MessageT, Alloc, Deleter, ROSMessageType>(
      std::move(message), {subscription_id}, allocator);
  }  

  /// Return true if the given rmw_gid_t matches any stored Publishers.
  RCLCPP_PUBLIC
  bool
  matches_any_publishers(const rmw_gid_t * id) const;

  /// Return the number of intraprocess subscriptions that are matched with a given publisher id.
  RCLCPP_PUBLIC
  size_t
  get_subscription_count(uint64_t intra_process_publisher_id) const;

  RCLCPP_PUBLIC
  rclcpp::experimental::SubscriptionIntraProcessBase::SharedPtr
  get_subscription_intra_process(uint64_t intra_process_subscription_id);

  RCLCPP_PUBLIC
  rclcpp::experimental::ClientIntraProcessBase::SharedPtr
  get_client_intra_process(uint64_t intra_process_client_id);

  RCLCPP_PUBLIC
  rclcpp::experimental::ServiceIntraProcessBase::SharedPtr
  get_service_intra_process(uint64_t intra_process_service_id);

  RCLCPP_PUBLIC
  bool
  service_is_available(uint64_t intra_process_client_id);

  RCLCPP_PUBLIC
  rclcpp::experimental::ActionClientIntraProcessBase::SharedPtr
  get_action_client_intra_process(uint64_t intra_process_action_client_id);

  RCLCPP_PUBLIC
  rclcpp::experimental::ActionServerIntraProcessBase::SharedPtr
  get_action_server_intra_process(uint64_t intra_process_action_server_id);

  RCLCPP_PUBLIC
  bool
  action_server_is_available(uint64_t ipc_action_client_id);

private:
  struct SplittedSubscriptions
  {
    std::vector<uint64_t> take_shared_subscriptions;
    std::vector<uint64_t> take_ownership_subscriptions;
  };

  using SubscriptionMap =
    std::unordered_map<uint64_t, rclcpp::experimental::SubscriptionIntraProcessBase::WeakPtr>;

  using PublisherMap =
    std::unordered_map<uint64_t, rclcpp::PublisherBase::WeakPtr>;

  using PublisherBufferMap =
    std::unordered_map<uint64_t, rclcpp::experimental::buffers::IntraProcessBufferBase::WeakPtr>;

  using PublisherToSubscriptionIdsMap =
    std::unordered_map<uint64_t, SplittedSubscriptions>;

  using ClientMap =
    std::unordered_map<uint64_t, rclcpp::experimental::ClientIntraProcessBase::WeakPtr>;

  using ServiceMap =
    std::unordered_map<uint64_t, rclcpp::experimental::ServiceIntraProcessBase::WeakPtr>;

  using ClientToServiceIdsMap =
    std::unordered_map<uint64_t, uint64_t>;

  using ActionClientMap =
    std::unordered_map<uint64_t, rclcpp::experimental::ActionClientIntraProcessBase::WeakPtr>;

  using ActionServerMap =
    std::unordered_map<uint64_t, rclcpp::experimental::ActionServerIntraProcessBase::WeakPtr>;

  using ActionClientToServerIdsMap =
    std::unordered_map<uint64_t, uint64_t>;

  RCLCPP_PUBLIC
  static
  uint64_t
  get_next_unique_id();

  RCLCPP_PUBLIC
  void
  insert_sub_id_for_pub(uint64_t sub_id, uint64_t pub_id, bool use_take_shared_method);

  RCLCPP_PUBLIC
  bool
  can_communicate(
    rclcpp::PublisherBase::SharedPtr pub,
    rclcpp::experimental::SubscriptionIntraProcessBase::SharedPtr sub) const;

  RCLCPP_PUBLIC
  bool
  can_communicate(
    rclcpp::experimental::ClientIntraProcessBase::SharedPtr client,
    rclcpp::experimental::ServiceIntraProcessBase::SharedPtr service) const;

  RCLCPP_PUBLIC
  bool
  can_communicate(
    rclcpp::experimental::ActionClientIntraProcessBase::SharedPtr client,
    rclcpp::experimental::ActionServerIntraProcessBase::SharedPtr service) const;

  template<
    typename ROSMessageType,
    typename Alloc = std::allocator<ROSMessageType>
  >
  RCLCPP_PUBLIC
  void do_transient_local_publish(
    const uint64_t pub_id, const uint64_t sub_id,
    const bool use_take_shared_method)
  {
    using ROSMessageTypeAllocatorTraits = allocator::AllocRebind<ROSMessageType, Alloc>;
    using ROSMessageTypeAllocator = typename ROSMessageTypeAllocatorTraits::allocator_type;
    using ROSMessageTypeDeleter = allocator::Deleter<ROSMessageTypeAllocator, ROSMessageType>;

    auto publisher_buffer = publisher_buffers_[pub_id].lock();
    if (!publisher_buffer) {
      throw std::runtime_error("publisher buffer has unexpectedly gone out of scope");
    }
    auto buffer = std::dynamic_pointer_cast<
      rclcpp::experimental::buffers::IntraProcessBuffer<
        ROSMessageType,
        ROSMessageTypeAllocator,
        ROSMessageTypeDeleter
      >
      >(publisher_buffer);
    if (!buffer) {
      throw std::runtime_error(
              "failed to dynamic cast publisher's IntraProcessBufferBase to "
              "IntraProcessBuffer<ROSMessageType,ROSMessageTypeAllocator,"
              "ROSMessageTypeDeleter> which can happen when the publisher and "
              "subscription use different allocator types, which is not supported");
    }
    if (use_take_shared_method) {
      auto data_vec = buffer->get_all_data_shared();
      for (auto shared_data : data_vec) {
        this->template add_shared_msg_to_buffer<
          ROSMessageType, ROSMessageTypeAllocator, ROSMessageTypeDeleter, ROSMessageType>(
          shared_data, sub_id);
      }
    } else {
      auto data_vec = buffer->get_all_data_unique();
      for (auto & owned_data : data_vec) {
        auto allocator = ROSMessageTypeAllocator();
        this->template add_owned_msg_to_buffer<
          ROSMessageType, ROSMessageTypeAllocator, ROSMessageTypeDeleter, ROSMessageType>(
          std::move(owned_data), sub_id, allocator);
      }
    }
  }

  template<
    typename MessageT,
    typename Alloc,
    typename Deleter,
    typename ROSMessageType>
  void
  add_shared_msg_to_buffers(
    std::shared_ptr<const MessageT> message,
    std::vector<uint64_t> subscription_ids)
  {
    using ROSMessageTypeAllocatorTraits = allocator::AllocRebind<ROSMessageType, Alloc>;
    using ROSMessageTypeAllocator = typename ROSMessageTypeAllocatorTraits::allocator_type;
    using ROSMessageTypeDeleter = allocator::Deleter<ROSMessageTypeAllocator, ROSMessageType>;

    using PublishedType = typename rclcpp::TypeAdapter<MessageT>::custom_type;
    using PublishedTypeAllocatorTraits = allocator::AllocRebind<PublishedType, Alloc>;
    using PublishedTypeAllocator = typename PublishedTypeAllocatorTraits::allocator_type;
    using PublishedTypeDeleter = allocator::Deleter<PublishedTypeAllocator, PublishedType>;

    for (auto id : subscription_ids) {
      auto subscription_it = subscriptions_.find(id);
      if (subscription_it == subscriptions_.end()) {
        throw std::runtime_error("subscription has unexpectedly gone out of scope");
      }
      auto subscription_base = subscription_it->second.lock();
      if (subscription_base == nullptr) {
        subscriptions_.erase(id);
        continue;
      }

      auto subscription = std::dynamic_pointer_cast<
        rclcpp::experimental::SubscriptionIntraProcessBuffer<PublishedType,
        PublishedTypeAllocator, PublishedTypeDeleter, ROSMessageType>
        >(subscription_base);
      if (subscription != nullptr) {
        subscription->provide_intra_process_data(message);
        continue;
      }

      auto ros_message_subscription = std::dynamic_pointer_cast<
        rclcpp::experimental::SubscriptionROSMsgIntraProcessBuffer<ROSMessageType,
        ROSMessageTypeAllocator, ROSMessageTypeDeleter>
        >(subscription_base);
      if (nullptr == ros_message_subscription) {
        throw std::runtime_error(
                "failed to dynamic cast SubscriptionIntraProcessBase to "
                "SubscriptionIntraProcessBuffer<MessageT, Alloc, Deleter>, or to "
                "SubscriptionROSMsgIntraProcessBuffer<ROSMessageType,ROSMessageTypeAllocator,"
                "ROSMessageTypeDeleter> which can happen when the publisher and "
                "subscription use different allocator types, which is not supported");
      }

      if constexpr (rclcpp::TypeAdapter<MessageT>::is_specialized::value) {
        ROSMessageType ros_msg;
        rclcpp::TypeAdapter<MessageT>::convert_to_ros_message(*message, ros_msg);
        ros_message_subscription->provide_intra_process_message(
          std::make_shared<ROSMessageType>(ros_msg));
      } else {
        if constexpr (std::is_same<MessageT, ROSMessageType>::value) {
          ros_message_subscription->provide_intra_process_message(message);
        } else {
          if constexpr (std::is_same<typename rclcpp::TypeAdapter<MessageT,
            ROSMessageType>::ros_message_type, ROSMessageType>::value)
          {
            ROSMessageType ros_msg;
            rclcpp::TypeAdapter<MessageT, ROSMessageType>::convert_to_ros_message(
              *message, ros_msg);
            ros_message_subscription->provide_intra_process_message(
              std::make_shared<ROSMessageType>(ros_msg));
          }
        }
      }
    }
  }

  template<
    typename MessageT,
    typename Alloc,
    typename Deleter,
    typename ROSMessageType>
  void
  add_owned_msg_to_buffers(
    std::unique_ptr<MessageT, Deleter> message,
    std::vector<uint64_t> subscription_ids,
    typename allocator::AllocRebind<MessageT, Alloc>::allocator_type & allocator)
  {
    using MessageAllocTraits = allocator::AllocRebind<MessageT, Alloc>;
    using MessageUniquePtr = std::unique_ptr<MessageT, Deleter>;

    using ROSMessageTypeAllocatorTraits = allocator::AllocRebind<ROSMessageType, Alloc>;
    using ROSMessageTypeAllocator = typename ROSMessageTypeAllocatorTraits::allocator_type;
    using ROSMessageTypeDeleter = allocator::Deleter<ROSMessageTypeAllocator, ROSMessageType>;

    using PublishedType = typename rclcpp::TypeAdapter<MessageT>::custom_type;
    using PublishedTypeAllocatorTraits = allocator::AllocRebind<PublishedType, Alloc>;
    using PublishedTypeAllocator = typename PublishedTypeAllocatorTraits::allocator_type;
    using PublishedTypeDeleter = allocator::Deleter<PublishedTypeAllocator, PublishedType>;

    for (auto it = subscription_ids.begin(); it != subscription_ids.end(); it++) {
      auto subscription_it = subscriptions_.find(*it);
      if (subscription_it == subscriptions_.end()) {
        throw std::runtime_error("subscription has unexpectedly gone out of scope");
      }
      auto subscription_base = subscription_it->second.lock();
      if (subscription_base == nullptr) {
        subscriptions_.erase(subscription_it);
        continue;
      }

      auto subscription = std::dynamic_pointer_cast<
        rclcpp::experimental::SubscriptionIntraProcessBuffer<PublishedType,
        PublishedTypeAllocator, PublishedTypeDeleter, ROSMessageType>
        >(subscription_base);
      if (subscription != nullptr) {
        if (std::next(it) == subscription_ids.end()) {
          // If this is the last subscription, give up ownership
          subscription->provide_intra_process_data(std::move(message));
          // Last message delivered, break from for loop
          break;
        } else {
          // Copy the message since we have additional subscriptions to serve
          Deleter deleter = message.get_deleter();
          auto ptr = MessageAllocTraits::allocate(allocator, 1);
          MessageAllocTraits::construct(allocator, ptr, *message);

          subscription->provide_intra_process_data(std::move(MessageUniquePtr(ptr, deleter)));
        }

        continue;
      }

      auto ros_message_subscription = std::dynamic_pointer_cast<
        rclcpp::experimental::SubscriptionROSMsgIntraProcessBuffer<ROSMessageType,
        ROSMessageTypeAllocator, ROSMessageTypeDeleter>
        >(subscription_base);
      if (nullptr == ros_message_subscription) {
        throw std::runtime_error(
                "failed to dynamic cast SubscriptionIntraProcessBase to "
                "SubscriptionIntraProcessBuffer<MessageT, Alloc, Deleter>, or to "
                "SubscriptionROSMsgIntraProcessBuffer<ROSMessageType,ROSMessageTypeAllocator,"
                "ROSMessageTypeDeleter> which can happen when the publisher and "
                "subscription use different allocator types, which is not supported");
      }

      if constexpr (rclcpp::TypeAdapter<MessageT>::is_specialized::value) {
        ROSMessageTypeAllocator ros_message_alloc(allocator);
        auto ptr = ros_message_alloc.allocate(1);
        ros_message_alloc.construct(ptr);
        ROSMessageTypeDeleter deleter;
        allocator::set_allocator_for_deleter(&deleter, &allocator);
        rclcpp::TypeAdapter<MessageT>::convert_to_ros_message(*message, *ptr);
        auto ros_msg = std::unique_ptr<ROSMessageType, ROSMessageTypeDeleter>(ptr, deleter);
        ros_message_subscription->provide_intra_process_message(std::move(ros_msg));
      } else {
        if constexpr (std::is_same<MessageT, ROSMessageType>::value) {
          if (std::next(it) == subscription_ids.end()) {
            // If this is the last subscription, give up ownership
            ros_message_subscription->provide_intra_process_message(std::move(message));
            // Last message delivered, break from for loop
            break;
          } else {
            // Copy the message since we have additional subscriptions to serve
            Deleter deleter = message.get_deleter();
            allocator::set_allocator_for_deleter(&deleter, &allocator);
            auto ptr = MessageAllocTraits::allocate(allocator, 1);
            MessageAllocTraits::construct(allocator, ptr, *message);

            ros_message_subscription->provide_intra_process_message(
              std::move(MessageUniquePtr(ptr, deleter)));
          }
        }
      }
    }
  }

  PublisherToSubscriptionIdsMap pub_to_subs_;
  SubscriptionMap subscriptions_;
  PublisherMap publishers_;
  ClientToServiceIdsMap clients_to_services_;
  ClientMap clients_;
  ServiceMap services_;
  ActionClientMap action_clients_;
  ActionServerMap action_servers_;
  ActionClientToServerIdsMap action_clients_to_servers_;

  std::unordered_map<size_t, uint64_t> clients_uuid_to_id_;
  PublisherBufferMap publisher_buffers_;

  mutable std::shared_timed_mutex mutex_;
};

}  // namespace experimental
}  // namespace rclcpp

#endif  // RCLCPP__EXPERIMENTAL__INTRA_PROCESS_MANAGER_HPP_
