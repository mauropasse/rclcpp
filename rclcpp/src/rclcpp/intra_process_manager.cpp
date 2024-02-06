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

#include "rclcpp/experimental/intra_process_manager.hpp"

#include <atomic>
#include <memory>
#include <mutex>

namespace rclcpp
{
namespace experimental
{

static std::atomic<uint64_t> _next_unique_id {1};

uint64_t
IntraProcessManager::add_publisher(
  rclcpp::PublisherBase::SharedPtr publisher,
  rclcpp::experimental::buffers::IntraProcessBufferBase::SharedPtr buffer)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);

  uint64_t pub_id = IntraProcessManager::get_next_unique_id();

  publishers_[pub_id] = publisher;
  if (publisher->is_durability_transient_local()) {
    if (buffer) {
      publisher_buffers_[pub_id] = buffer;
    } else {
      throw std::runtime_error(
              "transient_local publisher needs to pass"
              "a valid publisher buffer ptr when calling add_publisher()");
    }
  }

  // Initialize the subscriptions storage for this publisher.
  pub_to_subs_[pub_id] = SplittedSubscriptions();

  // create an entry for the publisher id and populate with already existing subscriptions
  for (auto & pair : subscriptions_) {
    auto subscription = pair.second.lock();
    if (!subscription) {
      continue;
    }
    if (can_communicate(publisher, subscription)) {
      uint64_t sub_id = pair.first;
      insert_sub_id_for_pub(sub_id, pub_id, subscription->use_take_shared_method());
    }
  }

  return pub_id;
}

uint64_t
IntraProcessManager::add_intra_process_client(ClientIntraProcessBase::SharedPtr client)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);

  uint64_t client_id = IntraProcessManager::get_next_unique_id();
  clients_[client_id] = client;

  // adds the client to the matchable service
  for (auto & pair : services_) {
    auto intra_process_service = pair.second.lock();
    if (!intra_process_service) {
      continue;
    }
    if (can_communicate(client, intra_process_service)) {
      uint64_t service_id = pair.first;
      clients_to_services_.emplace(client_id, service_id);
      intra_process_service->add_intra_process_client(client, client_id);
      break;
    }
  }

  return client_id;
}

uint64_t
IntraProcessManager::add_intra_process_service(ServiceIntraProcessBase::SharedPtr service)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);

  // First check if we have already a service registered with same service name
  // Todo: Open issue about this not being enforced with normal services
  auto it = services_.begin();

  while (it != services_.end()) {
    auto srv = it->second.lock();
    if (srv) {
      if (srv->get_service_name() == service->get_service_name()) {
        throw std::runtime_error(
                "Can't have multiple services with same service name.");
      }
      it++;
    } else {
      it = services_.erase(it);
    }
  }

  uint64_t service_id = IntraProcessManager::get_next_unique_id();
  services_[service_id] = service;

  // adds the service to the matchable clients
  for (auto & pair : clients_) {
    auto client = pair.second.lock();
    if (!client) {
      continue;
    }
    if (can_communicate(client, service)) {
      uint64_t client_id = pair.first;
      clients_to_services_.emplace(client_id, service_id);

      service->add_intra_process_client(client, client_id);
    }
  }

  return service_id;
}

uint64_t
IntraProcessManager::add_intra_process_action_client(
  ActionClientIntraProcessBase::SharedPtr client)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);

  uint64_t client_id = IntraProcessManager::get_next_unique_id();
  action_clients_[client_id] = client;

  // adds the intra-process action client to the matchable action server
  for (auto & pair : action_servers_) {
    auto server = pair.second.lock();
    if (!server) {
      continue;
    }
    if (can_communicate(client, server)) {
      uint64_t server_id = pair.first;
      action_clients_to_servers_.emplace(client_id, server_id);
      break;
    }
  }
  return client_id;
}

uint64_t
IntraProcessManager::add_intra_process_action_server(
  ActionServerIntraProcessBase::SharedPtr server)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);

  // First check if we have already a server registered with same server name
  auto it = action_servers_.begin();

  while (it != action_servers_.end()) {
    auto ipc_action_server = it->second.lock();
    if (ipc_action_server) {
      if (ipc_action_server->get_action_name() == server->get_action_name()) {
        throw std::runtime_error(
                "Can't have multiple action servers with same server name.");
      }
      it++;
    } else {
      it = action_servers_.erase(it);
    }
  }

  uint64_t server_id = IntraProcessManager::get_next_unique_id();
  action_servers_[server_id] = server;

  // adds the server to the matchable action_clients
  for (auto & pair : action_clients_) {
    auto ipc_action_client = pair.second.lock();
    if (!ipc_action_client) {
      continue;
    }
    if (can_communicate(ipc_action_client, server)) {
      uint64_t client_id = pair.first;
      action_clients_to_servers_.emplace(client_id, server_id);
    }
  }
  return server_id;
}

// Store an intra-process action client ID along its current
// goal UUID, since later when the server process a request
// it'll use the goal UUID to retrieve the client which asked for
// the result.
void
IntraProcessManager::store_intra_process_action_client_goal_uuid(
  uint64_t ipc_action_client_id,
  size_t uuid)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);
  clients_uuid_to_id_[uuid] = ipc_action_client_id;
}

// Remove an action client goal UUID entry
void
IntraProcessManager::remove_intra_process_action_client_goal_uuid(size_t uuid)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);

  auto iter = clients_uuid_to_id_.find(uuid);

  if (iter == clients_uuid_to_id_.end()) {
    throw std::runtime_error(
            "No ipc action clients stored with this UUID.");
  }

  clients_uuid_to_id_.erase(iter);
}

uint64_t
IntraProcessManager::get_action_client_id_from_goal_uuid(size_t uuid)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);

  auto iter = clients_uuid_to_id_.find(uuid);

  if (iter == clients_uuid_to_id_.end()) {
    RCLCPP_WARN(
      rclcpp::get_logger("rclcpp"),
      "No action clients match the specified goal UUID: %ld", uuid);

    return 0;
  }

  return iter->second;
}

void
IntraProcessManager::remove_subscription(uint64_t intra_process_subscription_id)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);

  subscriptions_.erase(intra_process_subscription_id);

  for (auto & pair : pub_to_subs_) {
    pair.second.take_shared_subscriptions.erase(
      std::remove(
        pair.second.take_shared_subscriptions.begin(),
        pair.second.take_shared_subscriptions.end(),
        intra_process_subscription_id),
      pair.second.take_shared_subscriptions.end());

    pair.second.take_ownership_subscriptions.erase(
      std::remove(
        pair.second.take_ownership_subscriptions.begin(),
        pair.second.take_ownership_subscriptions.end(),
        intra_process_subscription_id),
      pair.second.take_ownership_subscriptions.end());
  }
}

void
IntraProcessManager::remove_publisher(uint64_t intra_process_publisher_id)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);

  publishers_.erase(intra_process_publisher_id);
  publisher_buffers_.erase(intra_process_publisher_id);
  pub_to_subs_.erase(intra_process_publisher_id);
}

void
IntraProcessManager::remove_client(uint64_t intra_process_client_id)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);

  clients_.erase(intra_process_client_id);
  clients_to_services_.erase(intra_process_client_id);
}

void
IntraProcessManager::remove_service(uint64_t intra_process_service_id)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);

  services_.erase(intra_process_service_id);

  auto it = clients_to_services_.begin();

  while (it != clients_to_services_.end()) {
    if (it->second == intra_process_service_id) {
      it = clients_to_services_.erase(it);
    } else {
      it++;
    }
  }
}

void
IntraProcessManager::remove_action_client(uint64_t ipc_action_client_id)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);

  action_clients_.erase(ipc_action_client_id);
  action_clients_to_servers_.erase(ipc_action_client_id);

  auto it = clients_uuid_to_id_.begin();

  while (it != clients_uuid_to_id_.end()) {
    if (it->second == ipc_action_client_id) {
      it = clients_uuid_to_id_.erase(it);
    } else {
      it++;
    }
  }
}

void
IntraProcessManager::remove_action_server(uint64_t ipc_action_server_id)
{
  std::unique_lock<std::shared_timed_mutex> lock(mutex_);

  action_servers_.erase(ipc_action_server_id);

  auto it = action_clients_to_servers_.begin();

  while (it != action_clients_to_servers_.end()) {
    if (it->second == ipc_action_server_id) {
      it = action_clients_to_servers_.erase(it);
    } else {
      it++;
    }
  }
}

bool
IntraProcessManager::matches_any_publishers(const rmw_gid_t * id) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);

  for (auto & publisher_pair : publishers_) {
    auto publisher = publisher_pair.second.lock();
    if (!publisher) {
      continue;
    }
    if (*publisher.get() == id) {
      return true;
    }
  }
  return false;
}

size_t
IntraProcessManager::get_subscription_count(uint64_t intra_process_publisher_id) const
{
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);

  auto publisher_it = pub_to_subs_.find(intra_process_publisher_id);
  if (publisher_it == pub_to_subs_.end()) {
    // Publisher is either invalid or no longer exists.
    RCLCPP_WARN(
      rclcpp::get_logger("rclcpp"),
      "Calling get_subscription_count for invalid or no longer existing publisher id");
    return 0;
  }

  auto count =
    publisher_it->second.take_shared_subscriptions.size() +
    publisher_it->second.take_ownership_subscriptions.size();

  return count;
}

SubscriptionIntraProcessBase::SharedPtr
IntraProcessManager::get_subscription_intra_process(uint64_t intra_process_subscription_id)
{
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);

  auto subscription_it = subscriptions_.find(intra_process_subscription_id);
  if (subscription_it == subscriptions_.end()) {
    return nullptr;
  } else {
    auto subscription = subscription_it->second.lock();
    if (subscription) {
      return subscription;
    } else {
      subscriptions_.erase(subscription_it);
      return nullptr;
    }
  }
}

ServiceIntraProcessBase::SharedPtr
IntraProcessManager::get_service_intra_process(uint64_t intra_process_service_id)
{
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);

  auto service_it = services_.find(intra_process_service_id);
  if (service_it == services_.end()) {
    return nullptr;
  } else {
    auto service = service_it->second.lock();
    if (service) {
      return service;
    } else {
      services_.erase(service_it);
      return nullptr;
    }
  }
}

ClientIntraProcessBase::SharedPtr
IntraProcessManager::get_client_intra_process(uint64_t intra_process_client_id)
{
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);

  auto client_it = clients_.find(intra_process_client_id);
  if (client_it == clients_.end()) {
    return nullptr;
  } else {
    auto client = client_it->second.lock();
    if (client) {
      return client;
    } else {
      clients_.erase(client_it);
      return nullptr;
    }
  }
}

ActionClientIntraProcessBase::SharedPtr
IntraProcessManager::get_action_client_intra_process(
  uint64_t intra_process_action_client_id)
{
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);

  auto client_it = action_clients_.find(intra_process_action_client_id);
  if (client_it == action_clients_.end()) {
    RCLCPP_WARN(
      rclcpp::get_logger("rclcpp"),
      "No action clients match the specified ID: %ld", intra_process_action_client_id);
    return nullptr;
  } else {
    auto client = client_it->second.lock();
    if (client) {
      return client;
    } else {
      action_clients_.erase(client_it);
      RCLCPP_WARN(
        rclcpp::get_logger("rclcpp"),
        "Action client out of scope. ID: %ld", intra_process_action_client_id);
      return nullptr;
    }
  }
}

ActionServerIntraProcessBase::SharedPtr
IntraProcessManager::get_action_server_intra_process(
  uint64_t intra_process_action_server_id)
{
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);

  auto service_it = action_servers_.find(intra_process_action_server_id);
  if (service_it == action_servers_.end()) {
    return nullptr;
  } else {
    auto service = service_it->second.lock();
    if (service) {
      return service;
    } else {
      action_servers_.erase(service_it);
      return nullptr;
    }
  }
}

bool
IntraProcessManager::service_is_available(uint64_t intra_process_client_id)
{
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);

  auto service_it = clients_to_services_.find(intra_process_client_id);

  if (service_it != clients_to_services_.end()) {
    // A server matching the client has been found
    return true;
  }
  return false;
}

bool
IntraProcessManager::action_server_is_available(uint64_t ipc_action_client_id)
{
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  auto action_service_it = action_clients_to_servers_.find(ipc_action_client_id);

  if (action_service_it != action_clients_to_servers_.end()) {
    // An action server matching the action client has been found
    return true;
  }
  return false;
}

uint64_t
IntraProcessManager::get_next_unique_id()
{
  auto next_id = _next_unique_id.fetch_add(1, std::memory_order_relaxed);
  // Check for rollover (we started at 1).
  if (0 == next_id) {
    // This puts a technical limit on the number of times you can add a publisher or subscriber.
    // But even if you could add (and remove) them at 1 kHz (very optimistic rate)
    // it would still be a very long time before you could exhaust the pool of id's:
    //   2^64 / 1000 times per sec / 60 sec / 60 min / 24 hours / 365 days = 584,942,417 years
    // So around 585 million years. Even at 1 GHz, it would take 585 years.
    // I think it's safe to avoid trying to handle overflow.
    // If we roll over then it's most likely a bug.
    // *INDENT-OFF* (prevent uncrustify from making unnecessary indents here)
    throw std::overflow_error(
      "exhausted the unique id's for publishers and subscribers in this process "
      "(congratulations your computer is either extremely fast or extremely old)");
    // *INDENT-ON*
  }
  return next_id;
}

void
IntraProcessManager::insert_sub_id_for_pub(
  uint64_t sub_id,
  uint64_t pub_id,
  bool use_take_shared_method)
{
  if (use_take_shared_method) {
    pub_to_subs_[pub_id].take_shared_subscriptions.push_back(sub_id);
  } else {
    pub_to_subs_[pub_id].take_ownership_subscriptions.push_back(sub_id);
  }
}

bool
IntraProcessManager::can_communicate(
  rclcpp::PublisherBase::SharedPtr pub,
  rclcpp::experimental::SubscriptionIntraProcessBase::SharedPtr sub) const
{
  // publisher and subscription must be on the same topic
  if (strcmp(pub->get_topic_name(), sub->get_topic_name()) != 0) {
    return false;
  }

  auto check_result = rclcpp::qos_check_compatible(pub->get_actual_qos(), sub->get_actual_qos());
  if (check_result.compatibility == rclcpp::QoSCompatibility::Error) {
    return false;
  }

  return true;
}

bool
IntraProcessManager::can_communicate(
  ClientIntraProcessBase::SharedPtr client,
  ServiceIntraProcessBase::SharedPtr service) const
{
  // client and service must be under the same name
  if (strcmp(client->get_service_name(), service->get_service_name()) != 0) {
    return false;
  }

  auto check_result = rclcpp::qos_check_compatible(
    client->get_actual_qos(), service->get_actual_qos());

  if (check_result.compatibility == rclcpp::QoSCompatibility::Error) {
    return false;
  }

  return true;
}

bool
IntraProcessManager::can_communicate(
  ActionClientIntraProcessBase::SharedPtr client,
  ActionServerIntraProcessBase::SharedPtr service) const
{
  // client and service must be under the same name
  if (strcmp(client->get_action_name(), service->get_action_name()) != 0) {
    return false;
  }

  auto check_result = rclcpp::qos_check_compatible(
    client->get_actual_qos(), service->get_actual_qos());

  if (check_result.compatibility == rclcpp::QoSCompatibility::Error) {
    return false;
  }

  return true;
}

}  // namespace experimental
}  // namespace rclcpp
