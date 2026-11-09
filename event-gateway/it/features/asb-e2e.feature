# --------------------------------------------------------------------
# Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
#
# WSO2 LLC. licenses this file to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# --------------------------------------------------------------------

# Runs only against docker-compose.test.asb.yaml; see the test-asb make target.
# Waits are longer than the Kafka equivalents because every entity operation is
# an Azure management round trip.

@asb
Feature: Azure Service Bus End-to-End Flow
  As an event publisher and subscriber
  I want the gateway to work identically when a WebSub API is backed by Azure Service Bus
  So that broker choice stays a deployment decision

  Background:
    Given the event gateway services are running

  # ==================== BROKER SELECTION ====================

  Scenario: A WebSub API can select the Azure Service Bus driver
    Given I authenticate using basic auth as "admin"
    When I create a WebSub API with the following configuration:
      """
      {
        "apiVersion": "gateway.api-platform.wso2.com/v1",
        "kind": "WebSubApi",
        "metadata": { "name": "asb-select-v1-0" },
        "spec": {
          "displayName": "asb-select",
          "version": "v1.0",
          "context": "/asb-select",
          "channels": { "issues": {} },
          "broker": { "type": "azureservicebus" },
          "deploymentState": "deployed"
        }
      }
      """
    Then the response should be successful

    # Reachability rather than a fixed sleep: the runtime must receive the xDS
    # push and provision the topic in Azure before the route answers.
    And the WebSub API "asb-select" version "v1.0" is reachable within 30 seconds

    # Cleanup
    Given I authenticate using basic auth as "admin"
    When I delete the WebSub API "asb-select-v1-0"
    Then the response should be successful

  # ==================== SUBSCRIBE ====================

  Scenario: Subscribing on an ASB-backed API returns 202 Accepted
    Given I authenticate using basic auth as "admin"
    When I create a WebSub API with the following configuration:
      """
      {
        "apiVersion": "gateway.api-platform.wso2.com/v1",
        "kind": "WebSubApi",
        "metadata": { "name": "asb-subscribe-v1-0" },
        "spec": {
          "displayName": "asb-subscribe",
          "version": "v1.0",
          "context": "/asb-subscribe",
          "channels": { "issues": {} },
          "broker": { "type": "azureservicebus" },
          "deploymentState": "deployed"
        }
      }
      """
    Then the response should be successful
    And the WebSub API "asb-subscribe" version "v1.0" is reachable within 30 seconds

    # Exercises TopicExists against Azure, then WebSub intent verification —
    # a synchronous callback the listener must echo before anything is stored.
    When I subscribe to topic "issues" on API "asb-subscribe" version "v1.0" with callback "http://wh-listener:8090/"
    Then the response status code should be 202

    # Cleanup
    Given I authenticate using basic auth as "admin"
    When I delete the WebSub API "asb-subscribe-v1-0"
    Then the response should be successful

  Scenario: Subscribing to a channel the API does not declare is rejected
    Given I authenticate using basic auth as "admin"
    When I create a WebSub API with the following configuration:
      """
      {
        "apiVersion": "gateway.api-platform.wso2.com/v1",
        "kind": "WebSubApi",
        "metadata": { "name": "asb-bad-topic-v1-0" },
        "spec": {
          "displayName": "asb-bad-topic",
          "version": "v1.0",
          "context": "/asb-bad-topic",
          "channels": { "issues": {} },
          "broker": { "type": "azureservicebus" },
          "deploymentState": "deployed"
        }
      }
      """
    Then the response should be successful
    And the WebSub API "asb-bad-topic" version "v1.0" is reachable within 30 seconds

    When I subscribe to topic "imaginary" on API "asb-bad-topic" version "v1.0" with callback "http://wh-listener:8090/"
    Then the response status code should be 404

    # Cleanup
    Given I authenticate using basic auth as "admin"
    When I delete the WebSub API "asb-bad-topic-v1-0"
    Then the response should be successful

  # ==================== PUBLISH AND DELIVER ====================

  Scenario: Full subscribe, publish, and delivery flow over Service Bus
    Given I authenticate using basic auth as "admin"
    When I create a WebSub API with the following configuration:
      """
      {
        "apiVersion": "gateway.api-platform.wso2.com/v1",
        "kind": "WebSubApi",
        "metadata": { "name": "asb-full-v1-0" },
        "spec": {
          "displayName": "asb-full",
          "version": "v1.0",
          "context": "/asb-full",
          "channels": {
            "issues": {},
            "commits": {}
          },
          "broker": { "type": "azureservicebus" },
          "deploymentState": "deployed"
        }
      }
      """
    Then the response should be successful
    And the WebSub API "asb-full" version "v1.0" is reachable within 30 seconds

    When I subscribe to topic "issues" on API "asb-full" version "v1.0" with callback "http://wh-listener:8090/"
    Then the response status code should be 202

    # The consumer's ASB Subscription must exist before a publish can reach it.
    And I wait for 5 seconds

    When I publish event "asb-issue-opened" to topic "issues" on API "asb-full" version "v1.0"
    Then the response status code should be 202

    And I wait for event delivery for 10 seconds
    Then the webhook listener should have received the event "asb-issue-opened"

    # Cleanup
    Given I authenticate using basic auth as "admin"
    When I delete the WebSub API "asb-full-v1-0"
    Then the response should be successful

  Scenario: Each channel routes to its own Service Bus topic
    Given I authenticate using basic auth as "admin"
    When I create a WebSub API with the following configuration:
      """
      {
        "apiVersion": "gateway.api-platform.wso2.com/v1",
        "kind": "WebSubApi",
        "metadata": { "name": "asb-channels-v1-0" },
        "spec": {
          "displayName": "asb-channels",
          "version": "v1.0",
          "context": "/asb-channels",
          "channels": {
            "issues": {},
            "commits": {}
          },
          "broker": { "type": "azureservicebus" },
          "deploymentState": "deployed"
        }
      }
      """
    Then the response should be successful
    And the WebSub API "asb-channels" version "v1.0" is reachable within 30 seconds

    When I subscribe to topic "commits" on API "asb-channels" version "v1.0" with callback "http://wh-listener:8090/"
    Then the response status code should be 202
    And I wait for 5 seconds

    # Published to the channel that was subscribed — must arrive.
    When I publish event "asb-commit-pushed" to topic "commits" on API "asb-channels" version "v1.0"
    Then the response status code should be 202
    And I wait for event delivery for 10 seconds
    Then the webhook listener should have received the event "asb-commit-pushed"

    # Cleanup
    Given I authenticate using basic auth as "admin"
    When I delete the WebSub API "asb-channels-v1-0"
    Then the response should be successful

  # ==================== UNSUBSCRIBE ====================

  Scenario: Unsubscribing on an ASB-backed API is accepted
    Given I authenticate using basic auth as "admin"
    When I create a WebSub API with the following configuration:
      """
      {
        "apiVersion": "gateway.api-platform.wso2.com/v1",
        "kind": "WebSubApi",
        "metadata": { "name": "asb-unsub-v1-0" },
        "spec": {
          "displayName": "asb-unsub",
          "version": "v1.0",
          "context": "/asb-unsub",
          "channels": { "news": {} },
          "broker": { "type": "azureservicebus" },
          "deploymentState": "deployed"
        }
      }
      """
    Then the response should be successful
    And the WebSub API "asb-unsub" version "v1.0" is reachable within 30 seconds

    When I subscribe to topic "news" on API "asb-unsub" version "v1.0" with callback "http://wh-listener:8090/"
    Then the response status code should be 202
    And I wait for 5 seconds

    # The driver deletes the subscriber's Subscription here. Kafka has no such
    # entity, so DeleteSubscription sits outside the BrokerDriver interface.
    When I unsubscribe from topic "news" on API "asb-unsub" version "v1.0" with callback "http://wh-listener:8090/"
    Then the response status code should be 202

    # Cleanup
    Given I authenticate using basic auth as "admin"
    When I delete the WebSub API "asb-unsub-v1-0"
    Then the response should be successful

  # ==================== RESTART RECOVERY ====================

  # The one behaviour ASB cannot inherit from Kafka: there is nothing to replay,
  # so surviving a restart depends entirely on the snapshot queue.
  Scenario: A subscription survives a gateway restart
    Given I authenticate using basic auth as "admin"
    When I create a WebSub API with the following configuration:
      """
      {
        "apiVersion": "gateway.api-platform.wso2.com/v1",
        "kind": "WebSubApi",
        "metadata": { "name": "asb-restart-v1-0" },
        "spec": {
          "displayName": "asb-restart",
          "version": "v1.0",
          "context": "/asb-restart",
          "channels": { "issues": {} },
          "broker": { "type": "azureservicebus" },
          "deploymentState": "deployed"
        }
      }
      """
    Then the response should be successful
    And the WebSub API "asb-restart" version "v1.0" is reachable within 30 seconds

    # The route answers before Start() finishes provisioning, and a subscribe in
    # that window reaches the sync topic with nothing subscribed to it yet. The
    # cache subscription is that subscriber, so its existence closes the window.
    And the ASB cache subscription for API "asb-restart-v1-0" version "v1.0" should exist within 30 seconds

    When I subscribe to topic "issues" on API "asb-restart" version "v1.0" with callback "http://wh-listener:8090/"
    Then the response status code should be 202

    # Restarting before the snapshot is written loses the subscriber, so wait on
    # the condition rather than a fixed sleep, which was flaky both ways.
    Then the ASB snapshot for API "asb-restart-v1-0" version "v1.0" should contain 1 subscribers within 60 seconds

    When I restart the event gateway
    And the WebSub API "asb-restart" version "v1.0" is reachable within 60 seconds

    # Replay runs inside Start(), behind the route as above. Nothing observable
    # marks it done, so this is a timing assumption rather than a condition.
    And I wait for 15 seconds

    # Nobody re-subscribed. If this delivers, the subscriber list came back
    # from the snapshot queue.
    When I publish event "asb-survived-restart" to topic "issues" on API "asb-restart" version "v1.0"
    Then the response status code should be 202
    And I wait for event delivery for 15 seconds
    Then the webhook listener should have received the event "asb-survived-restart"

    # Cleanup
    Given I authenticate using basic auth as "admin"
    When I delete the WebSub API "asb-restart-v1-0"
    Then the response should be successful

  # ==================== SELF-HEALING ====================

  # Azure reclaims the per-instance live-tail subscription when idle, and losing
  # it silently would freeze the snapshot. Does not subscribe: recovery only.
  Scenario: The live-tail subscription is rebuilt after being deleted
    Given I authenticate using basic auth as "admin"
    When I create a WebSub API with the following configuration:
      """
      {
        "apiVersion": "gateway.api-platform.wso2.com/v1",
        "kind": "WebSubApi",
        "metadata": { "name": "asb-heal-v1-0" },
        "spec": {
          "displayName": "asb-heal",
          "version": "v1.0",
          "context": "/asb-heal",
          "channels": { "issues": {} },
          "broker": { "type": "azureservicebus" },
          "deploymentState": "deployed"
        }
      }
      """
    Then the response should be successful
    And the WebSub API "asb-heal" version "v1.0" is reachable within 30 seconds
    And the ASB cache subscription for API "asb-heal-v1-0" version "v1.0" should exist within 30 seconds

    When I delete the ASB cache subscription for API "asb-heal-v1-0" version "v1.0"
    Then the ASB cache subscription for API "asb-heal-v1-0" version "v1.0" should exist within 90 seconds

    # Cleanup
    Given I authenticate using basic auth as "admin"
    When I delete the WebSub API "asb-heal-v1-0"
    Then the response should be successful


  # ==================== WEBBROKER ====================

  # The other half of the scope: Subscribe, Publish and EnsureTopics through the
  # websocket receiver, which the WebSub scenarios never touch.
  Scenario: A WebBroker API can expose Service Bus over WebSocket
    Given I authenticate using basic auth as "admin"
    When I create a WebBroker API with the following configuration:
      """
      {
        "apiVersion": "gateway.api-platform.wso2.com/v1",
        "kind": "WebBrokerApi",
        "metadata": { "name": "asb-broker-echo-v1.0" },
        "spec": {
          "displayName": "ASB Broker Echo API",
          "version": "v1.0",
          "context": "/asb-broker-echo/v1.0",
          "receiver": { "name": "websocket-receiver", "type": "websocket" },
          "broker": {
            "name": "asb-driver",
            "type": "azureservicebus"
          },
          "allChannels": {
            "on_connection_init": { "policies": [] },
            "on_produce": { "policies": [] },
            "on_consume": { "policies": [] }
          },
          "channels": {
            "asbecho": {
              "on_connection_init": { "policies": [] },
              "on_produce": { "policies": [] },
              "on_consume": { "policies": [] }
            }
          }
        }
      }
      """
    Then the response should be successful
    And the WebBroker API at "/asb-broker-echo/v1.0" is reachable within 30 seconds

    # The route answers before Start() creates the topic, so connecting early
    # fails to build its consumer.
    And I wait for 10 seconds

    When I connect to WebBroker API "/asb-broker-echo/v1.0" on channel "asbecho"

    # Longer than Kafka's 1 second: ASB never delivers a message published
    # before the Subscription existed.
    And I wait for 5 seconds
    And I send WebSocket message "asb-echo-hello"
    Then I should receive a WebSocket message containing "asb-echo-hello" within 15 seconds

    When I close the WebSocket connection

    # Cleanup
    Given I authenticate using basic auth as "admin"
    When I delete the WebBroker API "asb-broker-echo-v1.0"
    Then the response should be successful