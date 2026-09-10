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

# Runs only against docker-compose.test.asb.yaml, which needs
# APIP_EGW_ASB_CONNECTION_STRING in the environment. See it/Makefile target
# test-asb.
#
# Waits are longer than the Kafka equivalents on purpose. Every entity
# operation here is an Azure management API round trip, and a subscribe
# reaches the driver's cache only after its sync message travels out to the
# broker and back — measured at roughly four seconds against a live namespace.

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

    # The driver deletes the subscriber's ASB Subscription here — Kafka has no
    # equivalent entity, which is why DeleteSubscription sits outside the
    # BrokerDriver interface and is reached by an optional type assertion.
    When I unsubscribe from topic "news" on API "asb-unsub" version "v1.0" with callback "http://wh-listener:8090/"
    Then the response status code should be 202

    # Cleanup
    Given I authenticate using basic auth as "admin"
    When I delete the WebSub API "asb-unsub-v1-0"
    Then the response should be successful

  # ==================== PENDING — needs steps added in it/steps.go ====================
  #
  # The scenario below is the one this whole driver exists for: Service Bus
  # deletes consumed messages and a new Subscription never receives anything
  # published before it existed, so subscriber state is rebuilt from a snapshot
  # queue guarded by leader election. Nothing above proves that.
  #
  # It needs one step that does not exist yet — "I restart the event gateway",
  # a compose restart of the event-gateway service. Uncomment once that step is
  # registered, or godog will fail the run as undefined (Strict is on).
  #
  # Scenario: A subscription survives a gateway restart
  #   Given I authenticate using basic auth as "admin"
  #   When I create a WebSub API with the following configuration:
  #     """
  #     {
  #       "apiVersion": "gateway.api-platform.wso2.com/v1",
  #       "kind": "WebSubApi",
  #       "metadata": { "name": "asb-restart-v1-0" },
  #       "spec": {
  #         "displayName": "asb-restart",
  #         "version": "v1.0",
  #         "context": "/asb-restart",
  #         "channels": { "issues": {} },
  #         "broker": { "type": "azureservicebus" },
  #         "deploymentState": "deployed"
  #       }
  #     }
  #     """
  #   Then the response should be successful
  #   And the WebSub API "asb-restart" version "v1.0" is reachable within 30 seconds
  #
  #   When I subscribe to topic "issues" on API "asb-restart" version "v1.0" with callback "http://wh-listener:8090/"
  #   Then the response status code should be 202
  #
  #   # Long enough for the snapshot write to land: the change reaches the
  #   # leader's cache only after the sync round trip, then a one-second debounce.
  #   And I wait for 10 seconds
  #
  #   When I restart the event gateway
  #   And the WebSub API "asb-restart" version "v1.0" is reachable within 60 seconds
  #
  #   # Nobody re-subscribed. If this delivers, the subscriber list came back
  #   # from the snapshot queue.
  #   When I publish event "asb-survived-restart" to topic "issues" on API "asb-restart" version "v1.0"
  #   Then the response status code should be 202
  #   And I wait for event delivery for 15 seconds
  #   Then the webhook listener should have received the event "asb-survived-restart"
  #
  #   # Cleanup
  #   Given I authenticate using basic auth as "admin"
  #   When I delete the WebSub API "asb-restart-v1-0"
  #   Then the response should be successful
