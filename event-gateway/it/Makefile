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

# Event Gateway Integration Tests
SHELL := /bin/bash

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@echo 'Event Gateway Integration Tests'
	@echo ''
	@echo 'Usage:'
	@echo '  make test                  Run the full integration test suite (SQLite)'
	@echo '  make test-postgres         Run the full integration test suite with PostgreSQL'
	@echo '  make test-sqlserver        Run the full integration test suite with SQL Server'
	@echo '  make test-asb              Run the Azure Service Bus feature (needs a namespace)'
	@echo '  make test-health           Run only health-check scenarios'
	@echo '  make test-websub           Run only WebSub management scenarios'
	@echo '  make test-e2e              Run only end-to-end WebSub flow scenarios'
	@echo '  make test-webbroker        Run only WebBroker management scenarios'
	@echo '  make test-webbroker-e2e    Run only end-to-end WebBroker flow scenarios'
	@echo '  make tidy                  Run go mod tidy'
	@echo '  make clean                 Remove generated logs and test artefacts'

.PHONY: test
test: ## Run all integration tests
	go test -v -timeout 10m ./...

.PHONY: test-postgres
test-postgres: ## Run integration tests with PostgreSQL backend
	IT_COMPOSE_FILE=../docker-compose.test.postgres.yaml go test -v -timeout 10m ./...

.PHONY: test-sqlserver
test-sqlserver: ## Run integration tests with SQL Server backend
	IT_COMPOSE_FILE=../docker-compose.test.sqlserver.yaml go test -v -timeout 10m ./...

# Unlike every other target this one needs a reachable Azure Service Bus
# namespace: Service Bus has no container image, so it cannot be started as
# part of the stack. Requires APIP_EGW_ASB_CONNECTION_STRING, on a policy with
# Manage rights. The timeout is longer than the Kafka equivalents because every
# entity operation is a cloud round trip.
#
# -count=1 defeats Go's test cache. Everything this suite actually exercises —
# the gateway image, the containers, the namespace — is invisible to that cache,
# so a rebuilt image or a changed driver would otherwise replay a stale pass.
.PHONY: test-asb
test-asb: ## Run Azure Service Bus end-to-end feature (needs APIP_EGW_ASB_CONNECTION_STRING)
	@test -n "$$APIP_EGW_ASB_CONNECTION_STRING" || \
		{ echo "APIP_EGW_ASB_CONNECTION_STRING is not set — see docker-compose.test.asb.yaml"; exit 1; }
	IT_COMPOSE_FILE=../docker-compose.test.asb.yaml \
	IT_FEATURE_PATHS=asb-e2e.feature \
	go test -v -count=1 -timeout 15m ./...

.PHONY: test-health
test-health: ## Run health check feature only
	IT_FEATURE_PATHS=health.feature go test -v -timeout 5m ./...

.PHONY: test-websub
test-websub: ## Run WebSub API management feature only
	IT_FEATURE_PATHS=websub-api-management.feature go test -v -timeout 5m ./...

.PHONY: test-e2e
test-e2e: ## Run WebSub end-to-end feature only
	IT_FEATURE_PATHS=websub-e2e.feature go test -v -timeout 10m ./...

.PHONY: test-webbroker
test-webbroker: ## Run WebBroker API management feature only
	IT_FEATURE_PATHS=webbroker-api-management.feature go test -v -timeout 5m ./...

.PHONY: test-webbroker-e2e
test-webbroker-e2e: ## Run WebBroker end-to-end feature only
	IT_FEATURE_PATHS=webbroker-e2e.feature go test -v -timeout 10m ./...

.PHONY: tidy
tidy: ## Tidy go modules
	go mod tidy

.PHONY: clean
clean: ## Remove generated artefacts
	rm -rf logs/
