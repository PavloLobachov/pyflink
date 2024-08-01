include flink-env.env

PLATFORM ?= linux/amd64

# COLORS
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)

PACKAGE_NAME = streaming
VERSION = 1.0.0
SRC_DIR = src
CONFIG_DIR = config
BUILD_DIR = build
DIST_DIR = dist

TARGET_MAX_CHAR_NUM=20

## Show help with `make help`
help:
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${GREEN}%s${RESET}\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

.PHONY: docker_build
## Builds the Flink base image with pyFlink and connectors installed
build:
	docker build --platform ${PLATFORM} -t ${IMAGE_NAME} .

.PHONY: docker_build_no_cache
## Builds the Flink base image with pyFlink and connectors installed from scratch
build_no_cache:
	docker build --no-cache --platform ${PLATFORM} -t ${IMAGE_NAME} .

.PHONY: docker_up
## Builds the base Docker image and starts Flink cluster
up:
	docker compose --env-file flink-env.env up --build --remove-orphans  -d

.PHONY: docker_down
## Shuts down the Flink cluster
down:
	docker compose down --remove-orphans

.PHONY: job
## Submit the Flink job
ingestion_stream:
	docker compose exec jobmanager ./bin/flink run -py /opt/src/app.py --pyFiles /opt/src -d

aggregation_stream:
	docker compose exec jobmanager ./bin/flink run -py /opt/src/app.py --pyFiles /opt/src -d

sql_client:
	docker compose exec jobmanager ./bin/sql-client.sh

zip:
	docker compose exec jobmanager ./bin/flink run -py /opt/dist/my_project-1.0.0.zip -pymodule app

.PHONY: docker_stop
## Stops all services in Docker compose
stop:
	docker compose stop

.PHONY: docker_start
## Starts all services in Docker compose
start:
	docker compose start

.PHONY: docker_clean
## Stops and removes the Docker container as well as images with tag `<none>`
clean:
	docker compose stop
	docker ps -a --format '{{.Names}}' | grep "^${CONTAINER_PREFIX}" | xargs -I {} docker rm {}
	docker images | grep "<none>" | awk '{print $3}' | xargs -r docker rmi
	# Uncomment line `docker rmi` if you want to remove the Docker image from this set up too
	# docker rmi ${IMAGE_NAME}

.PHONY: clean
clean:
## Clean target: removes previous build and distribution directories
	rm -rf $(BUILD_DIR) $(DIST_DIR)

.PHONY: package
## Package target: installs dependencies, copies source and config files, and creates a zip package
package:
	mkdir -p $(BUILD_DIR) $(DIST_DIR)
	pip install -r requirements.txt --target $(BUILD_DIR)
	cp -r $(SRC_DIR)/* $(BUILD_DIR)
	cp -r $(CONFIG_DIR) $(BUILD_DIR)/config
	cd $(BUILD_DIR) && zip -r ../$(DIST_DIR)/$(PACKAGE_NAME)-$(VERSION).zip .