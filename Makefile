include flink-env.env

PLATFORM ?= linux/amd64

# COLORS
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)

PACKAGE_NAME = pyflink
VERSION = 1.0.0
SRC_DIR = src
CONFIG_DIR = config
BUILD_DIR = build
DIST_DIR = dist
VENV_DIR = virtual_env

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
docker_build:
	docker build --platform ${PLATFORM} -t ${IMAGE_NAME} .

.PHONY: docker_build_no_cache
## Builds the Flink base image with pyFlink and connectors installed from scratch
docker_build_no_cache:
	docker build --no-cache --platform ${PLATFORM} -t ${IMAGE_NAME} .

.PHONY: docker_up
## Builds the base Docker image and starts Flink cluster
docker_up:
	docker compose --env-file flink-env.env up --build --remove-orphans  -d

.PHONY: docker_down
## Shuts down the Flink cluster
docker_down:
	docker compose down --remove-orphans

.PHONY: docker_logs
## Follow logs
logs:
	docker compose logs -f

.PHONY: job
## Submit the Flink job
remote_collection_stream:
	docker compose exec jobmanager bash -c \
	"cd /opt && ./flink/bin/flink run --pyFiles /opt/target/job/src,/opt/target/job/config --pyModule target.job.src.app dev collect" \
	-d

remote_aggregation_stream:
	docker compose exec jobmanager bash -c \
	"cd /opt && ./flink/bin/flink run --pyFiles /opt/target/job/src,/opt/target/job/config --pyModule target.job.src.app dev aggregate" \
	-d

local_collection_stream:
	docker compose exec jobmanager bash -c \
	"cd /opt && ./flink/bin/flink run --pyFiles /opt/job/src,/opt/job/config --pyModule job.src.app dev collect" \
	-d

local_aggregation_stream:
	docker compose exec jobmanager bash -c \
	"cd /opt && ./flink/bin/flink run --pyFiles /opt/job/src,/opt/job/config --pyModule job.src.app dev aggregate" \
	-d

sql_client:
	docker compose exec jobmanager ./bin/sql-client.sh

.PHONY: docker_stop
## Stops all services in Docker compose
docker_stop:
	docker compose stop

.PHONY: docker_start
## Starts all services in Docker compose
docker_start:
	docker compose start

.PHONY: docker_clean
## Stops and removes the Docker container as well as images with tag `<none>`
docker_clean:
	docker compose stop
	docker ps -a --format '{{.Names}}' | grep "^${CONTAINER_PREFIX}" | xargs -I {} docker rm {}
	docker images | grep "<none>" | awk '{print $3}' | xargs -r docker rmi
	# Uncomment line `docker rmi` if you want to remove the Docker image from this set up too
	# docker rmi ${IMAGE_NAME}

.PHONY: build
clean:
## Clean target: removes previous build and distribution directories
	rm -rf $(BUILD_DIR) $(DIST_DIR) $(VENV_DIR)

## Package target: installs dependencies, copies source and config files, and creates a zip package
package:
	mkdir -p $(BUILD_DIR) $(DIST_DIR)
	cp -r $(SRC_DIR) $(BUILD_DIR)/$(SRC_DIR)
	cp -r $(CONFIG_DIR) $(BUILD_DIR)/$(CONFIG_DIR)
	cd $(BUILD_DIR) && zip -r ../$(DIST_DIR)/$(PACKAGE_NAME)-$(VERSION).zip .

deploy:
	docker compose exec jobmanager bash -c "rm -rf /opt/target/${PACKAGE_NAME}-${VERSION} && mkdir -p /opt/target/${PACKAGE_NAME}-${VERSION}"
	docker compose cp $(DIST_DIR)/${PACKAGE_NAME}-${VERSION}.zip jobmanager:/opt/${PACKAGE_NAME}-${VERSION}.zip
	docker compose exec jobmanager bash -c "unzip /opt/${PACKAGE_NAME}-${VERSION}.zip -d /opt/target/job"