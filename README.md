# Data Collection

## General Overview

This project contains two Flink streaming jobs implemented in Python:

1. **Data Cleaning and Publishing**: Reads data from a WebSocket, performs cleanup, and publishes it to a Kafka topic.
2. **Data Aggregation and Enrichment**: Reads data from a Kafka topic, aggregates it into a 20-second tumbling window,
   enriches the result with data from [Worldometers](https://www.worldometers.info/coronavirus/), and sends it to
   MongoDB.

## Message Flow Between Components
```
       +------------------------+  
       | Twitter Data Simulator |  
       |      (generator)       |  
       +------------------------+  
		   | 
		   | 
		   | 
		   v 
         +--------------------+  
         |   Collection Job   |  
         | (collector_stream) |  
         +--------------------+  
		   | 
		   | 
		   | 
		   v 
            +--------------+
            |  Kafka Topic |
            |   (tweets)   |
            +--------------+
		   | 
		   | 
		   | 
		   v 
         +---------------------+           +--------------+
         |   Aggregation Job   | ------->  | worldometers |
         | (aggregator_stream) |           |              |
         +---------------------+           +--------------+  
		   | 
		   | 
		   | 
		   v 
         +----------------------+
         |       MongoDB        |
         | (twitter_collection) |
         +----------------------+
```
## Description of Each Component

### Twitter Data Simulator (generator)

A script (`twitter_stream_simulator.py`) used to generate dummy data and publish it to a WebSocket on `localhost:5555`.

### Data Cleaning Job (collector_stream)

Reads data from the WebSocket, performs data cleanup, and publishes the cleaned data to the Kafka topic.

### Kafka Topic (tweets)

A Kafka topic where cleaned data is published by the data cleaning job and from which data is consumed by the
aggregation job.

### Aggregation Job (aggregator_stream)

Reads data from the Kafka topic, aggregates it into a 20-second tumbling window, enriches the data with information
from [Worldometers](https://www.worldometers.info/coronavirus/), and sends it to MongoDB.

### worldometers

Resource that represent Worldometers

### MongoDB (twitter_collection)

Saves enriched data into MongoDB.

## Steps to Run Everything

1. **Build the Docker Images**:
   ```sh
   make docker_build
   ```
2. **Start the Environment:**:
   ```sh
      make docker_up
   ```
2. **Package and deploy project:**:
   ```sh
      make clean package deploy
   ```
3. **Create Flink SQL Table to view result:**:
   ```sh
      make sql_client
   ```
   ```postgres-sql
   CREATE TABLE IF NOT EXISTS twitter_table (  
   `hash` STRING,
   `content` ARRAY<STRING>,
   `timestamp` STRING,
   `total_cases_count` BIGINT,
   `total_deaths_count` BIGINT,
   `total_recovered_count` BIGINT,
   `new_cases_count` BIGINT,
   `new_deaths_count` BIGINT,
   `new_recovered_count` BIGINT,
   `active_cases_count` BIGINT,
   `critical_cases_count` BIGINT,
     PRIMARY KEY (hash) NOT ENFORCED  
   ) WITH (  
   'connector' = 'mongodb',  
   'uri' = 'mongodb://mongodb:27017',  
   'database' = 'twitter_db',  
   'collection' = 'twitter_collection'
   );
   ```
4. **Run the Collector Stream:**:
   ```sh
      make remote_collection_stream
   ```
5. **Run the Aggregation Stream:**:
   ```sh
      make remote_aggregation_stream
   ```
6. **Shutdowm env:**:
   ```sh
      make docker_down
   ```
6. **Clean up:**:
   ```sh
      make docker_clean
   ```
