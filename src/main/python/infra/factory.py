from datetime import datetime

from pyflink.common import Configuration, Types, Duration, Row
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, TimeCharacteristic, \
    ExternalizedCheckpointCleanup, CheckpointingMode
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema

from core.job.aggregator_stream import AggregatorStream
from core.job.socket_stream import SocketStream
from core.streaming import Stream
from infra.service.provider import DataProvider
from infra.sink.kafka_value import KafkaValueSink
from infra.sink.mongo_value import MongoDbValueSink
from infra.source.http_value import HttpValueSource
from infra.source.kafka_value import KafkaValueSource


class Factory:
    class TweetTimestampAssigner(TimestampAssigner):
        def __init__(self):
            self.epoch = datetime.utcfromtimestamp(0)

        def extract_timestamp(self, value, record_timestamp) -> int:
            milliseconds = self._datetime_string_to_datetime(value[2])
            return int((milliseconds - self.epoch).total_seconds() * 1000)

        def _datetime_string_to_datetime(self, datetime_str) -> datetime:
            format_str = "%Y-%m-%d %H:%M:%S.%f"
            return datetime.strptime(datetime_str, format_str)

    _event_schema = (Types.ROW_NAMED(
        ["tweet_hash", "tweet", "event_time"],
        [Types.STRING(), Types.STRING(), Types.STRING()]))

    def get_local_env(self, checkpoint_path, jars_path) -> 'StreamExecutionEnvironment':
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(4)
        env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

        env.get_config().set_auto_watermark_interval(1000)
        env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

        env.enable_checkpointing(30000)
        env.get_checkpoint_config().set_min_pause_between_checkpoints(500)
        env.get_checkpoint_config().set_checkpoint_timeout(60000)
        env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(2)
        env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
        env.get_checkpoint_config().enable_externalized_checkpoints(
            ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        env.get_checkpoint_config().enable_unaligned_checkpoints()
        env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

        config = Configuration()
        config.set_string("state.checkpoint-storage", "filesystem")
        config.set_string("state.checkpoints.dir", f"file://{checkpoint_path}")
        env.configure(config)

        jar_files = [
            "file://" + jars_path + "bson-5.1.1.jar",
            "file://" + jars_path + "flink-connector-jdbc-3.2.0-1.19.jar",
            "file://" + jars_path + "flink-connector-kafka-3.2.0-1.19.jar",
            "file://" + jars_path + "flink-connector-mongodb-1.2.0-1.19.jar",
            "file://" + jars_path + "flink-csv-1.19.1.jar",
            "file://" + jars_path + "flink-faker-0.5.2.jar",
            "file://" + jars_path + "flink-json-1.19.1.jar",
            "file://" + jars_path + "flink-sql-avro-confluent-registry-1.19.1.jar",
            "file://" + jars_path + "flink-sql-connector-kafka-3.2.0-1.19.jar",
            "file://" + jars_path + "kafka-clients-3.2.3.jar",
            "file://" + jars_path + "mongodb-driver-core-5.1.1.jar",
            "file://" + jars_path + "mongodb-driver-sync-5.1.1.jar",
        ]
        env.add_jars(*jar_files)
        return env

    def create_local_socket_stream(self, env: 'StreamExecutionEnvironment', topic: str) -> Stream:
        tweet_http_source = HttpValueSource(
            host="localhost",
            port=5555,
            source_name="http_tweets_source")
        tweet_kafka_sink = KafkaValueSink[Row, Row](
            bootstrap_servers=["localhost:9092"],  # "localhost:19092"
            serialization_schema=(KafkaRecordSerializationSchema.builder()
                                  .set_topic_selector(lambda element: topic)
                                  .set_value_serialization_schema(JsonRowSerializationSchema
                                                                  .Builder()
                                                                  .with_type_info(self._event_schema)
                                                                  .build())
                                  .build()),
            sink_name="kafka_tweets_sink",
            event_translator=_socket_data_to_kafka_record)

        return SocketStream(env, tweet_http_source, tweet_kafka_sink)

    def create_local_aggregator_stream(self, env: 'StreamExecutionEnvironment', topic, group) -> Stream:
        watermark_strategy = (WatermarkStrategy
                              .for_monotonous_timestamps()
                              .with_timestamp_assigner(self.TweetTimestampAssigner())
                              .with_idleness(Duration.of_seconds(5)))
        tweet_kafka_source = KafkaValueSource(
            # bootstrap_servers=["broker-1:29092"],  # "localhost:9092", "broker-1:29092"
            bootstrap_servers=["localhost:9092"],  # "localhost:9092", "broker-1:29092"
            topic=topic,
            group=group,
            starting_offsets_initializer=KafkaOffsetsInitializer.earliest(),
            deserialization_schema=JsonRowDeserializationSchema
                .Builder()
                .type_info(self._event_schema)
                .build(),
            watermark_strategy=watermark_strategy,
            source_name="kafka_tweets_source")

        # go to mongodb container and do next:
        # 1 ./bin/mongosh
        # 2 use twitter_db
        # 3 db.createCollection("twitter_collection")
        # 4 read collection db.twitter_collection.find()
        schema = dict()
        schema["hash"] = Types.STRING()
        schema["content"] = Types.LIST(Types.STRING())
        schema["timestamp"] = Types.STRING()
        schema["total_cases_count"] = Types.LONG()
        schema["total_deaths_count"] = Types.LONG()
        schema["total_recovered_count"] = Types.LONG()
        schema["new_cases_count"] = Types.LONG()
        schema["new_deaths_count"] = Types.LONG()
        schema["new_recovered_count"] = Types.LONG()
        schema["active_cases_count"] = Types.LONG()
        schema["critical_cases_count"] = Types.LONG()
        tweet_mongo_sink = MongoDbValueSink[Row, Row](
            port=27017,
            host="localhost",  # localhost, mongodb
            database_name="twitter_db",
            collection_name="twitter_collection",
            table_name="twitter_table",
            flink_schema=schema,
            primary_keys=["hash"],
            sink_name="mongo_tweets_sink",
            event_translator=_aggregated_data_to_mongo_record)
        return AggregatorStream(env, tweet_kafka_source, tweet_mongo_sink, lambda: DataProvider())


def _socket_data_to_kafka_record(row: Row) -> Row:
    return row


def _aggregated_data_to_mongo_record(row: Row) -> Row:
    return Row(hash=str(row[0]),
               content=row[1],
               timestamp=str(row[2]),
               total_cases_count=int(row[3]),
               total_deaths_count=int(row[4]),
               total_recovered_count=int(row[5]),
               new_cases_count=int(row[6]),
               new_deaths_count=int(row[7]),
               new_recovered_count=int(row[8]),
               active_cases_count=int(row[9]),
               critical_cases_count=int(row[10]))
