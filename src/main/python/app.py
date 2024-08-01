import logging
import sys
from datetime import datetime, timedelta

from pyflink.common import Types, SimpleStringSchema, Configuration, ExecutionConfig, WatermarkStrategy, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, CheckpointingMode, \
    ExternalizedCheckpointCleanup, TimeCharacteristic
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema

# from core.job.aggregator_stream import AggregatorStream
from core.job.aggregator_stream import AggregatorStream
from core.job.socket_stream import SocketStream
from infra.sink.kafka_value import KafkaValueSink
from infra.sink.mongo_value import MongoDbValueSink
from infra.source.http_value import HttpValueSource
from infra.source.kafka_value import KafkaValueSource

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

if __name__ == "__main__":
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
    env.get_checkpoint_config().enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.get_checkpoint_config().enable_unaligned_checkpoints()
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

    config = Configuration()
    config.set_string("state.checkpoint-storage", "filesystem")
    config.set_string("state.checkpoints.dir",
                      "file:///Users/pavlolobachov/PycharmProjects/covid-case-reporter-stream/checkpoints"
                      # "file:///opt/checkpoints"
                      )
    env.configure(config)

    jars_path = "/Users/pavlolobachov/Downloads/flink_jars/"
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

    event_schema = (Types.ROW_NAMED(
        ["tweet_hash", "tweet", "event_time"],
        [Types.STRING(), Types.STRING(), Types.STRING()]))

    topic = "tweets"
    group = "g12"


    # tweet_http_source = HttpValueSource(
    #     host="localhost",
    #     port=5555,
    #     source_name="http_tweets_source")
    # tweet_kafka_sink = KafkaValueSink(
    #     bootstrap_servers=["localhost:9092"],  # "localhost:19092"
    #     serialization_schema=(KafkaRecordSerializationSchema.builder()
    #                           .set_topic_selector(lambda element: topic)
    #                           .set_value_serialization_schema(JsonRowSerializationSchema
    #                                                           .Builder()
    #                                                           .with_type_info(event_schema)
    #                                                           .build())
    #                           .build()),
    #     sink_name="kafka_tweets_sink")
    #
    # stream1 = SocketStream(env, tweet_http_source, tweet_kafka_sink)
    # stream1.process()

    class TweetTimestampAssigner(TimestampAssigner):
        def __init__(self):
            self.epoch = datetime.utcfromtimestamp(0)

        def extract_timestamp(self, value, record_timestamp) -> int:
            milliseconds = self._datetime_string_to_datetime(value[2])
            return int((milliseconds - self.epoch).total_seconds() * 1000)

        def _datetime_string_to_datetime(self, datetime_str) -> datetime:
            format_str = "%Y-%m-%d %H:%M:%S.%f"
            return datetime.strptime(datetime_str, format_str)


    watermark_strategy = (WatermarkStrategy
                          .for_monotonous_timestamps()
                          .with_timestamp_assigner(TweetTimestampAssigner())
                          .with_idleness(Duration.of_seconds(5)))
    tweet_kafka_source = KafkaValueSource(
        # bootstrap_servers=["broker-1:29092"],  # "localhost:9092", "broker-1:29092"
        bootstrap_servers=["localhost:9092"],  # "localhost:9092", "broker-1:29092"
        topic=topic,
        group=group,
        starting_offsets_initializer=KafkaOffsetsInitializer.earliest(),
        deserialization_schema=JsonRowDeserializationSchema
            .Builder()
            .type_info(event_schema)
            .build(),
        watermark_strategy=watermark_strategy,
        source_name="kafka_tweets_source")

    # go to mongodb container and do next:
    # 1 ./bin/mongosh
    # 2 use twitter_db
    # 3 db.createCollection("twitter_collection")
    # 4 read collection db.twitter_collection.find()
    schema = dict()
    schema["tweet_hash"] = Types.STRING()
    schema["tweets"] = Types.LIST(Types.STRING())
    schema["event_time"] = Types.STRING()
    tweet_mongo_sink = MongoDbValueSink(
        port=27017,
        host="localhost",  # localhost, mongodb
        database_name="twitter_db",
        collection_name="twitter_collection",
        table_name="twitter_table",
        flink_schema=schema,
        primary_keys=["tweet_hash"],
        sink_name="mongo_tweets_sink")

    stream2 = AggregatorStream(env, tweet_kafka_source, tweet_mongo_sink)
    stream2.process()

    # env.execute_async(job_name="TwitterCovidStreamingJob")
    env.execute(job_name="TwitterCovidStreamingJob")
