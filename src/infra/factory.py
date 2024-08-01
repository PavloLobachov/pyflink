import configparser
import os
from datetime import datetime
from pathlib import Path

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
    def __init__(self, env: str):
        self.env = env
        self.config = self._load_config(env)

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

    def create_local_socket_stream(self, env: 'StreamExecutionEnvironment') -> Stream:
        tweet_http_source = HttpValueSource[str, str](
            host=self.config['web_socket']['host'],
            port=int(self.config['web_socket']['port']),
            source_name="http_tweets_source")
        tweet_kafka_sink = KafkaValueSink[Row, Row](
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            serialization_schema=(KafkaRecordSerializationSchema.builder()
                                  .set_topic_selector(lambda element: self.config['kafka']['topic'])
                                  .set_value_serialization_schema(JsonRowSerializationSchema
                                                                  .Builder()
                                                                  .with_type_info(self._event_schema)
                                                                  .build())
                                  .build()),
            event_translator=_socket_data_to_kafka_record,
            sink_name="kafka_tweets_sink")

        return SocketStream(env, tweet_http_source, tweet_kafka_sink)

    def create_local_aggregator_stream(self, env: 'StreamExecutionEnvironment') -> Stream:
        watermark_strategy = (WatermarkStrategy
                              .for_monotonous_timestamps()
                              .with_timestamp_assigner(self.TweetTimestampAssigner())
                              .with_idleness(Duration.of_seconds(5)))
        tweet_kafka_source = KafkaValueSource[str, Row](
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            topic=self.config['kafka']['topic'],
            group=self.config['kafka']['group'],
            starting_offsets_initializer=KafkaOffsetsInitializer.earliest(),
            deserialization_schema=JsonRowDeserializationSchema
                .Builder()
                .type_info(self._event_schema)
                .build(),
            watermark_strategy=watermark_strategy,
            event_translator=_socket_kafka_record_to_row,
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
            port=int(self.config['mongo']['port']),
            host=self.config['mongo']['host'],
            database_name=self.config['mongo']['database_name'],
            collection_name=self.config['mongo']['collection_name'],
            table_name=self.config['mongo']['table_name'],
            flink_schema=schema,
            primary_keys=["hash"],
            event_translator=_aggregated_data_to_mongo_record,
            sink_name="mongo_tweets_sink")
        return AggregatorStream(env, tweet_kafka_source, tweet_mongo_sink, lambda: DataProvider())

    def get_env(self, job_name) -> 'StreamExecutionEnvironment':
        streaming_env = StreamExecutionEnvironment.get_execution_environment()
        streaming_env.set_parallelism(int(self.config['DEFAULT']['parallelism']))
        streaming_env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

        streaming_env.get_config().set_auto_watermark_interval(int(self.config['DEFAULT']['checkpoint_interval']))
        streaming_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

        streaming_env.enable_checkpointing(
            int(self.config['DEFAULT']['checkpoint_interval']))
        streaming_env.get_checkpoint_config().set_min_pause_between_checkpoints(
            int(self.config['DEFAULT']['min_pause_between_checkpoints']))
        streaming_env.get_checkpoint_config().set_checkpoint_timeout(
            int(self.config['DEFAULT']['checkpoint_timeout']))
        streaming_env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(
            int(self.config['DEFAULT']['tolerable_checkpoint_failure_number']))
        streaming_env.get_checkpoint_config().set_max_concurrent_checkpoints(
            int(self.config['DEFAULT']['max_concurrent_checkpoints']))
        streaming_env.get_checkpoint_config().enable_externalized_checkpoints(
            ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        streaming_env.get_checkpoint_config().enable_unaligned_checkpoints()
        streaming_env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

        config = Configuration()
        config.set_string("state.checkpoint-storage", "filesystem")
        config.set_string("state.checkpoints.dir", f"{self.config['DEFAULT']['checkpoint_path']}/{job_name}")
        streaming_env.configure(config)

        if self.env == 'local':
            jar_files = self.config['jars']['jars_files'].split(",")
            streaming_env.add_jars(*jar_files)
            return streaming_env
        elif self.env == 'dev':
            return streaming_env
        else:
            raise ValueError(f"Unsupported env: {self.env}")

    def _load_config(self, env: str):
        config = configparser.ConfigParser()
        base_path = os.path.dirname(os.path.abspath(__file__))
        config.read(os.path.join(base_path, '..', '..', 'config', 'base_config.ini'))
        if env == 'local':
            config.read(os.path.join(base_path, '..', '..', 'config', 'local', 'config.ini'))
        elif env == 'dev':
            config.read(os.path.join(base_path, '..', '..', 'config', 'dev', 'config.ini'))
        return config


def _socket_kafka_record_to_row(data: str) -> Row:
    return Row(data)


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
