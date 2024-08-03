import configparser
import os

from pyflink.common import Configuration, Types, Duration, Row
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import RuntimeExecutionMode, TimeCharacteristic, \
    ExternalizedCheckpointCleanup, CheckpointingMode
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema

from core.job.aggregator_stream import AggregatorStream
from core.job.socket_stream import SocketStream
from core.sink import ValueSink
from core.source import ValueSource
from core.streaming import Stream
from infra.extractors import TweetTimestampAssigner
from infra.service.provider import DataProvider
from infra.sink.kafka_value import KafkaValueSink
from infra.sink.mongo_value import MongoDbValueSink
from infra.source.http_value import HttpValueSource
from infra.source.kafka_value import KafkaValueSource
from infra.translators import _socket_kafka_record_to_row, _aggregated_data_to_mongo_record, \
    _socket_data_to_kafka_record, _event_schema


class Factory:
    def __init__(self, env: str):
        self.env = env
        self.config = self._load_config(env)
        self.jars = self._load_jars()

    def create_socket_stream(self, job_name: str) -> Stream:
        config = Configuration()
        config.set_string("state.checkpoint-storage", "filesystem")
        config.set_string("state.checkpoints.dir", f"{self.config['DEFAULT']['checkpoint_path']}/{job_name}")
        stream = SocketStream(lambda: self._tweet_http_source(),
                              lambda: self._tweet_kafka_sink(),
                              job_name,
                              self.jars,
                              config)
        return self._configure(stream)

    def create_aggregator_stream(self, job_name: str) -> Stream:
        config = Configuration()
        config.set_string("state.checkpoint-storage", "filesystem")
        config.set_string("state.checkpoints.dir", f"{self.config['DEFAULT']['checkpoint_path']}/{job_name}")
        stream = AggregatorStream(lambda: self._tweet_kafka_source(),
                                  lambda: self._tweet_mongo_sink(),
                                  lambda: DataProvider(),
                                  job_name,
                                  self.jars,
                                  config)
        return self._configure(stream)

    def _configure(self, streaming_env: Stream) -> Stream:
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

        return streaming_env

    def _load_jars(self) -> list[str]:
        if self.env == 'local':
            return self.config['jars']['jars_files'].split(",")
        return []

    def _tweet_http_source(self) -> ValueSource[str]:
        return HttpValueSource[str, str](
            host=self.config['web_socket']['host'],
            port=int(self.config['web_socket']['port']),
            source_name="http_tweets_source")

    def _tweet_kafka_sink(self) -> ValueSink[Row]:
        return KafkaValueSink[Row, Row](
            topic=self.config['kafka']['topic'],
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            serialization_schema=(JsonRowSerializationSchema
                                  .Builder()
                                  .with_type_info(_event_schema)
                                  .build()),
            event_translator=_socket_data_to_kafka_record,
            sink_name="kafka_tweets_sink")

    def _tweet_kafka_source(self) -> ValueSource[Row]:
        watermark_strategy = (WatermarkStrategy
                              .for_monotonous_timestamps()
                              .with_timestamp_assigner(TweetTimestampAssigner())
                              .with_idleness(Duration.of_seconds(5)))
        return KafkaValueSource[str, Row](
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            topic=self.config['kafka']['topic'],
            group=self.config['kafka']['group'],
            deserialization_schema=(JsonRowDeserializationSchema
                                    .Builder()
                                    .type_info(_event_schema)
                                    .build()),
            watermark_strategy=watermark_strategy,
            starting_offsets_initializer=KafkaOffsetsInitializer.earliest(),
            event_translator=_socket_kafka_record_to_row,
            source_name="kafka_tweets_source")

    def _tweet_mongo_sink(self) -> ValueSink[Row]:
        # go to mongodb container and do next:
        # 1 ./bin/mongosh
        # 2 use twitter_db
        # 3 db.createCollection("twitter_collection")
        # 4 db.twitter_collection.find()
        # 5 db.twitter_collection.drop()
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
        return MongoDbValueSink[Row, Row](
            port=int(self.config['mongo']['port']),
            host=self.config['mongo']['host'],
            database_name=self.config['mongo']['database_name'],
            collection_name=self.config['mongo']['collection_name'],
            table_name=self.config['mongo']['table_name'],
            flink_schema=schema,
            primary_keys=["hash"],
            event_translator=_aggregated_data_to_mongo_record,
            recreate_table=True,
            sink_name="mongo_tweets_sink")

    def _load_config(self, env: str):
        config = configparser.ConfigParser()
        base_path = os.path.dirname(os.path.abspath(__file__))
        config.read(os.path.join(base_path, '..', '..', 'config', 'base_config.ini'))
        if env == 'local':
            config.read(os.path.join(base_path, '..', '..', 'config', 'local', 'config.ini'))
        elif env == 'dev':
            config.read(os.path.join(base_path, '..', '..', 'config', 'dev', 'config.ini'))
        return config
