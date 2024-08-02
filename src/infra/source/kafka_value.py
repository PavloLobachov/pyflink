import logging
import sys
from typing import Callable, Optional, Generic, TypeVar

from pyflink.common import TypeInformation, DeserializationSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

from core.source import ValueSource
from infra.util.flink.type_conversions import flink_schema_to_row

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

I = TypeVar('I')
O = TypeVar('O')


class KafkaValueSource(Generic[I, O], ValueSource[O]):
    def __init__(self,
                 bootstrap_servers: str,
                 topic: str,
                 group: str,
                 starting_offsets_initializer: 'KafkaOffsetsInitializer',
                 deserialization_schema: DeserializationSchema,
                 source_name: str,
                 watermark_strategy: WatermarkStrategy,
                 flink_schema: Optional[dict[str, TypeInformation]] = None,
                 event_translator: Optional[Callable[[I], O]] = None):
        self.topic = topic
        self.group = group
        self.kafka_consumer = (KafkaSource.builder()
                               .set_bootstrap_servers(bootstrap_servers)
                               .set_topics(topic)
                               .set_group_id(group)
                               .set_starting_offsets(starting_offsets_initializer)
                               .set_value_only_deserializer(deserialization_schema)
                               .set_property("enable.auto.commit", "false")
                               .set_property("properties.commit.offsets.on.checkpoint", "true")
                               .build())
        self.source_name = source_name
        self.watermark_strategy = watermark_strategy
        self.flink_schema = flink_schema
        self.event_translator = event_translator
        logging.debug(f"bootstrap_servers:{bootstrap_servers}, "
                      f"topic:{topic}, "
                      f"group:{group}, "
                      f"source_name:{source_name}, "
                      f"flink_schema:{flink_schema}")

    def read(self, env: 'StreamExecutionEnvironment', **kwargs) -> 'DataStream':
        ds = env.from_source(source=self.kafka_consumer,
                             watermark_strategy=self.watermark_strategy,
                             source_name=f"{self.topic}-{self.group}-source")

        if self.event_translator and self.flink_schema:
            ds = ds.map(lambda row: self.event_translator(row), output_type=flink_schema_to_row(self.flink_schema))
        return (ds
                .uid(f"{self.source_name}-events")
                .assign_timestamps_and_watermarks(self.watermark_strategy))
