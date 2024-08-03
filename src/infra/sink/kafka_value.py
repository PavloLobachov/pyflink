import logging
import sys
from typing import Callable, Optional, TypeVar, Generic

from pyflink.common import TypeInformation, SerializationSchema
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema

from core.sink import ValueSink
from infra.util.flink.type_conversions import flink_schema_to_row

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

I = TypeVar('I')
O = TypeVar('O')


class KafkaValueSink(Generic[I, O], ValueSink[I]):
    def __init__(self,
                 topic: str,
                 bootstrap_servers: str,
                 serialization_schema: SerializationSchema,
                 sink_name: str,
                 exactly_once: bool = False,
                 transactional_id_prefix: Optional[str] = None,
                 flink_schema: Optional[dict[str, TypeInformation]] = None,
                 event_translator: Optional[Callable[[I], O]] = None):
        self.sink_name = sink_name
        record_serialization_schema = (KafkaRecordSerializationSchema.builder()
                                       .set_topic_selector(lambda _: topic)
                                       .set_value_serialization_schema(serialization_schema)
                                       .build())
        serializer_builder = (KafkaSink.builder()
                              .set_bootstrap_servers(bootstrap_servers)
                              .set_record_serializer(record_serialization_schema))
        if exactly_once and transactional_id_prefix:
            trx_timeout_ms = 1000 * 60 * 10
            self.kafka_producer = (serializer_builder
                                   .set_property("transaction.timeout.ms", str(trx_timeout_ms))
                                   .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                   .set_transactional_id_prefix(transactional_id_prefix)
                                   .build())
        else:
            self.kafka_producer = (serializer_builder
                                   .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                                   .build())
        self.flink_schema = flink_schema
        self.event_translator = event_translator
        logging.debug(f"bootstrap_servers:{bootstrap_servers}, sink_name:{sink_name}")

    def write(self, ds: 'DataStream', env: 'StreamExecutionEnvironment', **kwargs) -> None:
        if self.event_translator and self.flink_schema:
            ds = ds.map(lambda row: self.event_translator(row), output_type=flink_schema_to_row(self.flink_schema))
        (ds
         .uid(f"{self.sink_name}-events")
         .sink_to(self.kafka_producer))
