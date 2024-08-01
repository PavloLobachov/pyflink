import logging
import sys
from typing import Callable, Optional, Any, TypeVar, Generic

from pyflink.common.typeinfo import TypeInformation
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

from core.sink import ValueSink
from infra.util.flink.type_conversions import type_info_to_sql, flink_schema_to_row
from infra.util.name_generator import generate_random_name

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

I = TypeVar('I')
O = TypeVar('O')


class MongoDbValueSink(Generic[I, O], ValueSink[O]):
    def __init__(self,
                 host: str,
                 port: int,
                 database_name: str,
                 collection_name: str,
                 table_name: str,
                 sink_name: str,
                 flink_schema: dict[str, TypeInformation],
                 primary_keys: Optional[list[str]] = None,
                 recreate_table: bool = False,
                 event_translator: Optional[Callable[[I], O]] = None):
        self.host = host
        self.port = port
        self.database_name = database_name
        self.collection_name = collection_name
        self.table_name = table_name
        self.sink_name = sink_name
        self.flink_schema = flink_schema
        self.primary_keys = primary_keys
        self.recreate_table = recreate_table
        self.event_translator = event_translator
        self.temp_view_name = generate_random_name()

    def write(self, ds: 'DataStream', env: 'StreamExecutionEnvironment', **kwargs) -> None:
        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        table_env = StreamTableEnvironment.create(env, environment_settings=settings)

        if self.event_translator:
            ds = ds.map(lambda row: self.event_translator(row), output_type=flink_schema_to_row(self.flink_schema))

        if self.recreate_table:
            table_env.execute_sql(f"DROP TABLE IF EXISTS {self.table_name}")

        ddl = self._to_ddl()
        logging.info(f"Executing: {ddl}")
        print(f"Executing: {ddl}")
        table_env.execute_sql(ddl)

        ds = ds.uid(f"{self.sink_name}-events")
        ds.print()

        table_env.create_temporary_view(self.temp_view_name, ds)
        # table_env.from_data_stream()

        (table_env
         .from_path(self.temp_view_name)
         .execute_insert(self.table_name))

    def _to_ddl(self) -> str:
        sql_types = [f"`{col_name}` {type_info_to_sql(col_type)}" for col_name, col_type in self.flink_schema.items()]

        columns = ",\n".join(sql_types)
        if self.primary_keys:
            pk = ",".join(self.primary_keys)
            columns = f"{columns},\n  PRIMARY KEY ({pk}) NOT ENFORCED"

        ddl = (
                f"CREATE TABLE IF NOT EXISTS {self.table_name} (" +
                f"  {columns}" +
                "  ) WITH (" +
                "  'connector' = 'mongodb'," +
                f"  'uri' = 'mongodb://{self.host}:{self.port}'," +
                f"  'database' = '{self.database_name}'," +
                f"  'collection' = '{self.collection_name}'" +
                ");"
        )
        return ddl
