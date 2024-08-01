from typing import Callable, Optional, Any

from pyflink.common import TypeInformation
from pyflink.datastream import StreamExecutionEnvironment, DataStream

from core.source import ValueSource


class HttpValueSource(ValueSource):
    def __init__(self,
                 host: str,
                 port: int,
                 source_name: str,
                 event_translator: Optional[Callable[[Any], Any]] = None,
                 output_type: Optional[TypeInformation] = None):
        self.host = host
        self.port = port
        self.source_name = source_name
        self.output_type = output_type
        self.event_translator = event_translator

    def read(self, env: 'StreamExecutionEnvironment', **kwargs) -> 'DataStream':
        ds = DataStream(env._j_stream_execution_environment.socketTextStream(self.host, self.port))
        if self.event_translator and self.output_type:
            ds = ds.map(lambda i: self.event_translator(i), output_type=self.output_type)
        return ds.uid(f"{self.source_name}-events")
