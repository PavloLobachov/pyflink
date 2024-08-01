import abc

from pyflink.datastream import DataStream, StreamExecutionEnvironment

from core.sink import ValueSink
from core.source import ValueSource


class Stream(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def __init__(self, env: 'StreamExecutionEnvironment'):
        self.env = env

    def read(self, source: ValueSource, **kwargs) -> 'DataStream':
        return source.read(env=self.env, **kwargs)

    def write(self, sink: ValueSink, ds: 'DataStream', **kwargs) -> None:
        sink.write(ds=ds, env=self.env, **kwargs)

    @abc.abstractmethod
    def process(self) -> None:
        pass
