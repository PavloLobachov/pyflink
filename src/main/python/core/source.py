import abc
from pyflink.datastream import KeyedStream, DataStream, StreamExecutionEnvironment


class ValueSource(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def read(self, env: 'StreamExecutionEnvironment', **kwargs) -> 'DataStream':
        pass


class KeyValueSource(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def read(self, env: 'StreamExecutionEnvironment', **kwargs) -> 'KeyedStream':
        pass
