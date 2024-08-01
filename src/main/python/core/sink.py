import abc

from pyflink.datastream import DataStream, KeyedStream, StreamExecutionEnvironment


class ValueSink(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def write(self, ds: 'DataStream', env: 'StreamExecutionEnvironment', **kwargs) -> None:
        pass


class KeyValueSink(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def write(self, ds: 'KeyedStream', env: 'StreamExecutionEnvironment', **kwargs) -> None:
        pass
