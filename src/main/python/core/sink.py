import abc
from typing import TypeVar, Generic

from pyflink.datastream import DataStream, KeyedStream, StreamExecutionEnvironment

OK = TypeVar('OK')
OV = TypeVar('OV')


class ValueSink(Generic[OV], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def write(self, ds: 'DataStream', env: 'StreamExecutionEnvironment', **kwargs) -> None:
        pass


class KeyValueSink(Generic[OK, OV], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def write(self, ds: 'KeyedStream', env: 'StreamExecutionEnvironment', **kwargs) -> None:
        pass
