import abc
from typing import TypeVar, Generic
from pyflink.datastream import KeyedStream, DataStream, StreamExecutionEnvironment

OK = TypeVar('OK')
OV = TypeVar('OV')


class ValueSource(Generic[OV], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def read(self, env: 'StreamExecutionEnvironment', **kwargs) -> 'DataStream':
        pass


class KeyValueSource(Generic[OK, OV], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def read(self, env: 'StreamExecutionEnvironment', **kwargs) -> 'KeyedStream':
        pass
