import abc
from typing import TypeVar, Generic

O = TypeVar('O')


class ExternalService(Generic[O], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get(self) -> O:
        pass
