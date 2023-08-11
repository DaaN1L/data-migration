from abc import abstractmethod, ABC
from typing import Any

from .columns_info import ColumnsInfo


class DriverBase(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @abstractmethod
    def get_columns_info(self,
                         table: str,
                         schema: str,
                         include: list[str] | None = None,
                         exclude: list[str] | None = None) -> list[ColumnsInfo]:
        ...

    @abstractmethod
    def select(self,
               table: str,
               schema: str,
               columns: list[str],
               increment_key: list[str],
               from_: list[Any],
               to_: Any) -> list[Any]:
        ...

    @abstractmethod
    def create_table(self,
                     table: str,
                     schema: str,
                     pkey: str,
                     columns_description: list[ColumnsInfo]):
        ...

    @abstractmethod
    def insert(self,
               table: str,
               schema: str,
               columns_description: list[ColumnsInfo],
               values: list[tuple]):
        ...
