from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List
from ibis import Schema


@dataclass
class TableConfig:
    name: str
    schema: Schema
    optional: bool = False


class BaseSchemaConfiguration(ABC):

    @property
    @abstractmethod
    def TABLES(self) -> List[TableConfig]:
        pass

    def __table_dict(self) -> dict[str, TableConfig]:
        return {t.name: t for t in self.TABLES}

    def get_table_schema(self, n: str) -> TableConfig:
        return self.__table_dict()[n]
