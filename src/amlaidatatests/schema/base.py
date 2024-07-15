from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List

from ibis import Schema, Table


@dataclass
class TableConfig:
    name: str
    schema: Schema
    optional: bool = False
    is_open_ended_entity = True


@dataclass
class ResolvedTableConfig:
    name: str = field(init=False)
    table: Table
    optional: bool = False
    is_open_ended_entity: bool = True

    def __post_init__(self):
        self.name = self.table.get_name()


class BaseSchemaConfiguration(ABC):

    @property
    @abstractmethod
    def TABLES(self) -> List[TableConfig]: ...

    def __table_dict(self) -> dict[str, TableConfig]:
        return {t.name: t for t in self.TABLES}

    def get_table_config(self, n: str) -> TableConfig:
        return self.__table_dict()[n]

    def __getitem__(self, name: str):
        return self.get_table_config(name)
