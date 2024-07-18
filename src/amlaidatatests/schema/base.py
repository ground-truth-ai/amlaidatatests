from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional
import enum
from enum import auto

from ibis import Schema, Table


class TableType(enum.Enum):
    OPEN_ENDED_ENTITY = auto()
    """ An entity whose existence is generally never closed, e.g. a transaction
    """
    CLOSED_ENDED_ENTITY = auto()
    """ An entity whose existence is closed, e.g. a party or link which has a
    well defined end date """
    EVENT = auto()
    """ An immutable event whose existence is closed """


@dataclass
class TableConfig:
    name: str
    schema: Schema
    entity_keys: Optional[list[str]] = None
    optional: bool = False
    table_type: TableType = TableType.CLOSED_ENDED_ENTITY


@dataclass(kw_only=True)
class ResolvedTableConfig(TableConfig):
    name: str = field(init=False)
    schema: str = field(init=False)
    table: Table

    def __post_init__(self):
        self.name = self.table.get_name()
        self.schema = self.table.schema()


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
