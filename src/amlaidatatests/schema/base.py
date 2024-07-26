"""Base classes and enumerations for schema specification"""

import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import auto
from typing import List, Optional

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
    """Configuration object for tables in an aml ai schema"""

    name: str
    schema: Schema
    entity_keys: Optional[list[str]] = None
    optional: bool = False
    table_type: TableType = TableType.CLOSED_ENDED_ENTITY


@dataclass(kw_only=True)
class ResolvedTableConfig(TableConfig):
    """A TableConfig which also includes an ibis table
    object.

    This class inherits from TableConfig, but redundant fields
    which are part of the [ibis.Table] object are derived from
    table instead.
    """

    resolved_name: str = field(init=False)
    schema: Schema = field(init=False)
    table: Table

    def __post_init__(self):
        self.resolved_name = self.table.get_name()
        self.schema = self.table.schema()


class BaseSchemaConfiguration(ABC):
    """Root schema configuration. Schema versions should override this base
    class to implement new schema configurations"""

    @property
    @abstractmethod
    def TABLES(self) -> List[TableConfig]: ...

    def __table_dict(self) -> dict[str, TableConfig]:
        return {t.name: t for t in self.TABLES}

    def get_table_config(self, name: str) -> TableConfig:
        """Get the table config for a specified table name

        Args:
            name: Unqualified table name to retrieve

        Returns:
            TableConfig for the named table
        """
        return self.__table_dict()[name]

    def __getitem__(self, name: str):
        return self.get_table_config(name)
