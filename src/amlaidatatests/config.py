from typing import List, Union
import dacite
from dataclasses import dataclass
import yaml
from omegaconf import OmegaConf
from enum import Enum, auto

class TestName(Enum):
    unique_combination_of_columns = auto()

@dataclass
class Test:
    name: str

@dataclass
class Table:
    name: str
    tests: List[dict[str, Union[dict, int]]]


@dataclass
class AMLAIConfig:
    version: int
    tables: List[Table]
    tests: List[dict[str, Union[dict, int]]]

if __name__ == "__main__":
    conf = OmegaConf.structured(AMLAIConfig)
    conf = OmegaConf.merge(conf, OmegaConf.load("models/schema.yml"))