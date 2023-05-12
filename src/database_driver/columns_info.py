from typing import Any
from dataclasses import dataclass


@dataclass
class ColumnsInfo:
    name: str
    ordinal_position: int
    default: Any
    is_nullable: bool
    data_type: str
