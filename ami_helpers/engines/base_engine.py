from __future__ import annotations
from typing import Protocol, Any


class BaseTransformEngine(Protocol):
    def sql(self, query: str) -> Any:
        pass

    def read_table(self, table: str) -> Any:
        pass

    def write_table(self, df: Any, table: str, mode: str = "append") -> None:
        pass
