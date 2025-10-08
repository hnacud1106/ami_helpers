from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, Optional, Iterable, Any, Mapping
from pyspark.sql import SparkSession, DataFrame
from base_engine import BaseTransformEngine


@dataclass(frozen=True)
class TransformConfig:
    app_name: str = "app_name"
    master: Optional[str] = None
    enable_hive: bool = False
    stop_on_exit: bool = True
    suppress_stop_errors: bool = True
    conf: Dict[str, str] = field(default_factory=dict)


class TransformEngine(BaseTransformEngine):
    def __init__(self, cfg: TransformConfig = TransformConfig()):
        self._cfg = cfg
        b = SparkSession.builder.appName(cfg.app_name)
        if cfg.master:
            b = b.master(cfg.master)
        if cfg.enable_hive:
            b = b.enableHiveSupport()
        for k, v in (cfg.conf or {}).items():
            b = b.config(k, v)
        self._spark = b.getOrCreate()
        self._spark.sparkContext.setLogLevel("ERROR")

    def __enter__(self) -> "TransformEngine":
        return self

    def __exit__(self, exc_type, exc, tb):
        if not self._cfg.stop_on_exit:
            return False
        try:
            if not getattr(self, "_stopped", False):
                self._spark.stop()
                self._stopped = True
        except Exception:
            if not self._cfg.suppress_stop_errors:
                raise
        return False

    @property
    def spark(self) -> SparkSession:
        return self._spark

    @contextmanager
    def temporary_conf(self, pairs: Mapping[str, str]):
        old: Dict[str, Optional[str]] = {}
        for k, v in pairs.items():
            old[k] = self._spark.conf.get(k, None)
            self._spark.conf.set(k, v)
        try:
            yield
        finally:
            for k, prev in old.items():
                if prev is None:
                    self._spark.conf.unset(k)
                else:
                    self._spark.conf.set(k, prev)

    def sql(self, query: str, args: Optional[Any] = None) -> DataFrame:
        if not args:
            return self._spark.sql(query)
        return self._spark.sql(query, args)

    def read_table(self, table: str) -> DataFrame:
        return self._spark.table(table)

    def write_table(
            self,
            df: DataFrame,
            table: str,
            mode: str = "append",
            options: Optional[Dict[str, str]] = None,
            partition_by: Optional[Iterable[str]] = None,
    ) -> None:
        mode_allowed = {"append", "overwrite", "ignore", "error", "errorifexists"}
        if mode.lower() not in mode_allowed:
            raise ValueError(f"Unsupported mode: {mode}. Allowed: {mode_allowed}")
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if options:
            for k, v in options.items():
                writer = writer.option(k, v)
        writer.saveAsTable(table)
