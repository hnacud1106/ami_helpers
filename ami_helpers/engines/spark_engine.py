from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame

@dataclass(frozen=True)
class SparkConfig:
    app_name: str = "pipeline-spark"
    master: Optional[str] = None
    shuffle_partitions: int = 200
    broadcast_threshold: int = 10 * 1024 * 1024
    conf: Dict[str, str] = field(default_factory=dict)

class SparkTransformEngine:
    def __init__(self, cfg: SparkConfig = SparkConfig()):
        b = SparkSession.builder.appName(cfg.app_name)
        if cfg.master:
            b = b.master(cfg.master)
        b = (
            b.config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
             .config("spark.sql.autoBroadcastJoinThreshold", str(cfg.broadcast_threshold))
        )
        for k, v in (cfg.conf or {}).items():
            b = b.config(k, v)
        self._spark = b.getOrCreate()

    @property
    def spark(self) -> SparkSession:
        return self._spark

    def sql(self, query: str) -> DataFrame:
        return self._spark.sql(query)

    def read_table(self, table: str) -> DataFrame:
        return self._spark.table(table)

    def write_table(self, df: DataFrame, table: str, mode: str = "append") -> None:
        df.write.mode(mode).saveAsTable(table)
