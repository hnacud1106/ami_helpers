from typing import Literal
from base_engine import BaseTransformEngine
from hive_engine import HiveConfig, HiveTransformEngine
from spark_engine import SparkConfig, SparkTransformEngine

def make_engine(kind: Literal["spark", "hive"], **kwargs) -> BaseTransformEngine:
    if kind == "spark":
        return SparkTransformEngine(SparkConfig(**kwargs))
    if kind == "hive":
        return HiveTransformEngine(HiveConfig(**kwargs))
    raise ValueError(f"Unsupported engine: {kind}")
