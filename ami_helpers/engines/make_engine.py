from __future__ import annotations
from typing import Literal, Dict, Any
import os
from dotenv import load_dotenv

from base_engine import BaseTransformEngine
from hive_engine import HiveConfig, HiveTransformEngine
from spark_engine import SparkConfig, SparkTransformEngine

load_dotenv()


def _kwargs_from_env(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(kwargs)
    env_conf: Dict[str, str] = {}
    for k, v in os.environ.items():
        if k.startswith("SPARK_CONF__"):
            key = k[len("SPARK_CONF__"):]
            env_conf[key] = v
    code_conf = dict(merged.get("conf", {}) or {})
    env_conf.update(code_conf)
    if env_conf:
        merged["conf"] = env_conf

    if "app_name" not in merged:
        env_app = os.getenv("SPARK_APP_NAME")
        if env_app:
            merged["app_name"] = env_app
    if "master" not in merged:
        env_master = os.getenv("SPARK_MASTER")
        if env_master:
            merged["master"] = env_master
    return merged


def make_engine(kind: Literal["spark", "hive"], **kwargs) -> BaseTransformEngine:
    params = _kwargs_from_env(kwargs)
    if kind == "spark":
        return SparkTransformEngine(SparkConfig(**params))
    if kind == "hive":
        return HiveTransformEngine(HiveConfig(**params))
    raise ValueError(f"Unsupported engine: {kind}")


