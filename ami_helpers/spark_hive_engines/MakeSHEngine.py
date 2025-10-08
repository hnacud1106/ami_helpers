from __future__ import annotations
from typing import Literal, Dict, Any, Optional
import os
from dotenv import load_dotenv

from ami_helpers.spark_hive_engines.spark_hive_engines.transform_engine import TransformConfig, TransformEngine  # unified engine

load_dotenv()

def _parse_bool(v: Optional[str]) -> Optional[bool]:
    if v is None:
        return None
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def _kwargs_from_env(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(kwargs)
    env_conf: Dict[str, str] = {}
    for k, v in os.environ.items():
        if k.startswith("SPARK_CONF__"):
            key = k[len("SPARK_CONF__"):]
            env_conf[key] = v
    code_conf = dict((merged.get("conf") or {}))
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

    if "enable_hive" not in merged:
        env_hive = _parse_bool(os.getenv("SPARK_ENABLE_HIVE"))
        if env_hive is not None:
            merged["enable_hive"] = env_hive

    if "stop_on_exit" not in merged:
        env_stop = _parse_bool(os.getenv("SPARK_STOP_ON_EXIT"))
        if env_stop is not None:
            merged["stop_on_exit"] = env_stop
    if "suppress_stop_errors" not in merged:
        env_sup = _parse_bool(os.getenv("SPARK_SUPPRESS_STOP_ERRORS"))
        if env_sup is not None:
            merged["suppress_stop_errors"] = env_sup

    return merged

def make_engine(kind: Literal["spark", "hive"] = "spark", **kwargs) -> TransformEngine:
    params = _kwargs_from_env(kwargs)

    if "enable_hive" not in params:
        params["enable_hive"] = (kind == "hive")

    cfg = TransformConfig(
        app_name=params.get("app_name", "app_name"),
        master=params.get("master"),
        enable_hive=bool(params.get("enable_hive", False)),
        conf=params.get("conf", {}) or {},
        stop_on_exit=bool(params.get("stop_on_exit", True)),
        suppress_stop_errors=bool(params.get("suppress_stop_errors", True)),
    )
    return TransformEngine(cfg)

if __name__ == "__main__":
    with make_engine(
            kind="hive",
            conf={
                "spark.sql.shuffle.partitions": "512",  # override ENV
            },
    ) as eng:
        # Tuning cục bộ trong 1 khối
        with eng.temporary_conf({
            "spark.sql.autoBroadcastJoinThreshold": str(128 * 1024 * 1024),
        }):
            df = eng.sql("select f.* from fact f join dim d on f.k = d.k")
        eng.write_table(df, "dwh.sales", mode="overwrite")