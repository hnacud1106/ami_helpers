# helpers/db_connector.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional, Generator
import contextlib
import logging

from tenacity import retry, wait_exponential_jitter, stop_after_delay, retry_if_exception_type

import psycopg
from psycopg_pool import ConnectionPool

import clickhouse_connect

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DBConfig:
    dsn: str
    max_pool_size: int = 10
    min_pool_size: int = 1
    connect_timeout_s: int = 5
    statement_timeout_ms: int = 30000
    ssl_require: bool = True
    ch_database: Optional[str] = None
    ch_user: Optional[str] = None
    ch_password: Optional[str] = None


class BaseConnector:
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        raise NotImplementedError

    def fetchall(self, sql: str, params: Optional[Dict[str, Any]] = None) -> list[tuple]:
        raise NotImplementedError

    def health_check(self) -> bool:
        raise NotImplementedError


class PostgresConnector(BaseConnector):
    def __init__(self, cfg: DBConfig):
        dsn = cfg.dsn
        sep = '&' if '?' in dsn else '?'
        if 'connect_timeout=' not in dsn:
            dsn += f'{sep}connect_timeout={cfg.connect_timeout_s}'
            sep = '&'
        if cfg.ssl_require and 'sslmode=' not in dsn:
            dsn += f'{sep}sslmode=require'
        self._pool = ConnectionPool(
            conninfo=dsn,
            min_size=cfg.min_pool_size,
            max_size=cfg.max_pool_size,
            timeout=cfg.connect_timeout_s,
        )
        self._statement_timeout_ms = cfg.statement_timeout_ms

    @contextlib.contextmanager
    def _connection(self):
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {self._statement_timeout_ms}")
            yield conn

    @retry(
        wait=wait_exponential_jitter(initial=0.2, max=3.0),
        stop=stop_after_delay(15),
        retry=retry_if_exception_type((psycopg.OperationalError, psycopg.errors.DeadlockDetected)),
        reraise=True,
    )
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params or {})

    @retry(
        wait=wait_exponential_jitter(initial=0.2, max=3.0),
        stop=stop_after_delay(15),
        retry=retry_if_exception_type((psycopg.OperationalError, psycopg.errors.DeadlockDetected)),
        reraise=True,
    )
    def fetchall(self, sql: str, params: Optional[Dict[str, Any]] = None) -> list[tuple]:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()

    def health_check(self) -> bool:
        try:
            rows = self.fetchall("SELECT 1")
            return bool(rows and rows[0][0] == 1)
        except Exception as e:
            logger.warning("Postgres health check failed: %s", e)
            return False


class ClickHouseConnector(BaseConnector):
    def __init__(self, cfg: DBConfig):
        self._clients: list[Any] = []
        self._max_size = cfg.max_pool_size
        for _ in range(max(cfg.min_pool_size, 1)):
            self._clients.append(self._make_client(cfg))
        self._cfg = cfg

    def _make_client(self, cfg: DBConfig):
        return clickhouse_connect.get_client(
            url=cfg.dsn,
            username=cfg.ch_user,
            password=cfg.ch_password,
            database=cfg.ch_database,
            connect_timeout=cfg.connect_timeout_s,
        )

    @contextlib.contextmanager
    def _client(self):
        if self._clients:
            cl = self._clients.pop()
        else:
            cl = self._make_client(self._cfg)
        try:
            yield cl
        finally:
            if len(self._clients) < self._max_size:
                self._clients.append(cl)

    @retry(wait=wait_exponential_jitter(initial=0.2, max=3.0), stop=stop_after_delay(15), reraise=True)
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        with self._client() as cl:
            if params:
                cl.command(sql, parameters=params)
            else:
                cl.command(sql)

    @retry(wait=wait_exponential_jitter(initial=0.2, max=3.0), stop=stop_after_delay(15), reraise=True)
    def fetchall(self, sql: str, params: Optional[Dict[str, Any]] = None) -> list[tuple]:
        with self._client() as cl:
            if params:
                result = cl.query(sql, parameters=params)
            else:
                result = cl.query(sql)
            return result.result_rows

    def health_check(self) -> bool:
        try:
            rows = self.fetchall("SELECT 1")
            return bool(rows and rows[0][0] == 1)
        except Exception as e:
            logger.warning("ClickHouse health check failed: %s", e)
            return False


def create_connector(cfg: DBConfig) -> BaseConnector:
    low = cfg.dsn.lower()
    if low.startswith("postgres://") or low.startswith("postgresql://") or low.startswith("psycopg://"):
        return PostgresConnector(cfg)
    if low.startswith("http://") or low.startswith("https://"):
        return ClickHouseConnector(cfg)
    raise ValueError(f"Unsupported DSN scheme: {cfg.dsn}")
