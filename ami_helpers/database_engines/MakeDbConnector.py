from ami_helpers.database_engines.db_connectors.db_connector import DBConfig, BaseConnector, PostgresConnector, ClickHouseConnector
from dotenv import load_dotenv

load_dotenv()




def create_connector(cfg: DBConfig) -> BaseConnector:
    low = cfg.dsn.lower()
    if low.startswith("postgres://") or low.startswith("postgresql://") or low.startswith("psycopg://"):
        return PostgresConnector(cfg)
    if low.startswith("http://") or low.startswith("https://"):
        return ClickHouseConnector(cfg)
    raise ValueError(f"Unsupported DSN scheme: {cfg.dsn}")
