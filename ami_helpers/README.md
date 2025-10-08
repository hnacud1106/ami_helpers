# AMI Helpers - Thư viện hỗ trợ xử lý dữ liệu

## Tổng quan

AMI Helpers là một thư viện Python được thiết kế để hỗ trợ các tác vụ xử lý dữ liệu quy mô lớn với Apache Spark/Hive, kết nối database và hệ thống cảnh báo. Thư viện cung cấp các công cụ mạnh mẽ và dễ sử dụng cho việc:

- **Xử lý dữ liệu**: Spark/Hive engines với cấu hình linh hoạt
- **Kết nối database**: PostgreSQL và ClickHouse với connection pooling
- **Hệ thống cảnh báo**: Discord notifications với retry logic
- **Utilities**: Logging, retry mechanisms và error handling

## Cài đặt

### Yêu cầu hệ thống

- Python 3.8+
- Java 8+ (cho Apache Spark)
- Apache Spark 3.0+ (nếu sử dụng Spark engines)

### Cài đặt dependencies

```bash
# Clone repository
git clone <repository-url>
cd ami_helpers

# Tạo virtual environment
python -m venv ami_helper_venv

# Kích hoạt virtual environment
# Windows
ami_helper_venv\Scripts\activate
# Linux/Mac
source ami_helper_venv/bin/activate

# Cài đặt dependencies
pip install -r ami_helpers/requirements.txt
```

### Cấu hình môi trường

1. Copy file cấu hình mẫu:
```bash
cp ami_helpers/.env_sample ami_helpers/.env
```

2. Chỉnh sửa file `.env` với thông tin môi trường của bạn:
```env
# Spark Configuration
SPARK_APP_NAME=my_data_processing_app
SPARK_MASTER=spark://your-spark-master:7077
SPARK_CONF__spark.executor.memory=4G
SPARK_CONF__spark.executor.cores=4

# Hive Configuration
SPARK_CONF__hive.metastore.uris=thrift://your-hive-metastore:9083
SPARK_CONF__spark.sql.warehouse.dir=hdfs://your-namenode:9000/user/hive/warehouse

# Database Configuration (sẽ được cấu hình trong code)
# Discord Webhook (sẽ được cấu hình trong code)
```

## Cấu trúc thư viện

```
ami_helpers/
├── database_engines/          # Kết nối database (PostgreSQL, ClickHouse)
├── spark_hive_engines/        # Spark/Hive processing engines
├── alerts/                    # Hệ thống cảnh báo (Discord)
├── utils/                     # Utilities (logging, retry)
└── requirements.txt           # Dependencies
```

## Sử dụng nhanh

### 1. Xử lý dữ liệu với Spark

```python
from ami_helpers.spark_hive_engines import make_engine

# Tạo Spark engine
with make_engine(kind="spark", app_name="my_app") as engine:
    # Đọc dữ liệu từ bảng
    df = engine.read_table("my_database.my_table")
    
    # Thực hiện SQL query
    result = engine.sql("""
        SELECT 
            date_col,
            COUNT(*) as record_count
        FROM my_database.my_table 
        WHERE date_col >= '2024-01-01'
        GROUP BY date_col
    """)
    
    # Ghi kết quả
    engine.write_table(result, "my_database.summary_table", mode="overwrite")
```

### 2. Kết nối Database

```python
from ami_helpers.database_engines import create_connector, DBConfig

# Cấu hình PostgreSQL
pg_config = DBConfig(
    dsn="postgresql://user:password@localhost:5432/mydb",
    max_pool_size=10
)

# Tạo connector
pg_connector = create_connector(pg_config)

# Thực hiện query
results = pg_connector.fetchall("SELECT * FROM users WHERE active = %(active)s", {"active": True})

# Thực hiện update
pg_connector.execute("UPDATE users SET last_login = NOW() WHERE id = %(user_id)s", {"user_id": 123})
```

### 3. Hệ thống cảnh báo Discord

```python
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig, send_discord_alert

# Cấu hình Discord webhook
discord_config = DiscordAlertConfig(
    webhook_url="https://discord.com/api/webhooks/your-webhook-url",
    username="Data Pipeline Bot",
    environment="production"
)

# Gửi cảnh báo
send_discord_alert(
    discord_config,
    title="Pipeline Completed",
    description="Data processing job finished successfully",
    fields={
        "records_processed": "1,000,000",
        "duration": "15 minutes",
        "status": "SUCCESS"
    },
    level="info"
)
```

### 4. Retry với Discord alerts

```python
from ami_helpers.utils.retry import retriable_with_discord
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig

discord_config = DiscordAlertConfig(
    webhook_url="your-webhook-url",
    environment="production"
)

@retriable_with_discord(
    (ConnectionError, TimeoutError),
    discord_config,
    max_attempts=3,
    notify_every_retry=True
)
def risky_database_operation():
    # Code có thể gây lỗi
    pass
```

## Tài liệu chi tiết

- [Custom Parameters](docs/custom_parameters.md) - **Các tham số tùy chỉnh của thư viện** ⭐
- [Database Engines](docs/database_engines.md) - Hướng dẫn kết nối PostgreSQL và ClickHouse
- [Spark/Hive Engines](docs/spark_hive_engines.md) - Xử lý dữ liệu với Spark và Hive
- [Alert System](docs/alerts.md) - Hệ thống cảnh báo Discord
- [Utilities](docs/utils.md) - Logging và retry mechanisms
- [Examples](docs/examples.md) - Các ví dụ sử dụng thực tế
- [Troubleshooting](docs/troubleshooting.md) - Khắc phục sự cố thường gặp

## Hỗ trợ

Nếu gặp vấn đề hoặc cần hỗ trợ, vui lòng:

1. Kiểm tra [Troubleshooting Guide](docs/troubleshooting.md)
2. Tạo issue trên repository
3. Liên hệ team phát triển

## License

[Thêm thông tin license]
