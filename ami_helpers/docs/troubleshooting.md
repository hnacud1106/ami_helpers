# Troubleshooting - Khắc phục sự cố

## Tổng quan

Tài liệu này cung cấp hướng dẫn khắc phục các sự cố thường gặp khi sử dụng thư viện AMI Helpers. Các vấn đề được phân loại theo module và mức độ nghiêm trọng.

## Database Engines

### Lỗi kết nối PostgreSQL

#### 1. Connection timeout

**Triệu chứng:**
```
psycopg.OperationalError: connection timeout
```

**Nguyên nhân:**
- Database server không phản hồi
- Network connectivity issues
- Firewall blocking connections
- Database server quá tải

**Giải pháp:**
```python
# Tăng timeout
config = DBConfig(
    dsn="postgresql://user:pass@host:port/db",
    connect_timeout_s=30,  # Tăng từ 5s lên 30s
    max_pool_size=5        # Giảm pool size
)

# Kiểm tra kết nối
import psycopg
try:
    conn = psycopg.connect("postgresql://user:pass@host:port/db")
    print("Connection successful")
except Exception as e:
    print(f"Connection failed: {e}")
```

#### 2. Pool exhausted

**Triệu chứng:**
```
psycopg_pool.PoolTimeout: connection pool exhausted
```

**Nguyên nhân:**
- Quá nhiều connections đồng thời
- Connections không được đóng đúng cách
- Pool size quá nhỏ

**Giải pháp:**
```python
# Tăng pool size
config = DBConfig(
    dsn="postgresql://user:pass@host:port/db",
    max_pool_size=20,  # Tăng từ 10 lên 20
    min_pool_size=5    # Tăng min pool size
)

# Sử dụng context manager
with connector._connection() as conn:
    # Sử dụng connection
    pass
# Connection sẽ được tự động đóng
```

#### 3. SSL connection failed

**Triệu chứng:**
```
psycopg.OperationalError: SSL connection failed
```

**Giải pháp:**
```python
# Tắt SSL requirement
config = DBConfig(
    dsn="postgresql://user:pass@host:port/db",
    ssl_require=False  # Tắt SSL requirement
)

# Hoặc cấu hình SSL trong DSN
dsn = "postgresql://user:pass@host:port/db?sslmode=require"
```

### Lỗi kết nối ClickHouse

#### 1. Connection refused

**Triệu chứng:**
```
clickhouse_connect.errors.ServerError: Connection refused
```

**Giải pháp:**
```python
# Kiểm tra ClickHouse server
import requests
try:
    response = requests.get("http://clickhouse-host:8123/ping")
    print(f"ClickHouse status: {response.status_code}")
except Exception as e:
    print(f"ClickHouse unreachable: {e}")

# Kiểm tra credentials
config = DBConfig(
    dsn="http://clickhouse-host:8123",
    ch_user="default",
    ch_password="",
    ch_database="default"
)
```

#### 2. Authentication failed

**Triệu chứng:**
```
clickhouse_connect.errors.ServerError: Authentication failed
```

**Giải pháp:**
```python
# Kiểm tra credentials
config = DBConfig(
    dsn="http://clickhouse-host:8123",
    ch_user="correct_username",
    ch_password="correct_password",
    ch_database="correct_database"
)
```

## Spark/Hive Engines

### Lỗi Spark startup

#### 1. Java heap space

**Triệu chứng:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Giải pháp:**
```python
# Tăng driver memory
with make_engine(
    kind="spark",
    conf={
        "spark.driver.memory": "4G",      # Tăng từ 2G lên 4G
        "spark.executor.memory": "4G",    # Tăng executor memory
        "spark.driver.maxResultSize": "2G" # Tăng result size
    }
) as engine:
    pass
```

#### 2. Connection refused to Spark master

**Triệu chứng:**
```
java.net.ConnectException: Connection refused
```

**Giải pháp:**
```python
# Kiểm tra Spark master
import requests
try:
    response = requests.get("http://spark-master:8080")
    print(f"Spark master status: {response.status_code}")
except Exception as e:
    print(f"Spark master unreachable: {e}")

# Sử dụng local mode
with make_engine(
    kind="spark",
    master="local[4]"  # Sử dụng local mode
) as engine:
    pass
```

#### 3. Hive metastore connection failed

**Triệu chứng:**
```
java.sql.SQLException: Could not connect to address
```

**Giải pháp:**
```python
# Kiểm tra Hive metastore
import socket
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('hive-metastore-host', 9083))
    if result == 0:
        print("Hive metastore is reachable")
    else:
        print("Hive metastore is not reachable")
    sock.close()
except Exception as e:
    print(f"Error checking Hive metastore: {e}")

# Sử dụng in-memory catalog
with make_engine(
    kind="spark",
    conf={
        "spark.sql.catalogImplementation": "in-memory"
    }
) as engine:
    pass
```

### Lỗi Spark SQL

#### 1. Table not found

**Triệu chứng:**
```
AnalysisException: Table or view not found
```

**Giải pháp:**
```python
# Kiểm tra available tables
with make_engine(kind="hive") as engine:
    tables = engine.spark.catalog.listTables()
    for table in tables:
        print(f"Table: {table.name}, Database: {table.database}")
    
    # Sử dụng fully qualified name
    df = engine.sql("SELECT * FROM database.table_name")
```

#### 2. Partition not found

**Triệu chứng:**
```
AnalysisException: Partition not found
```

**Giải pháp:**
```python
# Kiểm tra partitions
with make_engine(kind="hive") as engine:
    partitions = engine.spark.catalog.listPartitions("table_name")
    for partition in partitions:
        print(f"Partition: {partition}")
    
    # Sử dụng dynamic partition
    engine.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

## Alert System

### Lỗi Discord webhook

#### 1. Webhook not found

**Triệu chứng:**
```
requests.exceptions.HTTPError: 404 Client Error: Not Found
```

**Giải pháp:**
```python
# Kiểm tra webhook URL
import requests

webhook_url = "https://discord.com/api/webhooks/YOUR_WEBHOOK_URL"
try:
    response = requests.get(webhook_url)
    print(f"Webhook status: {response.status_code}")
except Exception as e:
    print(f"Webhook error: {e}")

# Tạo webhook mới
# 1. Vào Discord server settings
# 2. Integrations → Webhooks
# 3. Create webhook mới
```

#### 2. Rate limit exceeded

**Triệu chứng:**
```
requests.exceptions.HTTPError: 429 Too Many Requests
```

**Giải pháp:**
```python
# Implement rate limiting
import time
from collections import defaultdict

class RateLimitedAlertManager:
    def __init__(self, config, max_alerts_per_minute=10):
        self.config = config
        self.max_alerts = max_alerts_per_minute
        self.alert_times = defaultdict(list)
    
    def send_alert(self, title, description, fields, level="info"):
        current_time = time.time()
        minute_key = int(current_time // 60)
        
        # Clean old entries
        self.alert_times[minute_key] = [
            t for t in self.alert_times[minute_key] 
            if current_time - t < 60
        ]
        
        # Check rate limit
        if len(self.alert_times[minute_key]) >= self.max_alerts:
            print(f"Rate limit exceeded for minute {minute_key}")
            return False
        
        # Send alert
        success = send_discord_alert(
            self.config, title, description, fields, level
        )
        
        if success:
            self.alert_times[minute_key].append(current_time)
        
        return success
```

#### 3. Message too long

**Triệu chứng:**
```
requests.exceptions.HTTPError: 400 Bad Request
```

**Giải pháp:**
```python
# Truncate message content
def safe_send_alert(config, title, description, fields, level="info"):
    # Truncate long content
    max_length = 1800
    if len(description) > max_length:
        description = description[:max_length-3] + "..."
    
    # Truncate field values
    truncated_fields = {}
    for key, value in fields.items():
        if len(str(value)) > max_length:
            truncated_fields[key] = str(value)[:max_length-3] + "..."
        else:
            truncated_fields[key] = str(value)
    
    return send_discord_alert(
        config, title, description, truncated_fields, level
    )
```

## Utils

### Lỗi logging

#### 1. Logs không hiển thị

**Triệu chứng:**
- Không thấy log messages trong console hoặc file

**Giải pháp:**
```python
# Kiểm tra log level
import logging
logger = logging.getLogger(__name__)
print(f"Logger level: {logger.level}")
print(f"Root logger level: {logging.getLogger().level}")

# Bật debug logging
setup_logging(level="DEBUG", logfile="debug.log")

# Hoặc sử dụng basicConfig
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)
```

#### 2. Log file không được tạo

**Triệu chứng:**
- File log không được tạo hoặc không có quyền ghi

**Giải pháp:**
```python
# Kiểm tra quyền ghi file
import os
log_file = "application.log"
try:
    with open(log_file, "w") as f:
        f.write("Test log entry")
    print("Log file created successfully")
except Exception as e:
    print(f"Cannot create log file: {e}")

# Sử dụng absolute path
import os
log_file = os.path.abspath("application.log")
setup_logging(level="INFO", logfile=log_file)
```

### Lỗi retry

#### 1. Retry không hoạt động

**Triệu chứng:**
- Function không retry khi gặp lỗi

**Giải pháp:**
```python
# Kiểm tra exception types
try:
    operation()
except Exception as e:
    print(f"Exception type: {type(e)}")
    print(f"Exception: {e}")

# Đảm bảo exception được catch đúng
@retriable(
    (ValueError, ConnectionError),  # Chỉ catch specific exceptions
    max_attempts=3
)
def operation():
    raise TypeError("Wrong exception type")  # Sẽ không retry
```

#### 2. Retry quá nhiều lần

**Triệu chứng:**
- Function retry quá nhiều lần, gây chậm

**Giải pháp:**
```python
# Giới hạn số lần retry
@retriable(
    (ConnectionError,),
    max_attempts=3,      # Chỉ retry 3 lần
    max_seconds=30       # Hoặc giới hạn thời gian
)
def operation():
    pass
```

## Performance Issues

### Database Performance

#### 1. Query chậm

**Triệu chứng:**
- Database queries mất quá nhiều thời gian

**Giải pháp:**
```python
# Sử dụng connection pooling
config = DBConfig(
    dsn="postgresql://user:pass@host:port/db",
    max_pool_size=20,    # Tăng pool size
    min_pool_size=5,     # Giữ sẵn connections
    connect_timeout_s=10 # Timeout phù hợp
)

# Sử dụng prepared statements
connector.execute(
    "SELECT * FROM users WHERE id = %(user_id)s",
    {"user_id": 123}
)

# Batch operations
for batch in batches:
    connector.execute("INSERT INTO table VALUES (...)", batch)
```

#### 2. Memory usage cao

**Triệu chứng:**
- Ứng dụng sử dụng quá nhiều memory

**Giải pháp:**
```python
# Giảm batch size
batch_size = 1000  # Thay vì 10000

# Sử dụng generator thay vì list
def process_data():
    for record in data_generator():
        yield process_record(record)

# Cleanup connections
with connector._connection() as conn:
    # Sử dụng connection
    pass
# Connection sẽ được tự động đóng
```

### Spark Performance

#### 1. Spark job chậm

**Triệu chứng:**
- Spark jobs mất quá nhiều thời gian

**Giải pháp:**
```python
# Tối ưu Spark configuration
with make_engine(
    kind="spark",
    conf={
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.shuffle.partitions": "200"  # Điều chỉnh theo data size
    }
) as engine:
    pass

# Cache frequently used data
df = engine.read_table("large_table")
df.cache()
```

#### 2. Out of memory

**Triệu chứng:**
- Spark jobs bị out of memory

**Giải pháp:**
```python
# Tăng memory allocation
with make_engine(
    kind="spark",
    conf={
        "spark.driver.memory": "8G",
        "spark.executor.memory": "8G",
        "spark.driver.maxResultSize": "4G",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB"
    }
) as engine:
    pass

# Sử dụng repartition
df = df.repartition(200, "partition_column")
```

## Debugging Tips

### 1. Enable Debug Logging

```python
# Bật debug logging cho tất cả modules
import logging
logging.basicConfig(level=logging.DEBUG)

# Hoặc sử dụng setup_logging
from ami_helpers.utils.log_info import setup_logging
setup_logging(level="DEBUG", logfile="debug.log")
```

### 2. Monitor Resource Usage

```python
import psutil
import time

def monitor_resources():
    while True:
        cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        print(f"CPU: {cpu_percent}%, Memory: {memory.percent}%, Disk: {disk.percent}%")
        time.sleep(5)

# Chạy monitoring trong background
import threading
monitor_thread = threading.Thread(target=monitor_resources)
monitor_thread.daemon = True
monitor_thread.start()
```

### 3. Test Individual Components

```python
# Test database connection
def test_database_connection():
    try:
        connector = create_connector(config)
        result = connector.fetchall("SELECT 1")
        print("Database connection successful")
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False

# Test Spark engine
def test_spark_engine():
    try:
        with make_engine(kind="spark") as engine:
            df = engine.sql("SELECT 1 as test")
            result = df.collect()
            print("Spark engine successful")
            return True
    except Exception as e:
        print(f"Spark engine failed: {e}")
        return False

# Test Discord webhook
def test_discord_webhook():
    try:
        config = DiscordAlertConfig(webhook_url="your-webhook-url")
        result = send_discord_alert(
            config,
            "Test Alert",
            "Testing webhook connection",
            {"Status": "Test"},
            "info"
        )
        print(f"Discord webhook test: {result}")
        return result
    except Exception as e:
        print(f"Discord webhook failed: {e}")
        return False
```

## FAQ

### Q: Làm thế nào để tăng performance của database queries?

A: 
- Sử dụng connection pooling
- Tối ưu SQL queries với indexes
- Sử dụng prepared statements
- Batch operations thay vì single operations
- Tăng pool size phù hợp với workload

### Q: Spark job bị out of memory, làm thế nào?

A:
- Tăng driver và executor memory
- Giảm batch size hoặc tăng partitions
- Sử dụng repartition để phân bố data đều
- Enable adaptive query execution
- Sử dụng broadcast join cho small tables

### Q: Discord notifications không gửi được?

A:
- Kiểm tra webhook URL có đúng không
- Kiểm tra rate limits
- Truncate message content nếu quá dài
- Kiểm tra network connectivity
- Test webhook với simple message trước

### Q: Làm thế nào để debug retry mechanism?

A:
- Bật debug logging
- Kiểm tra exception types có match không
- Log retry attempts
- Sử dụng appropriate retry parameters
- Test với simple function trước

### Q: Làm thế nào để monitor system health?

A:
- Sử dụng SystemHealthMonitor class
- Setup Discord alerts cho critical metrics
- Monitor CPU, memory, disk usage
- Check database và web service health
- Setup automated health checks

## Liên hệ hỗ trợ

Nếu gặp vấn đề không được giải quyết trong tài liệu này:

1. Kiểm tra logs để tìm error details
2. Tạo issue trên repository với thông tin:
   - Error message đầy đủ
   - Steps to reproduce
   - Environment details
   - Log files (nếu có)
3. Liên hệ team phát triển qua Discord hoặc email

