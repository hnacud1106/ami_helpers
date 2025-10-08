# Database Engines - Hướng dẫn kết nối Database

## Tổng quan

Module `database_engines` cung cấp các connector để kết nối và làm việc với các loại database khác nhau. Hiện tại hỗ trợ:

- **PostgreSQL**: Sử dụng psycopg3 với connection pooling
- **ClickHouse**: Sử dụng clickhouse-connect với client pooling

## Cài đặt và cấu hình

### Dependencies

Các dependencies cần thiết đã được cài đặt trong `requirements.txt`:

```
psycopg==3.2.10
psycopg-pool==3.2.6
clickhouse-connect==0.9.2
```

### Cấu hình Database

#### DBConfig Parameters - Chi tiết từng tham số

```python
from ami_helpers.database_engines import create_connector, DBConfig

# Cấu hình PostgreSQL với tất cả tham số
pg_config = DBConfig(
    # === THAM SỐ BẮT BUỘC ===
    dsn="postgresql://username:password@host:port/database",
    
    # === CONNECTION POOLING ===
    max_pool_size=10,        # Số connection tối đa trong pool
    min_pool_size=1,         # Số connection tối thiểu trong pool
    
    # === TIMEOUT SETTINGS ===
    connect_timeout_s=5,      # Timeout kết nối (giây)
    statement_timeout_ms=30000, # Timeout thực thi query (ms)
    
    # === SECURITY ===
    ssl_require=True,         # Yêu cầu SSL connection
    
    # === CLICKHOUSE SPECIFIC ===
    ch_database="default",    # Database name (chỉ cho ClickHouse)
    ch_user="default",        # Username (chỉ cho ClickHouse)
    ch_password=""           # Password (chỉ cho ClickHouse)
)
```

#### Chi tiết từng tham số

##### 1. **dsn** (Bắt buộc)
```python
# PostgreSQL DSN formats
dsn="postgresql://user:pass@localhost:5432/mydb"
dsn="postgresql://user:pass@localhost:5432/mydb?sslmode=require"
dsn="postgresql://user:pass@localhost:5432/mydb?sslmode=require&connect_timeout=10"

# ClickHouse DSN formats
dsn="http://clickhouse-host:8123"
dsn="https://clickhouse-host:8443"
dsn="http://clickhouse-host:8123?secure=1"
```

##### 2. **max_pool_size** (Mặc định: 10)
```python
# Cho workload nhẹ
max_pool_size=5

# Cho workload trung bình
max_pool_size=10

# Cho workload nặng
max_pool_size=20

# Cho high-throughput applications
max_pool_size=50
```

**Mục đích**: Kiểm soát số lượng connections đồng thời để tránh quá tải database server.

**Cách chọn giá trị**:
- **Workload nhẹ** (< 100 requests/phút): 5-10
- **Workload trung bình** (100-1000 requests/phút): 10-20
- **Workload nặng** (> 1000 requests/phút): 20-50

##### 3. **min_pool_size** (Mặc định: 1)
```python
# Giữ sẵn connections để giảm latency
min_pool_size=2

# Cho applications cần response time nhanh
min_pool_size=5

# Cho batch processing
min_pool_size=1
```

**Mục đích**: Giữ sẵn số connections tối thiểu để giảm latency khi có request mới.

**Cách chọn giá trị**:
- **Real-time applications**: 2-5 (giảm latency)
- **Batch processing**: 1 (tiết kiệm resources)
- **Mixed workload**: 2-3

##### 4. **connect_timeout_s** (Mặc định: 5)
```python
# Cho local database
connect_timeout_s=2

# Cho database trong cùng network
connect_timeout_s=5

# Cho database remote qua internet
connect_timeout_s=10

# Cho database có network không ổn định
connect_timeout_s=30
```

**Mục đích**: Thời gian chờ tối đa để thiết lập connection.

**Cách chọn giá trị**:
- **Local database**: 2-3 giây
- **Same network**: 5 giây
- **Remote database**: 10-15 giây
- **Unstable network**: 20-30 giây

##### 5. **statement_timeout_ms** (Mặc định: 30000)
```python
# Cho queries nhanh
statement_timeout_ms=5000

# Cho queries trung bình
statement_timeout_ms=30000

# Cho queries phức tạp
statement_timeout_ms=120000

# Cho batch operations
statement_timeout_ms=300000
```

**Mục đích**: Thời gian chờ tối đa để thực thi một query.

**Cách chọn giá trị**:
- **Simple queries**: 5-10 giây
- **Complex queries**: 30-60 giây
- **Batch operations**: 2-5 phút
- **Data migration**: 5-10 phút

##### 6. **ssl_require** (Mặc định: True)
```python
# Production environment
ssl_require=True

# Development environment
ssl_require=False

# Override trong DSN
dsn="postgresql://user:pass@host:port/db?sslmode=require"
```

**Mục đích**: Bắt buộc sử dụng SSL connection để bảo mật.

**Cách chọn**:
- **Production**: Luôn True
- **Development**: Có thể False nếu database không hỗ trợ SSL
- **Sensitive data**: Luôn True

##### 7. **ClickHouse Specific Parameters**

```python
# ClickHouse configuration
ch_config = DBConfig(
    dsn="http://clickhouse-host:8123",
    ch_database="analytics",     # Database name
    ch_user="analytics_user",    # Username
    ch_password="secure_password", # Password
    max_pool_size=15,            # ClickHouse thường cần nhiều connections hơn
    min_pool_size=3
)
```

**ch_database**: Tên database ClickHouse để kết nối
**ch_user**: Username để authenticate
**ch_password**: Password để authenticate

#### Ví dụ cấu hình theo use case

##### 1. **Web Application** (High concurrency, low latency)
```python
web_config = DBConfig(
    dsn="postgresql://user:pass@localhost:5432/webapp",
    max_pool_size=20,        # Nhiều connections cho concurrent users
    min_pool_size=5,         # Giữ sẵn connections
    connect_timeout_s=3,      # Timeout nhanh
    statement_timeout_ms=10000, # Queries phải nhanh
    ssl_require=True
)
```

##### 2. **Batch Processing** (Low concurrency, high throughput)
```python
batch_config = DBConfig(
    dsn="postgresql://user:pass@localhost:5432/warehouse",
    max_pool_size=5,         # Ít connections
    min_pool_size=1,         # Không cần giữ sẵn
    connect_timeout_s=10,    # Timeout dài hơn
    statement_timeout_ms=300000, # Queries có thể chậm
    ssl_require=True
)
```

##### 3. **Analytics Database** (ClickHouse)
```python
analytics_config = DBConfig(
    dsn="http://clickhouse-host:8123",
    ch_database="analytics",
    ch_user="analytics_user",
    ch_password="password",
    max_pool_size=15,        # ClickHouse cần nhiều connections
    min_pool_size=3,         # Giữ sẵn cho queries
    connect_timeout_s=5,
    statement_timeout_ms=120000, # Analytics queries có thể chậm
    ssl_require=False        # ClickHouse thường không dùng SSL
)
```

##### 4. **Development Environment**
```python
dev_config = DBConfig(
    dsn="postgresql://user:pass@localhost:5432/devdb",
    max_pool_size=3,         # Ít connections cho dev
    min_pool_size=1,
    connect_timeout_s=5,
    statement_timeout_ms=30000,
    ssl_require=False        # Dev thường không cần SSL
)
```

## Sử dụng cơ bản

### Thực hiện Query

#### PostgreSQL

```python
# SELECT với parameters
results = pg_connector.fetchall(
    "SELECT * FROM users WHERE age > %(min_age)s AND city = %(city)s",
    {"min_age": 18, "city": "Hanoi"}
)

# INSERT/UPDATE/DELETE
pg_connector.execute(
    "INSERT INTO users (name, email, age) VALUES (%(name)s, %(email)s, %(age)s)",
    {"name": "John Doe", "email": "john@example.com", "age": 25}
)

# Batch operations
users_data = [
    {"name": "Alice", "email": "alice@example.com", "age": 30},
    {"name": "Bob", "email": "bob@example.com", "age": 28}
]

for user in users_data:
    pg_connector.execute(
        "INSERT INTO users (name, email, age) VALUES (%(name)s, %(email)s, %(age)s)",
        user
    )
```

#### ClickHouse

```python
# SELECT query
results = ch_connector.fetchall(
    "SELECT * FROM events WHERE date >= %(start_date)s",
    {"start_date": "2024-01-01"}
)

# INSERT data
ch_connector.execute(
    "INSERT INTO events (user_id, event_type, timestamp) VALUES",
    {"user_id": 123, "event_type": "click", "timestamp": "2024-01-01 10:00:00"}
)

# Bulk insert
ch_connector.execute("""
    INSERT INTO events (user_id, event_type, timestamp) VALUES
    (1, 'click', '2024-01-01 10:00:00'),
    (2, 'view', '2024-01-01 10:01:00'),
    (3, 'purchase', '2024-01-01 10:02:00')
""")
```

### Health Check

```python
# Kiểm tra kết nối database
if pg_connector.health_check():
    print("PostgreSQL connection is healthy")
else:
    print("PostgreSQL connection failed")

if ch_connector.health_check():
    print("ClickHouse connection is healthy")
else:
    print("ClickHouse connection failed")
```

## Xử lý lỗi và Retry

### Retry tự động

Các connector đã được tích hợp sẵn retry logic:

```python
# PostgreSQL connector tự động retry khi gặp:
# - psycopg.OperationalError (mất kết nối, timeout)
# - psycopg.errors.DeadlockDetected (deadlock)

# ClickHouse connector tự động retry cho tất cả exceptions

# Cấu hình retry:
# - initial_wait: 0.2 giây
# - max_wait: 3.0 giây  
# - max_delay: 15 giây
```

### Xử lý lỗi thủ công

```python
import logging

logger = logging.getLogger(__name__)

try:
    results = pg_connector.fetchall("SELECT * FROM users")
    logger.info(f"Retrieved {len(results)} users")
except Exception as e:
    logger.error(f"Database query failed: {e}")
    # Xử lý lỗi hoặc fallback logic
```

## Best Practices

### 1. Connection Pool Management

```python
# Sử dụng connection pool hiệu quả
config = DBConfig(
    dsn="postgresql://user:pass@host:port/db",
    max_pool_size=20,    # Điều chỉnh theo tải
    min_pool_size=2,     # Giữ sẵn connections
    connect_timeout_s=10 # Timeout phù hợp
)
```

### 2. Query Optimization

```python
# Sử dụng prepared statements với parameters
# Tốt
results = connector.fetchall(
    "SELECT * FROM users WHERE id = %(user_id)s",
    {"user_id": 123}
)

# Tránh string concatenation
# Không tốt
results = connector.fetchall(f"SELECT * FROM users WHERE id = {user_id}")
```

### 3. Transaction Management

```python
# PostgreSQL - Sử dụng transaction
connector.execute("BEGIN")
try:
    connector.execute("INSERT INTO users (...) VALUES (...)")
    connector.execute("INSERT INTO user_profiles (...) VALUES (...)")
    connector.execute("COMMIT")
except Exception as e:
    connector.execute("ROLLBACK")
    raise e
```

### 4. Monitoring và Logging

```python
import logging
from ami_helpers.utils.log_info import setup_logging

# Setup logging
setup_logging(level="INFO", logfile="database_operations.log")
logger = logging.getLogger(__name__)

# Log database operations
logger.info("Starting database operation")
start_time = time.time()

results = connector.fetchall("SELECT COUNT(*) FROM users")

duration = time.time() - start_time
logger.info(f"Query completed in {duration:.2f}s, returned {len(results)} rows")
```

## Troubleshooting

### Lỗi thường gặp

#### 1. Connection timeout

```
psycopg.OperationalError: connection timeout
```

**Giải pháp:**
- Tăng `connect_timeout_s` trong DBConfig
- Kiểm tra network connectivity
- Kiểm tra firewall settings

#### 2. Pool exhausted

```
psycopg_pool.PoolTimeout: connection pool exhausted
```

**Giải pháp:**
- Tăng `max_pool_size`
- Kiểm tra connection leaks
- Sử dụng context managers

#### 3. ClickHouse connection refused

```
clickhouse_connect.errors.ServerError: Connection refused
```

**Giải pháp:**
- Kiểm tra ClickHouse server status
- Kiểm tra port và hostname
- Kiểm tra authentication credentials

### Debug Mode

```python
import logging

# Bật debug logging
logging.basicConfig(level=logging.DEBUG)

# Hoặc sử dụng setup_logging
from ami_helpers.utils.log_info import setup_logging
setup_logging(level="DEBUG")
```

## Ví dụ thực tế

### ETL Pipeline với Database

```python
from ami_helpers.database_engines import create_connector, DBConfig
from ami_helpers.utils.log_info import setup_logging
import logging

# Setup logging
setup_logging(level="INFO")
logger = logging.getLogger(__name__)

def etl_pipeline():
    # Source database (PostgreSQL)
    source_config = DBConfig(
        dsn="postgresql://user:pass@source-host:5432/source_db",
        max_pool_size=5
    )
    source_connector = create_connector(source_config)
    
    # Target database (ClickHouse)
    target_config = DBConfig(
        dsn="http://clickhouse-host:8123",
        ch_database="analytics",
        ch_user="analytics_user",
        ch_password="password",
        max_pool_size=10
    )
    target_connector = create_connector(target_config)
    
    try:
        # Extract data from source
        logger.info("Extracting data from source database")
        source_data = source_connector.fetchall("""
            SELECT user_id, event_type, timestamp, amount
            FROM events 
            WHERE timestamp >= %(start_date)s
        """, {"start_date": "2024-01-01"})
        
        logger.info(f"Extracted {len(source_data)} records")
        
        # Transform and load to target
        logger.info("Loading data to target database")
        for row in source_data:
            target_connector.execute("""
                INSERT INTO events (user_id, event_type, timestamp, amount)
                VALUES (%(user_id)s, %(event_type)s, %(timestamp)s, %(amount)s)
            """, {
                "user_id": row[0],
                "event_type": row[1], 
                "timestamp": row[2],
                "amount": row[3]
            })
        
        logger.info("ETL pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise
    finally:
        # Cleanup connections
        pass

if __name__ == "__main__":
    etl_pipeline()
```
