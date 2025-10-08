# Custom Parameters - Các tham số tùy chỉnh của AMI Helpers

## Tổng quan

Tài liệu này tập trung vào các tham số **custom** mà thư viện AMI Helpers tự định nghĩa, không phải các tham số kế thừa từ Spark/PostgreSQL/Discord. Các tham số này được thiết kế để đơn giản hóa việc sử dụng và cung cấp các tính năng bổ sung.

## 1. Spark/Hive Engines

### make_engine() Parameters

#### **kind** (Mặc định: "spark")
```python
# Tạo Spark engine
with make_engine(kind="spark") as engine:
    pass

# Tạo Hive engine
with make_engine(kind="hive") as engine:
    pass
```

**Mục đích**: Chọn loại engine (Spark hoặc Hive)
**Cách chọn**:
- **"spark"**: Chỉ sử dụng Spark, không có Hive integration
- **"hive"**: Sử dụng Spark với Hive Metastore integration

#### **stop_on_exit** (Mặc định: True)
```python
# Tự động dừng SparkContext khi thoát
with make_engine(stop_on_exit=True) as engine:
    pass

# Giữ SparkContext chạy (cho interactive mode)
with make_engine(stop_on_exit=False) as engine:
    pass
```

**Mục đích**: Kiểm soát lifecycle của SparkContext
**Cách chọn**:
- **True**: Cho batch jobs, đảm bảo cleanup
- **False**: Cho interactive notebooks, giữ context chạy

#### **suppress_stop_errors** (Mặc định: True)
```python
# Ẩn lỗi khi dừng SparkContext
with make_engine(suppress_stop_errors=True) as engine:
    pass

# Hiển thị lỗi khi dừng SparkContext
with make_engine(suppress_stop_errors=False) as engine:
    pass
```

**Mục đích**: Ẩn/hiển thị lỗi khi dừng SparkContext
**Cách chọn**:
- **True**: Để tránh spam error messages
- **False**: Để debug các vấn đề khi dừng context

### TransformEngine Methods

#### **temporary_conf()** - Context Manager
```python
with engine.temporary_conf({
    "spark.sql.autoBroadcastJoinThreshold": "128MB",
    "spark.sql.adaptive.enabled": "false"
}):
    # Code chạy với config tạm thời
    df = engine.sql("SELECT * FROM large_table")
# Config tự động được restore
```

**Mục đích**: Thay đổi cấu hình tạm thời cho một block code
**Cách sử dụng**: Sử dụng trong context manager để đảm bảo config được restore

#### **write_table()** - Write to Hive Table
```python
# Ghi với mode mặc định (append)
engine.write_table(df, "database.table_name")

# Ghi với mode khác
engine.write_table(df, "database.table_name", mode="overwrite")

# Ghi với partition
engine.write_table(df, "database.table_name", 
                  mode="overwrite", 
                  partition_by=["year", "month"])

# Ghi với options
engine.write_table(df, "database.table_name",
                  mode="overwrite",
                  options={"compression": "snappy"})
```

**Mục đích**: Ghi DataFrame vào Hive table
**Parameters**:
- **df**: DataFrame cần ghi
- **table**: Tên table (fully qualified)
- **mode**: "append", "overwrite", "ignore", "error", "errorifexists"
- **options**: Dictionary chứa các options cho writer
- **partition_by**: List các columns để partition

## 2. Database Engines

### DBConfig Parameters

#### **max_pool_size** (Mặc định: 10)
```python
# Cho workload nhẹ
max_pool_size=5

# Cho workload trung bình
max_pool_size=10

# Cho workload nặng
max_pool_size=20
```

**Mục đích**: Số connection tối đa trong pool
**Cách chọn**:
- **Light workload**: 5-10
- **Medium workload**: 10-20
- **Heavy workload**: 20-50

#### **min_pool_size** (Mặc định: 1)
```python
# Giữ sẵn connections để giảm latency
min_pool_size=2

# Cho batch processing
min_pool_size=1
```

**Mục đích**: Số connection tối thiểu trong pool
**Cách chọn**:
- **Real-time apps**: 2-5 (giảm latency)
- **Batch processing**: 1 (tiết kiệm resources)

#### **connect_timeout_s** (Mặc định: 5)
```python
# Cho local database
connect_timeout_s=2

# Cho database trong cùng network
connect_timeout_s=5

# Cho database remote
connect_timeout_s=10
```

**Mục đích**: Thời gian chờ tối đa để thiết lập connection
**Cách chọn**:
- **Local**: 2-3 giây
- **Same network**: 5 giây
- **Remote**: 10-15 giây

#### **statement_timeout_ms** (Mặc định: 30000)
```python
# Cho queries nhanh
statement_timeout_ms=5000

# Cho queries trung bình
statement_timeout_ms=30000

# Cho batch operations
statement_timeout_ms=300000
```

**Mục đích**: Thời gian chờ tối đa để thực thi query
**Cách chọn**:
- **Simple queries**: 5-10 giây
- **Complex queries**: 30-60 giây
- **Batch operations**: 2-5 phút

#### **ssl_require** (Mặc định: True)
```python
# Production environment
ssl_require=True

# Development environment
ssl_require=False
```

**Mục đích**: Bắt buộc sử dụng SSL connection
**Cách chọn**:
- **Production**: Luôn True
- **Development**: Có thể False

### ClickHouse Specific Parameters

#### **ch_database** (Mặc định: None)
```python
ch_database="analytics"
ch_database="default"
```

**Mục đích**: Tên database ClickHouse để kết nối
**Cách sử dụng**: Chỉ dùng cho ClickHouse connections

#### **ch_user** (Mặc định: None)
```python
ch_user="analytics_user"
ch_user="default"
```

**Mục đích**: Username để authenticate với ClickHouse
**Cách sử dụng**: Chỉ dùng cho ClickHouse connections

#### **ch_password** (Mặc định: None)
```python
ch_password="secure_password"
ch_password=""
```

**Mục đích**: Password để authenticate với ClickHouse
**Cách sử dụng**: Chỉ dùng cho ClickHouse connections

## 3. Alert System

### DiscordAlertConfig Parameters

#### **username** (Mặc định: None)
```python
username="Data Pipeline Bot"
username="ETL Monitor"
username="System Health Bot"
```

**Mục đích**: Tên hiển thị của bot khi gửi messages
**Cách sử dụng**: Đặt tên mô tả rõ ràng về chức năng của bot

#### **avatar_url** (Mặc định: None)
```python
avatar_url="https://example.com/bot-avatar.png"
avatar_url="https://cdn.discordapp.com/avatars/bot-id/avatar.png"
```

**Mục đích**: Avatar image hiển thị cùng với messages
**Yêu cầu**: URL phải accessible từ internet, format PNG/JPG/GIF

#### **timeout_s** (Mặc định: 5)
```python
# Timeout ngắn cho local network
timeout_s=3

# Timeout mặc định
timeout_s=5

# Timeout dài cho network không ổn định
timeout_s=10
```

**Mục đích**: Thời gian chờ tối đa khi gửi request đến Discord
**Cách chọn**:
- **Local network**: 3-5 giây
- **Internet**: 5-10 giây
- **Unstable network**: 10-30 giây

#### **environment** (Mặc định: None)
```python
environment="production"
environment="staging"
environment="development"
environment="testing"
environment="local"
```

**Mục đích**: Xác định môi trường để phân biệt alerts
**Cách sử dụng**: Thêm vào title và fields để dễ phân biệt alerts từ các môi trường khác nhau

#### **service** (Mặc định: None)
```python
service="data-pipeline"
service="etl-processor"
service="user-service"
service="payment-gateway"
service="analytics-engine"
```

**Mục đích**: Xác định service nào gửi alert
**Cách sử dụng**: Thêm vào fields để tracking và debugging

#### **include_args** (Mặc định: True)
```python
# Bao gồm function arguments trong alert
include_args=True

# Không bao gồm arguments (privacy/security)
include_args=False
```

**Mục đích**: Quyết định có bao gồm function arguments trong alert hay không
**Cách chọn**:
- **True**: Debugging, development
- **False**: Production với sensitive data

#### **include_traceback** (Mặc định: True)
```python
# Bao gồm stack trace trong alert
include_traceback=True

# Không bao gồm stack trace
include_traceback=False
```

**Mục đích**: Quyết định có bao gồm stack trace trong alert hay không
**Cách chọn**:
- **True**: Debugging, development
- **False**: Production để giảm noise

## 4. Retry Mechanisms

### retriable() Parameters

#### **ex_types** (Bắt buộc)
```python
# Retry cho network errors
ex_types=(requests.exceptions.ConnectionError, requests.exceptions.Timeout)

# Retry cho database errors
ex_types=(psycopg.OperationalError, psycopg.errors.DeadlockDetected)

# Retry cho multiple exception types
ex_types=(ConnectionError, TimeoutError, ValueError)
```

**Mục đích**: Xác định loại exceptions nào sẽ trigger retry
**Cách chọn**:
- **Network operations**: ConnectionError, TimeoutError
- **Database operations**: OperationalError, DeadlockDetected
- **API calls**: HTTPError, ConnectionError
- **File operations**: IOError, OSError

#### **max_seconds** (Mặc định: 30)
```python
# Retry nhanh cho real-time operations
max_seconds=10

# Retry mặc định
max_seconds=30

# Retry dài cho batch operations
max_seconds=120
```

**Mục đích**: Tổng thời gian retry tối đa
**Cách chọn**:
- **Real-time operations**: 10-30 giây
- **Batch operations**: 60-120 giây
- **Critical operations**: 300+ giây

#### **max_attempts** (Mặc định: None)
```python
# Giới hạn số lần retry
max_attempts=3

# Retry nhiều lần cho critical operations
max_attempts=10

# Không giới hạn số lần (chỉ giới hạn thời gian)
max_attempts=None
```

**Mục đích**: Số lần retry tối đa
**Cách chọn**:
- **Simple operations**: 3-5 lần
- **Critical operations**: 5-10 lần
- **Unlimited**: None (chỉ giới hạn thời gian)

#### **initial_wait** (Mặc định: 0.2)
```python
# Chờ ngắn cho fast retry
initial_wait=0.1

# Chờ mặc định
initial_wait=0.2

# Chờ dài cho slow operations
initial_wait=1.0
```

**Mục đích**: Thời gian chờ ban đầu trước lần retry đầu tiên
**Cách chọn**:
- **Fast operations**: 0.1-0.2 giây
- **Normal operations**: 0.2-0.5 giây
- **Slow operations**: 1.0-2.0 giây

#### **max_wait** (Mặc định: 3.0)
```python
# Chờ tối đa ngắn
max_wait=1.0

# Chờ tối đa mặc định
max_wait=3.0

# Chờ tối đa dài
max_wait=10.0
```

**Mục đích**: Thời gian chờ tối đa giữa các lần retry
**Cách chọn**:
- **Fast operations**: 1-3 giây
- **Normal operations**: 3-5 giây
- **Slow operations**: 10-30 giây

#### **reraise** (Mặc định: True)
```python
# Re-raise exception sau khi hết retry
reraise=True

# Không re-raise, return None
reraise=False
```

**Mục đích**: Quyết định có re-raise exception sau khi hết retry hay không
**Cách chọn**:
- **True**: Để caller xử lý exception
- **False**: Để function fail silently

### retriable_with_discord() Parameters

#### **alert_cfg** (Bắt buộc)
```python
# Discord configuration
alert_cfg = DiscordAlertConfig(
    webhook_url="https://discord.com/api/webhooks/YOUR_WEBHOOK_URL",
    environment="production",
    service="my-service"
)
```

**Mục đích**: Cấu hình Discord để gửi notifications
**Yêu cầu**: Phải có DiscordAlertConfig hợp lệ

#### **notify_every_retry** (Mặc định: False)
```python
# Gửi notification mỗi lần retry
notify_every_retry=True

# Chỉ gửi notification khi fail hoàn toàn
notify_every_retry=False
```

**Mục đích**: Quyết định có gửi notification mỗi lần retry hay không
**Cách chọn**:
- **True**: Cho critical operations cần monitoring
- **False**: Để tránh spam notifications

## 5. Logging System

### setup_logging() Parameters

#### **level** (Mặc định: "INFO")
```python
# Debug logging
setup_logging(level="DEBUG")

# Info logging
setup_logging(level="INFO")

# Warning logging
setup_logging(level="WARNING")

# Error logging
setup_logging(level="ERROR")
```

**Mục đích**: Xác định mức độ logging
**Cách chọn**:
- **DEBUG**: Thông tin debug chi tiết
- **INFO**: Thông tin chung về hoạt động
- **WARNING**: Cảnh báo nhưng không dừng chương trình
- **ERROR**: Lỗi nghiêm trọng

#### **logfile** (Mặc định: None)
```python
# Log vào file
setup_logging(level="INFO", logfile="application.log")

# Log vào console only
setup_logging(level="INFO")

# Log vào file với path tuyệt đối
setup_logging(level="INFO", logfile="/var/log/myapp/app.log")
```

**Mục đích**: Xác định file để ghi logs
**Cách chọn**:
- **None**: Chỉ log vào console
- **String**: Đường dẫn file để ghi logs

## Ví dụ sử dụng tổng hợp

### 1. **Development Environment**
```python
# Spark engine cho development
with make_engine(
    kind="spark",
    app_name="dev_etl_pipeline",
    stop_on_exit=True,
    suppress_stop_errors=True
) as engine:
    
    # Database connection
    db_config = DBConfig(
        dsn="postgresql://user:pass@localhost:5432/devdb",
        max_pool_size=5,
        min_pool_size=1,
        connect_timeout_s=5,
        ssl_require=False
    )
    db_connector = create_connector(db_config)
    
    # Discord alerts
    alert_config = DiscordAlertConfig(
        webhook_url="https://discord.com/api/webhooks/DEV_WEBHOOK_URL",
        username="Dev Debug Bot",
        environment="development",
        service="etl-pipeline",
        include_args=True,
        include_traceback=True
    )
    
    # Retry với Discord notifications
    @retriable_with_discord(
        (ConnectionError, TimeoutError),
        alert_config,
        max_attempts=3,
        notify_every_retry=True
    )
    def process_data():
        # Process data logic
        pass
```

### 2. **Production Environment**
```python
# Spark engine cho production
with make_engine(
    kind="hive",
    app_name="production_etl_pipeline",
    stop_on_exit=True,
    suppress_stop_errors=True
) as engine:
    
    # Database connection
    db_config = DBConfig(
        dsn="postgresql://user:pass@prod-host:5432/proddb",
        max_pool_size=20,
        min_pool_size=5,
        connect_timeout_s=10,
        ssl_require=True
    )
    db_connector = create_connector(db_config)
    
    # Discord alerts
    alert_config = DiscordAlertConfig(
        webhook_url="https://discord.com/api/webhooks/PROD_WEBHOOK_URL",
        username="Production Monitor",
        environment="production",
        service="etl-pipeline",
        include_args=False,  # Không include sensitive data
        include_traceback=True
    )
    
    # Retry với Discord notifications
    @retriable_with_discord(
        (Exception,),
        alert_config,
        max_attempts=5,
        max_seconds=300,
        notify_every_retry=False  # Không spam notifications
    )
    def critical_operation():
        # Critical operation logic
        pass
```

### 3. **Interactive Analysis**
```python
# Spark engine cho interactive analysis
with make_engine(
    kind="hive",
    app_name="interactive_analysis",
    stop_on_exit=False,  # Giữ context chạy
    suppress_stop_errors=False  # Hiển thị errors để debug
) as engine:
    
    # Sử dụng temporary configuration
    with engine.temporary_conf({
        "spark.sql.autoBroadcastJoinThreshold": "50MB"
    }):
        result = engine.sql("""
            SELECT s.*, p.name as product_name
            FROM sales s
            JOIN products p ON s.product_id = p.id
        """)
    
    # Context vẫn chạy, có thể thực hiện thêm queries
    summary = engine.sql("SELECT COUNT(*) as total FROM sales")
    print(f"Total sales: {summary.collect()[0]['total']}")
```

## Best Practices

### 1. **Chọn tham số phù hợp với môi trường**
```python
# Development: Debug-friendly
stop_on_exit=True
suppress_stop_errors=False
include_args=True
include_traceback=True

# Production: Performance và security
stop_on_exit=True
suppress_stop_errors=True
include_args=False
include_traceback=True
```

### 2. **Sử dụng connection pooling hiệu quả**
```python
# Real-time applications
max_pool_size=20
min_pool_size=5

# Batch processing
max_pool_size=5
min_pool_size=1
```

### 3. **Tuning retry parameters**
```python
# Fast operations
max_attempts=3
max_seconds=10
initial_wait=0.1
max_wait=1.0

# Critical operations
max_attempts=10
max_seconds=300
initial_wait=1.0
max_wait=10.0
```

### 4. **Monitoring và alerting**
```python
# Critical operations cần monitoring
notify_every_retry=True

# Normal operations
notify_every_retry=False
```

