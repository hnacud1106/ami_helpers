# Spark/Hive Engines - Hướng dẫn xử lý dữ liệu

## Tổng quan

Module `spark_hive_engines` cung cấp các engine mạnh mẽ để xử lý dữ liệu quy mô lớn với Apache Spark và Hive. Engine hỗ trợ:

- **Spark Engine**: Xử lý dữ liệu với Apache Spark
- **Hive Engine**: Tích hợp với Hive Metastore và HDFS
- **Cấu hình linh hoạt**: Từ environment variables hoặc code
- **Resource management**: Tự động quản lý SparkContext lifecycle

## Cài đặt và cấu hình

### Yêu cầu hệ thống

- **Java 8+**: Cần thiết cho Apache Spark
- **Apache Spark 3.0+**: Download và cài đặt Spark
- **Hadoop/HDFS**: Nếu sử dụng Hive (optional)
- **Hive Metastore**: Nếu sử dụng Hive engine (optional)

### Cấu hình Environment Variables

Tạo file `.env` với các cấu hình sau:

```env
# ===============================
# Spark Application Configuration
# ===============================

# Tên ứng dụng Spark
SPARK_APP_NAME=my_data_processing_app

# URL Spark master (local, yarn, spark://host:port)
SPARK_MASTER=spark://your-spark-master:7077

# Dừng SparkContext khi ứng dụng kết thúc
SPARK_STOP_ON_EXIT=true

# ===============================
# Spark Executor & Resource Config
# ===============================

# Bật/tắt dynamic allocation
SPARK_CONF__spark.dynamicAllocation.enabled=false

# Số CPU core mỗi executor
SPARK_CONF__spark.executor.cores=4

# RAM mỗi executor
SPARK_CONF__spark.executor.memory=4G

# Driver memory
SPARK_CONF__spark.driver.memory=2G

# ===============================
# Spark SQL / Hive Configurations
# ===============================

# Thư mục warehouse (HDFS hoặc local path)
SPARK_CONF__spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse

# Loại catalog: hive / in-memory
SPARK_CONF__spark.sql.catalogImplementation=hive

# Cho phép tạo managed table trong external DB
SPARK_CONF__spark.sql.legacy.allowCreatingManagedTableInExternalDb=true

# Số partition khi shuffle dữ liệu
SPARK_CONF__spark.sql.shuffle.partitions=200

# Ghi đè partition mode: static / dynamic
SPARK_CONF__spark.sql.sources.partitionOverwriteMode=dynamic

# Bật adaptive query execution
SPARK_CONF__spark.sql.adaptive.enabled=true

# ===============================
# Hive Metastore Configuration
# ===============================

# Địa chỉ Hive Metastore Thrift
SPARK_CONF__hive.metastore.uris=thrift://hive-metastore:9083

# Cho phép dynamic partition
SPARK_CONF__hive.exec.dynamic.partition=true

# Chế độ dynamic partition (strict / nonstrict)
SPARK_CONF__hive.exec.dynamic.partition.mode=nonstrict
```

## Chi tiết các tham số Custom của AMI Helpers

### 1. **make_engine() Parameters**

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

#### **app_name** (Mặc định: từ SPARK_APP_NAME hoặc "app_name")
```python
# Từ environment variable
# SPARK_APP_NAME=my_app

# Từ code
with make_engine(app_name="etl_pipeline") as engine:
    pass
```

**Mục đích**: Tên ứng dụng hiển thị trong Spark UI
**Cách sử dụng**: Đặt tên mô tả rõ ràng về chức năng của ứng dụng

#### **master** (Mặc định: từ SPARK_MASTER)
```python
# Local mode
with make_engine(master="local[4]") as engine:
    pass

# Cluster mode
with make_engine(master="spark://master:7077") as engine:
    pass
```

**Mục đích**: Xác định Spark master URL
**Cách chọn**:
- **Development**: `local[4]` hoặc `local[*]`
- **Production**: `spark://master:7077` hoặc `yarn`

#### **enable_hive** (Mặc định: True nếu kind="hive")
```python
# Bật Hive support
with make_engine(enable_hive=True) as engine:
    pass

# Tắt Hive support
with make_engine(enable_hive=False) as engine:
    pass
```

**Mục đích**: Bật/tắt Hive Metastore integration
**Cách chọn**:
- **True**: Khi cần truy cập Hive tables
- **False**: Khi chỉ sử dụng Spark SQL thuần

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

### 2. **TransformConfig Parameters**

#### **app_name** (Mặc định: "app_name")
```python
config = TransformConfig(
    app_name="my_data_processing_app"
)
```

**Mục đích**: Tên ứng dụng Spark
**Cách sử dụng**: Đặt tên mô tả rõ ràng

#### **master** (Mặc định: None)
```python
config = TransformConfig(
    master="local[4]"
)
```

**Mục đích**: Spark master URL
**Cách sử dụng**: Xác định cluster hoặc local mode

#### **enable_hive** (Mặc định: False)
```python
config = TransformConfig(
    enable_hive=True
)
```

**Mục đích**: Bật Hive Metastore integration
**Cách sử dụng**: True khi cần truy cập Hive tables

#### **stop_on_exit** (Mặc định: True)
```python
config = TransformConfig(
    stop_on_exit=True
)
```

**Mục đích**: Tự động dừng SparkContext khi thoát
**Cách sử dụng**: True cho batch jobs, False cho interactive

#### **suppress_stop_errors** (Mặc định: True)
```python
config = TransformConfig(
    suppress_stop_errors=True
)
```

**Mục đích**: Ẩn lỗi khi dừng SparkContext
**Cách sử dụng**: True để tránh spam errors

#### **conf** (Mặc định: {})
```python
config = TransformConfig(
    conf={
        "spark.executor.memory": "4G",
        "spark.sql.shuffle.partitions": "200"
    }
)
```

**Mục đích**: Cấu hình Spark properties
**Cách sử dụng**: Dictionary chứa các Spark configuration properties

### 3. **TransformEngine Methods**

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

#### **sql()** - SQL Execution
```python
# SQL đơn giản
df = engine.sql("SELECT * FROM my_table")

# SQL với parameters
df = engine.sql("SELECT * FROM my_table WHERE id = %(id)s", {"id": 123})
```

**Mục đích**: Thực hiện SQL queries
**Cách sử dụng**: 
- **query**: SQL string
- **args**: Parameters cho prepared statements (optional)

#### **read_table()** - Read Hive Table
```python
# Đọc từ Hive table
df = engine.read_table("database.table_name")

# Đọc từ temp view
df = engine.read_table("temp_view_name")
```

**Mục đích**: Đọc dữ liệu từ Hive table hoặc temp view
**Cách sử dụng**: Sử dụng fully qualified table name

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

## Ví dụ sử dụng thực tế

### 1. **Development Environment**
```python
# Cấu hình cho development
with make_engine(
    kind="spark",
    app_name="dev_etl_pipeline",
    master="local[4]",
    stop_on_exit=True,
    suppress_stop_errors=True,
    conf={
        "spark.executor.memory": "2G",
        "spark.driver.memory": "1G",
        "spark.sql.shuffle.partitions": "50"
    }
) as engine:
    
    # Sử dụng engine
    df = engine.sql("SELECT 1 as test")
    result = df.collect()
    print(f"Result: {result[0]['test']}")
```

### 2. **Production ETL Pipeline**
```python
# Cấu hình cho production
with make_engine(
    kind="hive",
    app_name="production_etl_pipeline",
    master="spark://spark-master:7077",
    enable_hive=True,
    stop_on_exit=True,
    suppress_stop_errors=True,
    conf={
        "spark.executor.memory": "8G",
        "spark.driver.memory": "4G",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.enabled": "true"
    }
) as engine:
    
    # ETL pipeline
    source_df = engine.read_table("staging.raw_data")
    
    # Transform
    transformed_df = engine.sql("""
        SELECT 
            id,
            name,
            UPPER(category) as category,
            amount * 1.1 as adjusted_amount,
            current_timestamp() as processed_at
        FROM raw_data
    """)
    
    # Load
    engine.write_table(
        transformed_df,
        "analytics.processed_data",
        mode="overwrite",
        partition_by=["processed_at"]
    )
```

### 3. **Interactive Analysis**
```python
# Cấu hình cho interactive analysis
with make_engine(
    kind="hive",
    app_name="interactive_analysis",
    master="local[*]",
    enable_hive=True,
    stop_on_exit=False,  # Giữ context chạy
    suppress_stop_errors=False,  # Hiển thị errors để debug
    conf={
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    }
) as engine:
    
    # Analysis queries
    df = engine.sql("SELECT * FROM analytics.sales_data")
    
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

### 4. **Custom TransformConfig**
```python
# Tạo custom configuration
config = TransformConfig(
    app_name="custom_etl",
    master="yarn",
    enable_hive=True,
    stop_on_exit=True,
    suppress_stop_errors=True,
    conf={
        "spark.executor.memory": "4G",
        "spark.driver.memory": "2G",
        "spark.sql.shuffle.partitions": "100",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }
)

# Sử dụng custom config
engine = TransformEngine(config)

try:
    # Sử dụng engine
    df = engine.sql("SELECT * FROM my_table")
    result = df.collect()
    print(f"Retrieved {len(result)} rows")
finally:
    # Manual cleanup
    engine.__exit__(None, None, None)
```

## Sử dụng cơ bản

### Tạo Spark Engine

```python
from ami_helpers.spark_hive_engines import make_engine

# Tạo Spark engine với cấu hình mặc định
with make_engine(kind="spark") as engine:
    # Sử dụng engine
    df = engine.sql("SELECT 1 as test")
    df.show()
```

### Tạo Hive Engine

```python
# Tạo Hive engine với Hive Metastore
with make_engine(kind="hive") as engine:
    # Có thể truy cập Hive tables
    df = engine.read_table("default.my_table")
    df.show()
```

### Cấu hình tùy chỉnh

```python
# Cấu hình từ code (override environment variables)
with make_engine(
    kind="spark",
    app_name="custom_app",
    master="local[4]",
    conf={
        "spark.executor.memory": "2G",
        "spark.executor.cores": "2",
        "spark.sql.shuffle.partitions": "100"
    }
) as engine:
    # Sử dụng engine với cấu hình tùy chỉnh
    pass
```

## Các chức năng chính

### 1. SQL Queries

```python
with make_engine(kind="spark") as engine:
    # Thực hiện SQL query
    result_df = engine.sql("""
        SELECT 
            user_id,
            COUNT(*) as event_count,
            MAX(timestamp) as last_event
        FROM events 
        WHERE date >= '2024-01-01'
        GROUP BY user_id
        HAVING COUNT(*) > 10
    """)
    
    result_df.show()
```

### 2. Đọc dữ liệu từ bảng

```python
with make_engine(kind="hive") as engine:
    # Đọc từ Hive table
    users_df = engine.read_table("default.users")
    
    # Đọc với điều kiện
    active_users = engine.sql("""
        SELECT * FROM default.users 
        WHERE status = 'active'
    """)
```

### 3. Ghi dữ liệu vào bảng

```python
with make_engine(kind="hive") as engine:
    # Tạo DataFrame
    from pyspark.sql import Row
    data = [
        Row(user_id=1, name="Alice", age=25),
        Row(user_id=2, name="Bob", age=30),
        Row(user_id=3, name="Charlie", age=35)
    ]
    
    df = engine.spark.createDataFrame(data)
    
    # Ghi vào bảng với các mode khác nhau
    engine.write_table(df, "default.users", mode="overwrite")
    engine.write_table(df, "default.users", mode="append")
    engine.write_table(df, "default.users", mode="ignore")
```

### 4. Ghi với partition

```python
with make_engine(kind="hive") as engine:
    # Ghi dữ liệu với partition
    engine.write_table(
        df, 
        "default.events", 
        mode="overwrite",
        partition_by=["year", "month", "day"]
    )
```

### 5. Cấu hình tạm thời

```python
with make_engine(kind="spark") as engine:
    # Thay đổi cấu hình tạm thời cho một block code
    with engine.temporary_conf({
        "spark.sql.autoBroadcastJoinThreshold": "128MB",
        "spark.sql.adaptive.skewJoin.enabled": "true"
    }):
        # Join operation với cấu hình tối ưu
        result = engine.sql("""
            SELECT a.*, b.name 
            FROM large_table a 
            JOIN small_table b ON a.id = b.id
        """)
```

## Advanced Usage

### 1. ETL Pipeline hoàn chỉnh

```python
from ami_helpers.spark_hive_engines import make_engine
from ami_helpers.utils.log_info import setup_logging
import logging

setup_logging(level="INFO")
logger = logging.getLogger(__name__)

def etl_pipeline():
    with make_engine(kind="hive", app_name="etl_pipeline") as engine:
        try:
            # Extract: Đọc dữ liệu từ source
            logger.info("Extracting data from source tables")
            raw_events = engine.read_table("staging.raw_events")
            users = engine.read_table("staging.users")
            
            # Transform: Xử lý và join dữ liệu
            logger.info("Transforming data")
            enriched_events = engine.sql("""
                SELECT 
                    e.*,
                    u.name as user_name,
                    u.segment as user_segment,
                    DATE(e.timestamp) as event_date
                FROM staging.raw_events e
                LEFT JOIN staging.users u ON e.user_id = u.id
                WHERE e.timestamp >= '2024-01-01'
            """)
            
            # Load: Ghi vào target table
            logger.info("Loading data to target table")
            engine.write_table(
                enriched_events,
                "analytics.enriched_events",
                mode="overwrite",
                partition_by=["event_date"]
            )
            
            # Validation
            count = enriched_events.count()
            logger.info(f"ETL completed successfully. Processed {count} records")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise

if __name__ == "__main__":
    etl_pipeline()
```

### 2. Data Quality Checks

```python
def data_quality_checks():
    with make_engine(kind="hive") as engine:
        # Kiểm tra null values
        null_check = engine.sql("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(user_id) as non_null_user_ids,
                COUNT(*) - COUNT(user_id) as null_user_ids
            FROM analytics.events
        """)
        
        # Kiểm tra duplicates
        duplicate_check = engine.sql("""
            SELECT 
                user_id, 
                event_type, 
                timestamp,
                COUNT(*) as duplicate_count
            FROM analytics.events
            GROUP BY user_id, event_type, timestamp
            HAVING COUNT(*) > 1
        """)
        
        # Kiểm tra data range
        range_check = engine.sql("""
            SELECT 
                MIN(timestamp) as min_timestamp,
                MAX(timestamp) as max_timestamp,
                COUNT(DISTINCT user_id) as unique_users
            FROM analytics.events
        """)
        
        # Hiển thị kết quả
        print("=== Data Quality Report ===")
        null_check.show()
        duplicate_check.show()
        range_check.show()
```

### 3. Performance Optimization

```python
def optimized_processing():
    with make_engine(
        kind="hive",
        conf={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
        }
    ) as engine:
        
        # Cache frequently used tables
        users_df = engine.read_table("analytics.users")
        users_df.cache()
        
        # Sử dụng broadcast join cho small tables
        with engine.temporary_conf({
            "spark.sql.autoBroadcastJoinThreshold": "50MB"
        }):
            result = engine.sql("""
                SELECT /*+ BROADCAST(small_table) */
                    l.*, s.name
                FROM large_table l
                JOIN small_table s ON l.id = s.id
            """)
        
        # Repartition trước khi ghi
        result = result.repartition(200, "partition_column")
        engine.write_table(result, "analytics.result", mode="overwrite")
```

## Monitoring và Debugging

### 1. Logging

```python
from ami_helpers.utils.log_info import setup_logging
import logging

# Setup logging
setup_logging(level="INFO", logfile="spark_operations.log")
logger = logging.getLogger(__name__)

def monitored_processing():
    with make_engine(kind="spark") as engine:
        logger.info("Starting data processing")
        
        # Log Spark UI URL
        logger.info(f"Spark UI: {engine.spark.sparkContext.uiWebUrl}")
        
        # Process data
        df = engine.sql("SELECT * FROM my_table")
        count = df.count()
        
        logger.info(f"Processed {count} records")
```

### 2. Performance Monitoring

```python
import time

def performance_monitoring():
    with make_engine(kind="spark") as engine:
        start_time = time.time()
        
        # Enable detailed logging
        engine.spark.sparkContext.setLogLevel("INFO")
        
        # Process data
        result = engine.sql("SELECT * FROM large_table WHERE date >= '2024-01-01'")
        result.count()
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"Processing completed in {duration:.2f} seconds")
        
        # Get Spark metrics
        sc = engine.spark.sparkContext
        print(f"Total tasks: {sc.statusTracker().getExecutorInfos()}")
```

## Best Practices

### 1. Resource Management

```python
# Sử dụng context manager để đảm bảo cleanup
with make_engine(kind="spark") as engine:
    # Code xử lý
    pass
# SparkContext sẽ được tự động dừng
```

### 2. Cấu hình phù hợp

```python
# Cấu hình theo workload
def configure_for_batch_processing():
    return make_engine(
        kind="hive",
        conf={
            "spark.executor.memory": "8G",
            "spark.executor.cores": "4",
            "spark.sql.shuffle.partitions": "400",
            "spark.sql.adaptive.enabled": "true"
        }
    )

def configure_for_streaming():
    return make_engine(
        kind="spark",
        conf={
            "spark.executor.memory": "4G",
            "spark.executor.cores": "2",
            "spark.sql.streaming.checkpointLocation": "/checkpoint/path"
        }
    )
```

### 3. Error Handling

```python
def robust_processing():
    try:
        with make_engine(kind="hive") as engine:
            # Xử lý dữ liệu
            result = engine.sql("SELECT * FROM my_table")
            engine.write_table(result, "output.table", mode="overwrite")
            
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        # Cleanup hoặc retry logic
        raise
```

## Troubleshooting

### Lỗi thường gặp

#### 1. Java heap space

```
java.lang.OutOfMemoryError: Java heap space
```

**Giải pháp:**
- Tăng driver memory: `spark.driver.memory=4G`
- Tăng executor memory: `spark.executor.memory=4G`
- Giảm batch size hoặc tăng partitions

#### 2. Connection refused to Spark master

```
java.net.ConnectException: Connection refused
```

**Giải pháp:**
- Kiểm tra Spark master đang chạy
- Kiểm tra network connectivity
- Kiểm tra port configuration

#### 3. Hive metastore connection failed

```
java.sql.SQLException: Could not connect to address
```

**Giải pháp:**
- Kiểm tra Hive metastore service
- Kiểm tra connection string
- Kiểm tra authentication

### Debug Tips

```python
# Bật debug logging
engine.spark.sparkContext.setLogLevel("DEBUG")

# Kiểm tra Spark configuration
for key, value in engine.spark.conf.getAll().items():
    print(f"{key}: {value}")

# Kiểm tra available tables
engine.spark.catalog.listTables().show()
```
