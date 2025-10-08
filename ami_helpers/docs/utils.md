# Utilities - Các tiện ích hỗ trợ

## Tổng quan

Module `utils` cung cấp các tiện ích cơ bản và quan trọng cho việc phát triển ứng dụng:

- **Logging**: Cấu hình logging toàn diện với console và file
- **Retry Mechanisms**: Hệ thống retry thông minh với exponential backoff
- **Error Handling**: Xử lý lỗi và retry với Discord notifications

## Logging System

### Cấu hình cơ bản

```python
from ami_helpers.utils.log_info import setup_logging
import logging

# Cấu hình logging cơ bản
setup_logging(level="INFO")

# Sử dụng logger
logger = logging.getLogger(__name__)
logger.info("Application started")
logger.warning("This is a warning message")
logger.error("This is an error message")
```

### Cấu hình với file log

```python
# Cấu hình logging với file output
setup_logging(
    level="DEBUG",
    logfile="application.log"
)

logger = logging.getLogger("my_module")
logger.debug("Debug information")
logger.info("Info message")
logger.warning("Warning message")
logger.error("Error message")
```

### Các level logging

```python
# DEBUG: Thông tin debug chi tiết
logger.debug("Variable value: %s", variable_value)

# INFO: Thông tin chung về hoạt động
logger.info("Processing %d records", record_count)

# WARNING: Cảnh báo nhưng không dừng chương trình
logger.warning("High memory usage: %d%%", memory_percent)

# ERROR: Lỗi nghiêm trọng
logger.error("Database connection failed: %s", str(exception))
```

### Cấu hình nâng cao

```python
# Cấu hình logging cho production
def setup_production_logging():
    setup_logging(
        level="INFO",
        logfile="/var/log/myapp/application.log"
    )
    
    # Cấu hình logger cho các module cụ thể
    logging.getLogger("stomp.py").setLevel(logging.WARN)
    logging.getLogger("elasticsearch").setLevel(logging.WARN)
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("celery.app.trace").setLevel(logging.WARNING)
    logging.getLogger("py4j").setLevel(logging.WARN)
    logging.getLogger("httpx").setLevel(logging.WARN)

# Cấu hình logging cho development
def setup_development_logging():
    setup_logging(
        level="DEBUG",
        logfile="debug.log"
    )
```

## Retry Mechanisms

### Retry cơ bản

```python
from ami_helpers.utils.retry import retriable
import requests
import time

@retriable(
    (requests.exceptions.ConnectionError, requests.exceptions.Timeout),
    max_attempts=3,
    max_seconds=30,
    initial_wait=0.5,
    max_wait=5.0
)
def fetch_data_from_api(url):
    """Fetch data from API with automatic retry"""
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

# Sử dụng
try:
    data = fetch_data_from_api("https://api.example.com/data")
    print(f"Fetched {len(data)} records")
except Exception as e:
    print(f"Failed to fetch data after retries: {e}")
```

## Chi tiết các tham số Retry Mechanism

### 1. **retriable** - Retry đơn giản

#### Tham số cơ bản
```python
@retriable(
    ex_types=(Exception,),           # Các exception types sẽ retry
    max_seconds=30,                 # Thời gian retry tối đa (giây)
    max_attempts=None,              # Số lần retry tối đa
    initial_wait=0.2,               # Thời gian chờ ban đầu (giây)
    max_wait=3.0,                  # Thời gian chờ tối đa (giây)
    reraise=True                    # Re-raise exception sau khi hết retry
)
def my_function():
    pass
```

#### Chi tiết từng tham số

##### **ex_types** (Bắt buộc)
```python
# Retry cho network errors
ex_types=(requests.exceptions.ConnectionError, requests.exceptions.Timeout)

# Retry cho database errors
ex_types=(psycopg.OperationalError, psycopg.errors.DeadlockDetected)

# Retry cho multiple exception types
ex_types=(ConnectionError, TimeoutError, ValueError)

# Retry cho tất cả exceptions (không khuyến nghị)
ex_types=(Exception,)
```

**Mục đích**: Xác định loại exceptions nào sẽ trigger retry
**Cách chọn**:
- **Network operations**: ConnectionError, TimeoutError
- **Database operations**: OperationalError, DeadlockDetected
- **API calls**: HTTPError, ConnectionError
- **File operations**: IOError, OSError

##### **max_seconds** (Mặc định: 30)
```python
# Retry nhanh cho real-time operations
max_seconds=10

# Retry mặc định
max_seconds=30

# Retry dài cho batch operations
max_seconds=120

# Retry rất dài cho critical operations
max_seconds=300
```

**Mục đích**: Tổng thời gian retry tối đa
**Cách chọn**:
- **Real-time operations**: 10-30 giây
- **Batch operations**: 60-120 giây
- **Critical operations**: 300+ giây

##### **max_attempts** (Mặc định: None)
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

##### **initial_wait** (Mặc định: 0.2)
```python
# Chờ ngắn cho fast retry
initial_wait=0.1

# Chờ mặc định
initial_wait=0.2

# Chờ dài cho slow operations
initial_wait=1.0

# Chờ rất dài cho heavy operations
initial_wait=2.0
```

**Mục đích**: Thời gian chờ ban đầu trước lần retry đầu tiên
**Cách chọn**:
- **Fast operations**: 0.1-0.2 giây
- **Normal operations**: 0.2-0.5 giây
- **Slow operations**: 1.0-2.0 giây

##### **max_wait** (Mặc định: 3.0)
```python
# Chờ tối đa ngắn
max_wait=1.0

# Chờ tối đa mặc định
max_wait=3.0

# Chờ tối đa dài
max_wait=10.0

# Chờ tối đa rất dài
max_wait=30.0
```

**Mục đích**: Thời gian chờ tối đa giữa các lần retry
**Cách chọn**:
- **Fast operations**: 1-3 giây
- **Normal operations**: 3-5 giây
- **Slow operations**: 10-30 giây

##### **reraise** (Mặc định: True)
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

### 2. **retriable_with_discord** - Retry với Discord notifications

#### Tham số nâng cao
```python
@retriable_with_discord(
    ex_types=(Exception,),          # Các exception types sẽ retry
    alert_cfg=discord_config,       # Discord configuration
    max_seconds=30,                 # Thời gian retry tối đa
    max_attempts=None,              # Số lần retry tối đa
    initial_wait=0.2,               # Thời gian chờ ban đầu
    max_wait=3.0,                   # Thời gian chờ tối đa
    notify_every_retry=False,       # Gửi notification mỗi lần retry
    reraise=True                    # Re-raise exception
)
def my_function():
    pass
```

#### Chi tiết tham số mới

##### **alert_cfg** (Bắt buộc)
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

##### **notify_every_retry** (Mặc định: False)
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

## Ví dụ cấu hình theo use case

### 1. **API Calls** (Fast retry)
```python
@retriable(
    (requests.exceptions.ConnectionError, requests.exceptions.Timeout),
    max_attempts=3,
    max_seconds=15,
    initial_wait=0.1,
    max_wait=2.0
)
def call_external_api(url: str, data: dict):
    """Call external API with fast retry"""
    response = requests.post(url, json=data, timeout=5)
    response.raise_for_status()
    return response.json()
```

**Đặc điểm**:
- Retry nhanh (0.1-2.0s)
- Ít lần retry (3 lần)
- Thời gian tổng ngắn (15s)

### 2. **Database Operations** (Balanced retry)
```python
@retriable(
    (psycopg.OperationalError, psycopg.errors.DeadlockDetected),
    max_attempts=5,
    max_seconds=60,
    initial_wait=0.5,
    max_wait=5.0
)
def execute_database_query(query: str, params: dict):
    """Execute database query with balanced retry"""
    with connector._connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
```

**Đặc điểm**:
- Retry cân bằng (0.5-5.0s)
- Số lần retry vừa phải (5 lần)
- Thời gian tổng trung bình (60s)

### 3. **Critical Operations** (Comprehensive retry)
```python
@retriable_with_discord(
    (Exception,),
    alert_cfg=discord_config,
    max_attempts=10,
    max_seconds=300,
    initial_wait=1.0,
    max_wait=10.0,
    notify_every_retry=True
)
def critical_data_processing():
    """Critical data processing with comprehensive retry"""
    # Complex data processing logic
    pass
```

**Đặc điểm**:
- Retry dài (1.0-10.0s)
- Nhiều lần retry (10 lần)
- Thời gian tổng dài (300s)
- Discord notifications

### 4. **File Operations** (I/O retry)
```python
@retriable(
    (IOError, OSError),
    max_attempts=3,
    max_seconds=30,
    initial_wait=0.2,
    max_wait=3.0
)
def read_large_file(file_path: str):
    """Read large file with I/O retry"""
    with open(file_path, 'r') as f:
        return f.read()
```

**Đặc điểm**:
- Retry cho I/O errors
- Thời gian retry ngắn
- Ít lần retry

## Advanced Retry Patterns

### 1. **Conditional Retry**
```python
def should_retry(exception: Exception) -> bool:
    """Determine if exception should trigger retry"""
    if isinstance(exception, ConnectionError):
        return True
    if isinstance(exception, TimeoutError):
        return True
    if isinstance(exception, ValueError):
        return False  # Don't retry logic errors
    return False

@retriable(
    (ConnectionError, TimeoutError),
    max_attempts=3
)
def conditional_operation():
    """Operation with conditional retry logic"""
    try:
        # Operation that might fail
        pass
    except ValueError as e:
        # Don't retry logic errors
        raise e
    except Exception as e:
        if should_retry(e):
            raise e
        else:
            # Handle non-retryable errors
            pass
```

### 2. **Exponential Backoff với Jitter**
```python
# Thư viện đã sử dụng exponential backoff với jitter
# initial_wait=0.2, max_wait=3.0 sẽ tạo ra:
# Attempt 1: 0.2s
# Attempt 2: 0.4s + jitter
# Attempt 3: 0.8s + jitter
# Attempt 4: 1.6s + jitter
# Attempt 5: 3.0s + jitter (max_wait)
```

### 3. **Circuit Breaker Pattern**
```python
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "HALF_OPEN"
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
            
            raise e

# Sử dụng với retry
circuit_breaker = CircuitBreaker()

@retriable(
    (Exception,),
    max_attempts=3
)
def operation_with_circuit_breaker():
    return circuit_breaker.call(risky_operation)
```

### 4. **Retry với Different Strategies**
```python
# Strategy 1: Fast retry cho real-time operations
@retriable(
    (ConnectionError,),
    max_attempts=3,
    max_seconds=10,
    initial_wait=0.1,
    max_wait=1.0
)
def real_time_operation():
    pass

# Strategy 2: Slow retry cho batch operations
@retriable(
    (Exception,),
    max_attempts=5,
    max_seconds=300,
    initial_wait=1.0,
    max_wait=10.0
)
def batch_operation():
    pass

# Strategy 3: Critical retry với monitoring
@retriable_with_discord(
    (Exception,),
    alert_cfg=discord_config,
    max_attempts=10,
    max_seconds=600,
    initial_wait=2.0,
    max_wait=20.0,
    notify_every_retry=True
)
def critical_operation():
    pass
```

## Best Practices

### 1. **Chọn Exception Types phù hợp**
```python
# Tốt - Specific exceptions
@retriable(
    (requests.exceptions.ConnectionError, requests.exceptions.Timeout)
)
def api_call():
    pass

# Không tốt - Too broad
@retriable((Exception,))
def api_call():
    pass
```

### 2. **Tuning Retry Parameters**
```python
# Cho operations nhanh
@retriable(
    (ConnectionError,),
    max_attempts=3,
    max_seconds=10,
    initial_wait=0.1,
    max_wait=1.0
)

# Cho operations chậm
@retriable(
    (Exception,),
    max_attempts=5,
    max_seconds=120,
    initial_wait=1.0,
    max_wait=10.0
)
```

### 3. **Monitoring và Logging**
```python
import logging

logger = logging.getLogger(__name__)

@retriable(
    (Exception,),
    max_attempts=3
)
def monitored_operation():
    logger.info("Starting operation")
    try:
        # Operation logic
        result = perform_operation()
        logger.info("Operation completed successfully")
        return result
    except Exception as e:
        logger.warning(f"Operation failed: {e}")
        raise e
```

### 4. **Error Handling**
```python
@retriable(
    (ConnectionError, TimeoutError),
    max_attempts=3,
    reraise=True
)
def robust_operation():
    try:
        return risky_operation()
    except ValueError as e:
        # Don't retry logic errors
        logger.error(f"Logic error: {e}")
        raise e
    except Exception as e:
        # Retry other errors
        logger.warning(f"Retryable error: {e}")
        raise e
```

### Retry với Discord notifications

```python
from ami_helpers.utils.retry import retriable_with_discord
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig
import psycopg

# Cấu hình Discord
discord_config = DiscordAlertConfig(
    webhook_url="your-webhook-url",
    environment="production",
    service="database-service"
)

@retriable_with_discord(
    (psycopg.OperationalError, psycopg.errors.DeadlockDetected),
    discord_config,
    max_attempts=5,
    max_seconds=120,
    notify_every_retry=True,
    reraise=True
)
def critical_database_operation():
    """Critical database operation with monitoring"""
    # Simulate database operation
    import random
    if random.random() < 0.3:  # 30% chance of failure
        raise psycopg.OperationalError("Connection lost")
    
    return "Operation completed successfully"

# Sử dụng
try:
    result = critical_database_operation()
    print(f"Result: {result}")
except Exception as e:
    print(f"Operation failed: {e}")
```

### Cấu hình retry nâng cao

```python
# Retry với custom parameters
@retriable(
    (ConnectionError, TimeoutError, ValueError),
    max_attempts=10,           # Tối đa 10 lần thử
    max_seconds=300,           # Tối đa 5 phút
    initial_wait=1.0,          # Bắt đầu với 1 giây
    max_wait=30.0,             # Tối đa 30 giây giữa các lần thử
    reraise=True               # Re-raise exception sau khi hết retry
)
def complex_operation():
    # Operation có thể gây nhiều loại lỗi
    pass

# Retry chỉ với thời gian
@retriable(
    (Exception,),  # Retry cho tất cả exceptions
    max_seconds=60,  # Chỉ retry trong 60 giây
    initial_wait=0.1,
    max_wait=2.0
)
def quick_retry_operation():
    # Operation cần retry nhanh
    pass

# Retry chỉ với số lần thử
@retriable(
    (ConnectionError,),
    max_attempts=3,  # Chỉ thử 3 lần
    initial_wait=0.5,
    max_wait=5.0
)
def limited_retry_operation():
    # Operation với số lần retry giới hạn
    pass
```

## Advanced Usage

### 1. ETL Pipeline với Logging và Retry

```python
from ami_helpers.utils.log_info import setup_logging
from ami_helpers.utils.retry import retriable_with_discord
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig
import logging
import time

class ETLPipeline:
    def __init__(self):
        # Setup logging
        setup_logging(level="INFO", logfile="etl_pipeline.log")
        self.logger = logging.getLogger(__name__)
        
        # Setup Discord alerts
        self.discord_config = DiscordAlertConfig(
            webhook_url="your-webhook-url",
            environment="production",
            service="etl-pipeline"
        )
    
    @retriable_with_discord(
        (ConnectionError, TimeoutError),
        discord_config,
        max_attempts=3,
        notify_every_retry=True
    )
    def extract_data(self, source_url):
        """Extract data from source with retry"""
        self.logger.info(f"Starting data extraction from {source_url}")
        start_time = time.time()
        
        # Simulate extraction
        import requests
        response = requests.get(source_url, timeout=30)
        response.raise_for_status()
        
        duration = time.time() - start_time
        self.logger.info(f"Data extraction completed in {duration:.2f} seconds")
        
        return response.json()
    
    @retriable_with_discord(
        (ValueError, KeyError),
        discord_config,
        max_attempts=2
    )
    def transform_data(self, raw_data):
        """Transform data with retry"""
        self.logger.info(f"Starting data transformation for {len(raw_data)} records")
        
        # Simulate transformation
        if not raw_data:
            raise ValueError("No data to transform")
        
        transformed_data = []
        for record in raw_data:
            if 'id' not in record:
                raise KeyError("Missing required field 'id'")
            
            transformed_record = {
                'id': record['id'],
                'name': record.get('name', 'Unknown'),
                'processed_at': time.time()
            }
            transformed_data.append(transformed_record)
        
        self.logger.info(f"Data transformation completed: {len(transformed_data)} records")
        return transformed_data
    
    def load_data(self, data, target_table):
        """Load data to target (no retry needed)"""
        self.logger.info(f"Loading {len(data)} records to {target_table}")
        
        # Simulate loading
        time.sleep(1)
        
        self.logger.info(f"Data loading completed successfully")
    
    def run_pipeline(self, source_url, target_table):
        """Run complete ETL pipeline"""
        try:
            self.logger.info("Starting ETL pipeline")
            
            # Extract
            raw_data = self.extract_data(source_url)
            
            # Transform
            transformed_data = self.transform_data(raw_data)
            
            # Load
            self.load_data(transformed_data, target_table)
            
            self.logger.info("ETL pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {e}")
            raise

# Sử dụng
if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run_pipeline("https://api.example.com/data", "target_table")
```

### 2. Database Operations với Monitoring

```python
from ami_helpers.database_engines import create_connector, DBConfig
from ami_helpers.utils.retry import retriable_with_discord
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig
import psycopg
import logging

class DatabaseManager:
    def __init__(self, db_dsn: str):
        self.logger = logging.getLogger(__name__)
        
        # Database configuration
        self.db_config = DBConfig(
            dsn=db_dsn,
            max_pool_size=10,
            connect_timeout_s=10
        )
        self.connector = create_connector(self.db_config)
        
        # Discord configuration
        self.discord_config = DiscordAlertConfig(
            webhook_url="your-webhook-url",
            environment="production",
            service="database-manager"
        )
    
    @retriable_with_discord(
        (psycopg.OperationalError, psycopg.errors.DeadlockDetected),
        discord_config,
        max_attempts=5,
        max_seconds=60,
        notify_every_retry=True
    )
    def execute_query(self, query: str, params: dict = None):
        """Execute query with retry and monitoring"""
        self.logger.info(f"Executing query: {query[:100]}...")
        
        start_time = time.time()
        result = self.connector.fetchall(query, params)
        duration = time.time() - start_time
        
        self.logger.info(f"Query executed successfully in {duration:.2f}s, returned {len(result)} rows")
        return result
    
    @retriable_with_discord(
        (psycopg.OperationalError,),
        discord_config,
        max_attempts=3
    )
    def batch_insert(self, table: str, data: list):
        """Batch insert with retry"""
        self.logger.info(f"Starting batch insert to {table}: {len(data)} records")
        
        if not data:
            self.logger.warning("No data to insert")
            return
        
        # Prepare insert query
        columns = list(data[0].keys())
        placeholders = ", ".join([f"%({col})s" for col in columns])
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Execute batch insert
        for record in data:
            self.connector.execute(query, record)
        
        self.logger.info(f"Batch insert completed: {len(data)} records")
    
    def health_check(self):
        """Check database health"""
        try:
            if self.connector.health_check():
                self.logger.info("Database health check passed")
                return True
            else:
                self.logger.error("Database health check failed")
                return False
        except Exception as e:
            self.logger.error(f"Database health check error: {e}")
            return False

# Sử dụng
db_manager = DatabaseManager("postgresql://user:pass@host:port/db")

# Health check
if db_manager.health_check():
    # Execute query
    results = db_manager.execute_query(
        "SELECT * FROM users WHERE active = %(active)s",
        {"active": True}
    )
    
    # Batch insert
    new_users = [
        {"name": "Alice", "email": "alice@example.com", "active": True},
        {"name": "Bob", "email": "bob@example.com", "active": True}
    ]
    db_manager.batch_insert("users", new_users)
```

### 3. API Client với Retry

```python
import requests
from ami_helpers.utils.retry import retriable
from ami_helpers.utils.log_info import setup_logging
import logging

class APIClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key
        self.logger = logging.getLogger(__name__)
        
        # Setup session
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        })
    
    @retriable(
        (requests.exceptions.ConnectionError, 
         requests.exceptions.Timeout,
         requests.exceptions.HTTPError),
        max_attempts=3,
        max_seconds=30,
        initial_wait=1.0,
        max_wait=10.0
    )
    def get(self, endpoint: str, params: dict = None):
        """GET request with retry"""
        url = f"{self.base_url}/{endpoint}"
        self.logger.info(f"Making GET request to {url}")
        
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        self.logger.info(f"GET request successful: {response.status_code}")
        return response.json()
    
    @retriable(
        (requests.exceptions.ConnectionError,
         requests.exceptions.Timeout,
         requests.exceptions.HTTPError),
        max_attempts=3,
        max_seconds=30
    )
    def post(self, endpoint: str, data: dict):
        """POST request with retry"""
        url = f"{self.base_url}/{endpoint}"
        self.logger.info(f"Making POST request to {url}")
        
        response = self.session.post(url, json=data, timeout=30)
        response.raise_for_status()
        
        self.logger.info(f"POST request successful: {response.status_code}")
        return response.json()
    
    def get_user(self, user_id: int):
        """Get user by ID"""
        return self.get(f"users/{user_id}")
    
    def create_user(self, user_data: dict):
        """Create new user"""
        return self.post("users", user_data)

# Sử dụng
if __name__ == "__main__":
    setup_logging(level="INFO")
    
    client = APIClient("https://api.example.com", "your-api-key")
    
    try:
        # Get user
        user = client.get_user(123)
        print(f"User: {user}")
        
        # Create user
        new_user = client.create_user({
            "name": "John Doe",
            "email": "john@example.com"
        })
        print(f"Created user: {new_user}")
        
    except Exception as e:
        print(f"API operation failed: {e}")
```

## Best Practices

### 1. Logging Best Practices

```python
# Sử dụng structured logging
logger.info("Processing user %s with %d records", user_id, record_count)

# Sử dụng appropriate log levels
logger.debug("Variable values: %s", locals())  # Debug info
logger.info("Operation completed successfully")  # General info
logger.warning("High memory usage: %d%%", memory_percent)  # Warning
logger.error("Operation failed: %s", str(exception))  # Error

# Log exceptions properly
try:
    risky_operation()
except Exception as e:
    logger.error("Operation failed: %s", str(e), exc_info=True)
```

### 2. Retry Best Practices

```python
# Chỉ retry các lỗi có thể recover
@retriable(
    (ConnectionError, TimeoutError),  # Chỉ retry network errors
    max_attempts=3
)
def network_operation():
    pass

# Không retry các lỗi logic
def validate_data(data):
    if not data:
        raise ValueError("Data cannot be empty")  # Không retry lỗi này

# Sử dụng appropriate retry parameters
@retriable(
    (ConnectionError,),
    max_attempts=5,      # Nhiều lần thử cho network operations
    max_seconds=60,      # Timeout dài cho network operations
    initial_wait=1.0,    # Bắt đầu với 1 giây
    max_wait=10.0        # Tối đa 10 giây giữa các lần thử
)
def network_operation():
    pass
```

### 3. Error Handling

```python
# Kết hợp retry với proper error handling
def robust_operation():
    try:
        return risky_operation()
    except RetryError as e:
        logger.error("Operation failed after all retries: %s", e)
        # Fallback logic hoặc cleanup
        return None
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        raise
```

## Troubleshooting

### Lỗi thường gặp

#### 1. Logging không hiển thị

```python
# Kiểm tra log level
logger = logging.getLogger(__name__)
print(f"Logger level: {logger.level}")
print(f"Root logger level: {logging.getLogger().level}")

# Bật debug logging
logging.basicConfig(level=logging.DEBUG)
```

#### 2. Retry không hoạt động

```python
# Kiểm tra exception types
try:
    operation()
except Exception as e:
    print(f"Exception type: {type(e)}")
    print(f"Exception: {e}")

# Đảm bảo exception được catch đúng
@retriable(
    (ValueError,),  # Chỉ catch ValueError
    max_attempts=3
)
def operation():
    raise TypeError("Wrong exception type")  # Sẽ không retry
```

#### 3. Discord notifications không gửi

```python
# Test webhook connection
from ami_helpers.alerts.alert_engines.discord_alerts import send_discord_alert, DiscordAlertConfig

config = DiscordAlertConfig(webhook_url="your-webhook-url")
result = send_discord_alert(
    config,
    title="Test",
    description="Test message",
    fields={"Status": "Test"},
    level="info"
)
print(f"Discord alert result: {result}")
```

### Debug Tips

```python
# Enable debug logging for retry
import logging
logging.basicConfig(level=logging.DEBUG)

# Log retry attempts
@retriable(
    (Exception,),
    max_attempts=3
)
def debug_operation():
    print("Operation executed")
    raise Exception("Test error")

# Monitor retry behavior
debug_operation()
```
