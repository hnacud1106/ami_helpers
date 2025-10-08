# Alert System - Hệ thống cảnh báo Discord

## Tổng quan

Module `alerts` cung cấp hệ thống cảnh báo mạnh mẽ thông qua Discord webhooks. Hệ thống hỗ trợ:

- **Discord Notifications**: Gửi cảnh báo qua Discord webhook
- **Rich Embeds**: Hiển thị thông tin chi tiết với màu sắc và fields
- **Retry Integration**: Tích hợp với retry mechanism
- **Error Tracking**: Theo dõi và báo cáo lỗi chi tiết

## Cài đặt và cấu hình

### Dependencies

```
requests==2.32.5
```

### Tạo Discord Webhook

1. Truy cập Discord server của bạn
2. Vào Server Settings → Integrations → Webhooks
3. Tạo webhook mới và copy URL

### Cấu hình cơ bản

```python
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig, send_discord_alert

# Cấu hình Discord webhook
discord_config = DiscordAlertConfig(
    webhook_url="https://discord.com/api/webhooks/YOUR_WEBHOOK_URL",
    username="Data Pipeline Bot",
    avatar_url="https://example.com/bot-avatar.png",
    timeout_s=5,
    environment="production",
    service="data-pipeline",
    include_args=True,
    include_traceback=True
)
```

## Chi tiết các tham số DiscordAlertConfig

### 1. **webhook_url** (Bắt buộc)
```python
# Discord webhook URL format
webhook_url="https://discord.com/api/webhooks/WEBHOOK_ID/WEBHOOK_TOKEN"

# Ví dụ thực tế
webhook_url="https://discord.com/api/webhooks/1234567890123456789/abcdefghijklmnopqrstuvwxyz1234567890"
```

**Mục đích**: URL webhook để gửi messages đến Discord channel
**Cách lấy**:
1. Vào Discord server → Server Settings → Integrations → Webhooks
2. Tạo webhook mới hoặc copy URL từ webhook có sẵn
3. Paste URL vào tham số này

### 2. **username** (Optional)
```python
# Bot username hiển thị trong Discord
username="Data Pipeline Bot"
username="ETL Monitor"
username="System Health Bot"
username="Analytics Alert Bot"
```

**Mục đích**: Tên hiển thị của bot khi gửi messages
**Cách đặt tên**: Sử dụng tên mô tả rõ ràng về chức năng của bot

### 3. **avatar_url** (Optional)
```python
# URL avatar cho bot
avatar_url="https://example.com/bot-avatar.png"
avatar_url="https://cdn.discordapp.com/avatars/bot-id/avatar.png"
avatar_url="https://i.imgur.com/avatar.png"
```

**Mục đích**: Avatar image hiển thị cùng với messages
**Yêu cầu**: URL phải accessible từ internet, format PNG/JPG/GIF

### 4. **timeout_s** (Mặc định: 5)
```python
# Timeout ngắn cho local network
timeout_s=3

# Timeout mặc định
timeout_s=5

# Timeout dài cho network không ổn định
timeout_s=10

# Timeout rất dài cho debugging
timeout_s=30
```

**Mục đích**: Thời gian chờ tối đa khi gửi request đến Discord
**Cách chọn**:
- **Local network**: 3-5 giây
- **Internet**: 5-10 giây
- **Unstable network**: 10-30 giây

### 5. **environment** (Optional)
```python
# Môi trường deployment
environment="production"
environment="staging"
environment="development"
environment="testing"
environment="local"
```

**Mục đích**: Xác định môi trường để phân biệt alerts
**Cách sử dụng**: Thêm vào title và fields để dễ phân biệt alerts từ các môi trường khác nhau

### 6. **service** (Optional)
```python
# Tên service gửi alert
service="data-pipeline"
service="etl-processor"
service="user-service"
service="payment-gateway"
service="analytics-engine"
```

**Mục đích**: Xác định service nào gửi alert
**Cách sử dụng**: Thêm vào fields để tracking và debugging

### 7. **include_args** (Mặc định: True)
```python
# Bao gồm function arguments trong alert
include_args=True

# Không bao gồm arguments (privacy/security)
include_args=False
```

**Mục đích**: Quyết định có bao gồm function arguments trong alert hay không
**Khi nào dùng**:
- **True**: Debugging, development
- **False**: Production với sensitive data

### 8. **include_traceback** (Mặc định: True)
```python
# Bao gồm stack trace trong alert
include_traceback=True

# Không bao gồm stack trace
include_traceback=False
```

**Mục đích**: Quyết định có bao gồm stack trace trong alert hay không
**Khi nào dùng**:
- **True**: Debugging, development
- **False**: Production để giảm noise

## Ví dụ cấu hình theo use case

### 1. **Production Environment**
```python
# Production - Security focused
prod_config = DiscordAlertConfig(
    webhook_url="https://discord.com/api/webhooks/PROD_WEBHOOK_URL",
    username="Production Monitor",
    avatar_url="https://example.com/prod-bot.png",
    timeout_s=5,
    environment="production",
    service="data-pipeline",
    include_args=False,      # Không include sensitive data
    include_traceback=True   # Cần traceback để debug
)
```

### 2. **Development Environment**
```python
# Development - Debug focused
dev_config = DiscordAlertConfig(
    webhook_url="https://discord.com/api/webhooks/DEV_WEBHOOK_URL",
    username="Dev Debug Bot",
    avatar_url="https://example.com/dev-bot.png",
    timeout_s=10,           # Timeout dài hơn cho debugging
    environment="development",
    service="etl-processor",
    include_args=True,       # Include args để debug
    include_traceback=True   # Include traceback để debug
)
```

### 3. **Staging Environment**
```python
# Staging - Balanced
staging_config = DiscordAlertConfig(
    webhook_url="https://discord.com/api/webhooks/STAGING_WEBHOOK_URL",
    username="Staging Monitor",
    avatar_url="https://example.com/staging-bot.png",
    timeout_s=5,
    environment="staging",
    service="analytics-engine",
    include_args=True,       # Include args để test
    include_traceback=True   # Include traceback để debug
)
```

### 4. **Multiple Services**
```python
# Service-specific configurations
class AlertManager:
    def __init__(self):
        self.configs = {
            "etl": DiscordAlertConfig(
                webhook_url="https://discord.com/api/webhooks/ETL_WEBHOOK_URL",
                username="ETL Monitor",
                environment="production",
                service="etl-pipeline"
            ),
            "api": DiscordAlertConfig(
                webhook_url="https://discord.com/api/webhooks/API_WEBHOOK_URL",
                username="API Monitor",
                environment="production",
                service="api-gateway"
            ),
            "database": DiscordAlertConfig(
                webhook_url="https://discord.com/api/webhooks/DB_WEBHOOK_URL",
                username="Database Monitor",
                environment="production",
                service="database-service"
            )
        }
    
    def send_alert(self, service: str, title: str, description: str, fields: dict, level: str = "info"):
        config = self.configs.get(service)
        if config:
            return send_discord_alert(config, title, description, fields, level)
        return False
```

## Alert Levels và Colors

### 1. **Info Level** (Màu xanh)
```python
# Thông tin chung, không cần action
send_discord_alert(
    config,
    title="ℹ️ Pipeline Started",
    description="ETL pipeline started successfully",
    fields={"Status": "RUNNING", "Start Time": "2024-01-01 10:00:00"},
    level="info"
)
```

**Khi nào dùng**: Thông tin chung, status updates, successful operations

### 2. **Warning Level** (Màu vàng)
```python
# Cảnh báo, cần chú ý nhưng không critical
send_discord_alert(
    config,
    title="⚠️ High Memory Usage",
    description="Memory usage is above threshold",
    fields={"Memory Usage": "85%", "Threshold": "80%"},
    level="warning"
)
```

**Khi nào dùng**: Performance issues, resource usage cao, non-critical errors

### 3. **Error Level** (Màu đỏ)
```python
# Lỗi nghiêm trọng, cần action ngay
send_discord_alert(
    config,
    title="🚨 Pipeline Failed",
    description="ETL pipeline failed with critical error",
    fields={"Error": "Connection timeout", "Status": "FAILED"},
    level="error"
)
```

**Khi nào dùng**: Critical errors, system failures, data loss

## Rich Embeds và Fields

### 1. **Basic Fields**
```python
# Fields đơn giản
fields = {
    "Status": "SUCCESS",
    "Records Processed": "1,000,000",
    "Duration": "15 minutes",
    "Memory Used": "2.5GB"
}

send_discord_alert(config, title, description, fields, "info")
```

### 2. **Advanced Fields**
```python
# Fields với formatting
fields = {
    "📊 **Statistics**": "",  # Header field
    "Total Records": f"{total_records:,}",
    "Success Rate": f"{success_rate:.2f}%",
    "Average Processing Time": f"{avg_time:.2f}s",
    "",
    "🔧 **Configuration**": "",  # Another header
    "Batch Size": f"{batch_size:,}",
    "Parallel Workers": str(workers),
    "Memory Limit": f"{memory_limit}GB"
}

send_discord_alert(config, title, description, fields, "info")
```

### 3. **Dynamic Fields**
```python
def create_alert_fields(data: dict) -> dict:
    """Tạo fields động từ data"""
    fields = {}
    
    # Add basic info
    fields["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fields["Environment"] = data.get("environment", "unknown")
    
    # Add metrics
    if "metrics" in data:
        metrics = data["metrics"]
        fields["CPU Usage"] = f"{metrics.get('cpu', 0):.1f}%"
        fields["Memory Usage"] = f"{metrics.get('memory', 0):.1f}%"
        fields["Disk Usage"] = f"{metrics.get('disk', 0):.1f}%"
    
    # Add errors if any
    if "errors" in data:
        fields["Error Count"] = str(len(data["errors"]))
        fields["Last Error"] = data["errors"][-1] if data["errors"] else "None"
    
    return fields

# Sử dụng
data = {
    "environment": "production",
    "metrics": {"cpu": 75.5, "memory": 80.2, "disk": 45.1},
    "errors": ["Connection timeout", "Retry failed"]
}

fields = create_alert_fields(data)
send_discord_alert(config, "System Status", "Current system metrics", fields, "warning")
```

## Message Truncation và Limits

### 1. **Discord Limits**
```python
# Discord message limits
MAX_TITLE_LENGTH = 256
MAX_DESCRIPTION_LENGTH = 4096
MAX_FIELD_NAME_LENGTH = 256
MAX_FIELD_VALUE_LENGTH = 1024
MAX_FIELDS_PER_EMBED = 25
```

### 2. **Safe Truncation**
```python
def safe_send_alert(config: DiscordAlertConfig, title: str, description: str, fields: dict, level: str = "info"):
    """Gửi alert với truncation an toàn"""
    
    # Truncate title
    if len(title) > 256:
        title = title[:253] + "..."
    
    # Truncate description
    if len(description) > 4096:
        description = description[:4093] + "..."
    
    # Truncate fields
    truncated_fields = {}
    for key, value in fields.items():
        # Truncate field name
        if len(key) > 256:
            key = key[:253] + "..."
        
        # Truncate field value
        if len(str(value)) > 1024:
            value = str(value)[:1021] + "..."
        
        truncated_fields[key] = str(value)
    
    # Limit number of fields
    if len(truncated_fields) > 25:
        truncated_fields = dict(list(truncated_fields.items())[:25])
    
    return send_discord_alert(config, title, description, truncated_fields, level)
```

### 3. **Batch Alerts**
```python
def send_batch_alerts(config: DiscordAlertConfig, alerts: list):
    """Gửi nhiều alerts trong batch"""
    for alert in alerts:
        try:
            send_discord_alert(
                config,
                alert["title"],
                alert["description"],
                alert["fields"],
                alert.get("level", "info")
            )
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"Failed to send alert: {e}")
```

## Error Handling và Retry

### 1. **Basic Error Handling**
```python
def safe_send_alert_with_retry(config: DiscordAlertConfig, title: str, description: str, fields: dict, level: str = "info", max_retries: int = 3):
    """Gửi alert với retry logic"""
    
    for attempt in range(max_retries):
        try:
            result = send_discord_alert(config, title, description, fields, level)
            if result:
                return True
        except Exception as e:
            print(f"Alert send attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
    
    print(f"Failed to send alert after {max_retries} attempts")
    return False
```

### 2. **Fallback Alert System**
```python
class FallbackAlertManager:
    def __init__(self, primary_config: DiscordAlertConfig, fallback_config: DiscordAlertConfig):
        self.primary_config = primary_config
        self.fallback_config = fallback_config
    
    def send_alert(self, title: str, description: str, fields: dict, level: str = "info"):
        # Try primary webhook first
        try:
            result = send_discord_alert(self.primary_config, title, description, fields, level)
            if result:
                return True
        except Exception as e:
            print(f"Primary webhook failed: {e}")
        
        # Try fallback webhook
        try:
            result = send_discord_alert(self.fallback_config, title, description, fields, level)
            if result:
                print("Alert sent via fallback webhook")
                return True
        except Exception as e:
            print(f"Fallback webhook also failed: {e}")
        
        return False
```

## Monitoring và Analytics

### 1. **Alert Statistics**
```python
class AlertStats:
    def __init__(self):
        self.stats = {
            "total_sent": 0,
            "successful": 0,
            "failed": 0,
            "by_level": {"info": 0, "warning": 0, "error": 0},
            "by_service": {}
        }
    
    def record_alert(self, service: str, level: str, success: bool):
        self.stats["total_sent"] += 1
        if success:
            self.stats["successful"] += 1
        else:
            self.stats["failed"] += 1
        
        self.stats["by_level"][level] += 1
        self.stats["by_service"][service] = self.stats["by_service"].get(service, 0) + 1
    
    def get_stats(self):
        return self.stats.copy()
    
    def get_success_rate(self):
        if self.stats["total_sent"] == 0:
            return 0
        return (self.stats["successful"] / self.stats["total_sent"]) * 100

# Sử dụng
stats = AlertStats()

def monitored_send_alert(config: DiscordAlertConfig, service: str, title: str, description: str, fields: dict, level: str = "info"):
    try:
        result = send_discord_alert(config, title, description, fields, level)
        stats.record_alert(service, level, result)
        return result
    except Exception as e:
        stats.record_alert(service, level, False)
        raise e
```

### 2. **Alert Rate Limiting**
```python
class RateLimitedAlertManager:
    def __init__(self, config: DiscordAlertConfig, max_alerts_per_minute: int = 10):
        self.config = config
        self.max_alerts = max_alerts_per_minute
        self.alert_times = defaultdict(list)
    
    def send_alert(self, title: str, description: str, fields: dict, level: str = "info"):
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

## Sử dụng cơ bản

### Gửi cảnh báo đơn giản

```python
from ami_helpers.alerts.alert_engines.discord_alerts import send_discord_alert, DiscordAlertConfig

# Cấu hình
config = DiscordAlertConfig(
    webhook_url="your-webhook-url",
    username="Pipeline Bot"
)

# Gửi cảnh báo thành công
send_discord_alert(
    config,
    title="✅ Pipeline Completed",
    description="Data processing job finished successfully",
    fields={
        "Records Processed": "1,000,000",
        "Duration": "15 minutes",
        "Status": "SUCCESS"
    },
    level="info"
)

# Gửi cảnh báo lỗi
send_discord_alert(
    config,
    title="❌ Pipeline Failed",
    description="Data processing job encountered an error",
    fields={
        "Error": "Connection timeout",
        "Retry Attempts": "3",
        "Status": "FAILED"
    },
    level="error"
)
```

### Các loại cảnh báo

```python
# Info alert (màu xanh)
send_discord_alert(
    config,
    title="ℹ️ Information",
    description="Process started",
    fields={"Status": "Running"},
    level="info"
)

# Warning alert (màu vàng)
send_discord_alert(
    config,
    title="⚠️ Warning",
    description="High memory usage detected",
    fields={"Memory Usage": "85%", "Threshold": "80%"},
    level="warning"
)

# Error alert (màu đỏ)
send_discord_alert(
    config,
    title="🚨 Error",
    description="Critical error occurred",
    fields={"Error Code": "500", "Service": "API"},
    level="error"
)
```

## Tích hợp với Retry Mechanism

### Retry với Discord notifications

```python
from ami_helpers.utils.retry import retriable_with_discord
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig

# Cấu hình Discord
discord_config = DiscordAlertConfig(
    webhook_url="your-webhook-url",
    environment="production",
    service="database-service"
)

@retriable_with_discord(
    (ConnectionError, TimeoutError),  # Các exception sẽ retry
    discord_config,
    max_attempts=3,                   # Số lần retry tối đa
    max_seconds=60,                   # Thời gian retry tối đa (giây)
    notify_every_retry=True,          # Gửi thông báo mỗi lần retry
    reraise=True                      # Re-raise exception sau khi hết retry
)
def risky_database_operation():
    # Code có thể gây lỗi
    import random
    if random.random() < 0.7:  # 70% chance of failure
        raise ConnectionError("Database connection failed")
    return "Success"

# Sử dụng function
try:
    result = risky_database_operation()
    print(f"Operation result: {result}")
except Exception as e:
    print(f"Operation failed after retries: {e}")
```

### Retry đơn giản (không Discord)

```python
from ami_helpers.utils.retry import retriable

@retriable(
    (ConnectionError, TimeoutError),
    max_attempts=3,
    max_seconds=30
)
def simple_retry_function():
    # Code có thể gây lỗi
    pass
```

## Advanced Usage

### 1. ETL Pipeline với Monitoring

```python
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig, send_discord_alert
from ami_helpers.utils.retry import retriable_with_discord
import time
import logging

logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self):
        self.discord_config = DiscordAlertConfig(
            webhook_url="your-webhook-url",
            environment="production",
            service="etl-pipeline"
        )
    
    def send_start_notification(self):
        send_discord_alert(
            self.discord_config,
            title="🚀 ETL Pipeline Started",
            description="Data extraction and transformation process initiated",
            fields={
                "Start Time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "Environment": "production",
                "Status": "RUNNING"
            },
            level="info"
        )
    
    def send_progress_notification(self, stage, records_processed):
        send_discord_alert(
            self.discord_config,
            title="📊 ETL Progress Update",
            description=f"Pipeline stage '{stage}' completed",
            fields={
                "Stage": stage,
                "Records Processed": f"{records_processed:,}",
                "Timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            },
            level="info"
        )
    
    def send_success_notification(self, total_records, duration):
        send_discord_alert(
            self.discord_config,
            title="✅ ETL Pipeline Completed",
            description="Data processing completed successfully",
            fields={
                "Total Records": f"{total_records:,}",
                "Duration": f"{duration:.2f} minutes",
                "Status": "SUCCESS"
            },
            level="info"
        )
    
    def send_error_notification(self, error, stage):
        send_discord_alert(
            self.discord_config,
            title="❌ ETL Pipeline Failed",
            description=f"Pipeline failed at stage '{stage}'",
            fields={
                "Error": str(error),
                "Failed Stage": stage,
                "Timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "Status": "FAILED"
            },
            level="error"
        )
    
    @retriable_with_discord(
        (ConnectionError, TimeoutError),
        discord_config,
        max_attempts=3,
        notify_every_retry=True
    )
    def extract_data(self):
        # Simulate data extraction
        logger.info("Extracting data...")
        time.sleep(2)
        return 1000000
    
    @retriable_with_discord(
        (ValueError, KeyError),
        discord_config,
        max_attempts=2
    )
    def transform_data(self, data):
        # Simulate data transformation
        logger.info("Transforming data...")
        time.sleep(3)
        return data * 0.8  # Simulate some data loss
    
    def run_pipeline(self):
        try:
            self.send_start_notification()
            
            # Extract
            raw_data = self.extract_data()
            self.send_progress_notification("EXTRACT", raw_data)
            
            # Transform
            processed_data = self.transform_data(raw_data)
            self.send_progress_notification("TRANSFORM", processed_data)
            
            # Load (simulate)
            logger.info("Loading data...")
            time.sleep(1)
            
            duration = 6  # Simulated duration
            self.send_success_notification(processed_data, duration)
            
        except Exception as e:
            self.send_error_notification(e, "UNKNOWN")
            raise

# Sử dụng
if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run_pipeline()
```

### 2. System Health Monitoring

```python
import psutil
import time
from datetime import datetime

class SystemMonitor:
    def __init__(self):
        self.discord_config = DiscordAlertConfig(
            webhook_url="your-webhook-url",
            environment="production",
            service="system-monitor"
        )
        self.thresholds = {
            "cpu_percent": 80,
            "memory_percent": 85,
            "disk_percent": 90
        }
    
    def check_system_health(self):
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        alerts = []
        
        # Check CPU
        if cpu_percent > self.thresholds["cpu_percent"]:
            alerts.append({
                "type": "warning",
                "title": "⚠️ High CPU Usage",
                "description": f"CPU usage is {cpu_percent:.1f}%",
                "fields": {
                    "CPU Usage": f"{cpu_percent:.1f}%",
                    "Threshold": f"{self.thresholds['cpu_percent']}%",
                    "Status": "WARNING"
                }
            })
        
        # Check Memory
        if memory.percent > self.thresholds["memory_percent"]:
            alerts.append({
                "type": "warning",
                "title": "⚠️ High Memory Usage",
                "description": f"Memory usage is {memory.percent:.1f}%",
                "fields": {
                    "Memory Usage": f"{memory.percent:.1f}%",
                    "Available": f"{memory.available / (1024**3):.1f} GB",
                    "Threshold": f"{self.thresholds['memory_percent']}%",
                    "Status": "WARNING"
                }
            })
        
        # Check Disk
        if disk.percent > self.thresholds["disk_percent"]:
            alerts.append({
                "type": "error",
                "title": "🚨 High Disk Usage",
                "description": f"Disk usage is {disk.percent:.1f}%",
                "fields": {
                    "Disk Usage": f"{disk.percent:.1f}%",
                    "Free Space": f"{disk.free / (1024**3):.1f} GB",
                    "Threshold": f"{self.thresholds['disk_percent']}%",
                    "Status": "CRITICAL"
                }
            })
        
        # Send alerts
        for alert in alerts:
            send_discord_alert(
                self.discord_config,
                alert["title"],
                alert["description"],
                alert["fields"],
                level=alert["type"]
            )
        
        # Send health check if no alerts
        if not alerts:
            send_discord_alert(
                self.discord_config,
                title="✅ System Health Check",
                description="All system metrics are within normal ranges",
                fields={
                    "CPU Usage": f"{cpu_percent:.1f}%",
                    "Memory Usage": f"{memory.percent:.1f}%",
                    "Disk Usage": f"{disk.percent:.1f}%",
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Status": "HEALTHY"
                },
                level="info"
            )

# Chạy monitoring
monitor = SystemMonitor()
monitor.check_system_health()
```

### 3. Database Monitoring

```python
from ami_helpers.database_engines import create_connector, DBConfig
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig, send_discord_alert

class DatabaseMonitor:
    def __init__(self):
        self.discord_config = DiscordAlertConfig(
            webhook_url="your-webhook-url",
            environment="production",
            service="database-monitor"
        )
        
        # Cấu hình database
        self.db_config = DBConfig(
            dsn="postgresql://user:pass@host:port/db",
            max_pool_size=5
        )
        self.connector = create_connector(self.db_config)
    
    def check_database_health(self):
        try:
            # Kiểm tra kết nối
            if not self.connector.health_check():
                send_discord_alert(
                    self.discord_config,
                    title="🚨 Database Connection Failed",
                    description="Cannot connect to database",
                    fields={
                        "Database": "PostgreSQL",
                        "Status": "DOWN",
                        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    },
                    level="error"
                )
                return
            
            # Kiểm tra số lượng connections
            connections = self.connector.fetchall("""
                SELECT count(*) as active_connections 
                FROM pg_stat_activity 
                WHERE state = 'active'
            """)[0][0]
            
            # Kiểm tra database size
            db_size = self.connector.fetchall("""
                SELECT pg_size_pretty(pg_database_size(current_database())) as size
            """)[0][0]
            
            # Kiểm tra slow queries
            slow_queries = self.connector.fetchall("""
                SELECT count(*) as slow_queries
                FROM pg_stat_activity 
                WHERE state = 'active' 
                AND query_start < now() - interval '5 minutes'
            """)[0][0]
            
            # Gửi báo cáo
            send_discord_alert(
                self.discord_config,
                title="📊 Database Health Report",
                description="Database monitoring report",
                fields={
                    "Active Connections": str(connections),
                    "Database Size": db_size,
                    "Slow Queries": str(slow_queries),
                    "Status": "HEALTHY",
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                },
                level="info"
            )
            
        except Exception as e:
            send_discord_alert(
                self.discord_config,
                title="❌ Database Monitor Error",
                description="Error occurred during database monitoring",
                fields={
                    "Error": str(e),
                    "Status": "ERROR",
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                },
                level="error"
            )

# Sử dụng
monitor = DatabaseMonitor()
monitor.check_database_health()
```

## Best Practices

### 1. Cấu hình Discord Webhook

```python
# Tạo cấu hình tái sử dụng
class AlertManager:
    def __init__(self, webhook_url: str, environment: str):
        self.config = DiscordAlertConfig(
            webhook_url=webhook_url,
            environment=environment,
            service="data-pipeline",
            include_args=True,
            include_traceback=True,
            timeout_s=10
        )
    
    def send_success(self, message: str, **fields):
        send_discord_alert(
            self.config,
            title="✅ Success",
            description=message,
            fields=fields,
            level="info"
        )
    
    def send_warning(self, message: str, **fields):
        send_discord_alert(
            self.config,
            title="⚠️ Warning",
            description=message,
            fields=fields,
            level="warning"
        )
    
    def send_error(self, message: str, **fields):
        send_discord_alert(
            self.config,
            title="❌ Error",
            description=message,
            fields=fields,
            level="error"
        )

# Sử dụng
alert_manager = AlertManager("webhook-url", "production")
alert_manager.send_success("Pipeline completed", records="1000", duration="5min")
```

### 2. Rate Limiting

```python
import time
from collections import defaultdict

class RateLimitedAlertManager:
    def __init__(self, config: DiscordAlertConfig, max_alerts_per_minute: int = 10):
        self.config = config
        self.max_alerts = max_alerts_per_minute
        self.alert_times = defaultdict(list)
    
    def send_alert(self, title: str, description: str, fields: dict, level: str = "info"):
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

### 3. Error Handling

```python
def safe_send_alert(config: DiscordAlertConfig, title: str, description: str, fields: dict, level: str = "info"):
    try:
        return send_discord_alert(config, title, description, fields, level)
    except Exception as e:
        # Log error but don't crash the application
        logger.error(f"Failed to send Discord alert: {e}")
        return False
```

## Troubleshooting

### Lỗi thường gặp

#### 1. Webhook URL không hợp lệ

```
requests.exceptions.HTTPError: 404 Client Error: Not Found
```

**Giải pháp:**
- Kiểm tra webhook URL có đúng không
- Kiểm tra webhook có bị xóa không
- Tạo webhook mới

#### 2. Rate limit exceeded

```
requests.exceptions.HTTPError: 429 Too Many Requests
```

**Giải pháp:**
- Implement rate limiting
- Giảm tần suất gửi alert
- Sử dụng batch notifications

#### 3. Message quá dài

```
requests.exceptions.HTTPError: 400 Bad Request
```

**Giải pháp:**
- Truncate message content
- Sử dụng `_safe_truncate` function
- Chia nhỏ message thành nhiều phần

### Debug Tips

```python
# Test webhook connection
def test_webhook(config: DiscordAlertConfig):
    try:
        result = send_discord_alert(
            config,
            title="Test Alert",
            description="Testing webhook connection",
            fields={"Status": "Test"},
            level="info"
        )
        print(f"Webhook test result: {result}")
    except Exception as e:
        print(f"Webhook test failed: {e}")

# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)
```
