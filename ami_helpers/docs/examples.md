# Examples - Các ví dụ sử dụng thực tế

## Tổng quan

Tài liệu này cung cấp các ví dụ thực tế về cách sử dụng thư viện AMI Helpers trong các tình huống khác nhau. Các ví dụ được thiết kế để giúp bạn hiểu cách tích hợp và sử dụng các module một cách hiệu quả.

## 1. ETL Pipeline hoàn chỉnh

### Ví dụ: Xử lý dữ liệu bán hàng

```python
"""
ETL Pipeline: Xử lý dữ liệu bán hàng từ PostgreSQL sang ClickHouse
với monitoring và error handling
"""

from ami_helpers.spark_hive_engines import make_engine
from ami_helpers.database_engines import create_connector, DBConfig
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig, send_discord_alert
from ami_helpers.utils.retry import retriable_with_discord
from ami_helpers.utils.log_info import setup_logging
import logging
import time
from datetime import datetime, timedelta

class SalesETLPipeline:
    def __init__(self):
        # Setup logging
        setup_logging(level="INFO", logfile="sales_etl.log")
        self.logger = logging.getLogger(__name__)
        
        # Discord configuration
        self.discord_config = DiscordAlertConfig(
            webhook_url="your-discord-webhook-url",
            environment="production",
            service="sales-etl-pipeline"
        )
        
        # Database configurations
        self.pg_config = DBConfig(
            dsn="postgresql://user:pass@pg-host:5432/sales_db",
            max_pool_size=10
        )
        self.pg_connector = create_connector(self.pg_config)
        
        self.ch_config = DBConfig(
            dsn="http://clickhouse-host:8123",
            ch_database="analytics",
            ch_user="analytics_user",
            ch_password="password",
            max_pool_size=15
        )
        self.ch_connector = create_connector(self.ch_config)
    
    def send_notification(self, title: str, description: str, fields: dict, level: str = "info"):
        """Send Discord notification"""
        send_discord_alert(
            self.discord_config,
            title=title,
            description=description,
            fields=fields,
            level=level
        )
    
    @retriable_with_discord(
        (Exception,),
        discord_config,
        max_attempts=3,
        notify_every_retry=True
    )
    def extract_sales_data(self, start_date: str, end_date: str):
        """Extract sales data from PostgreSQL"""
        self.logger.info(f"Extracting sales data from {start_date} to {end_date}")
        
        query = """
            SELECT 
                s.id,
                s.customer_id,
                s.product_id,
                s.quantity,
                s.unit_price,
                s.total_amount,
                s.sale_date,
                s.created_at,
                c.name as customer_name,
                c.segment as customer_segment,
                p.name as product_name,
                p.category as product_category
            FROM sales s
            LEFT JOIN customers c ON s.customer_id = c.id
            LEFT JOIN products p ON s.product_id = p.id
            WHERE s.sale_date BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY s.sale_date
        """
        
        data = self.pg_connector.fetchall(query, {
            "start_date": start_date,
            "end_date": end_date
        })
        
        self.logger.info(f"Extracted {len(data)} sales records")
        return data
    
    def transform_sales_data(self, raw_data):
        """Transform sales data"""
        self.logger.info("Transforming sales data")
        
        transformed_data = []
        for record in raw_data:
            # Calculate derived fields
            total_amount = record[5]  # total_amount
            quantity = record[3]      # quantity
            unit_price = record[4]    # unit_price
            
            # Validate data
            if total_amount != quantity * unit_price:
                self.logger.warning(f"Data validation failed for record {record[0]}")
                continue
            
            # Create transformed record
            transformed_record = {
                'sale_id': record[0],
                'customer_id': record[1],
                'product_id': record[2],
                'quantity': quantity,
                'unit_price': unit_price,
                'total_amount': total_amount,
                'sale_date': record[6].strftime('%Y-%m-%d'),
                'created_at': record[7].strftime('%Y-%m-%d %H:%M:%S'),
                'customer_name': record[8],
                'customer_segment': record[9],
                'product_name': record[10],
                'product_category': record[11],
                'year': record[6].year,
                'month': record[6].month,
                'day': record[6].day,
                'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            transformed_data.append(transformed_record)
        
        self.logger.info(f"Transformed {len(transformed_data)} records")
        return transformed_data
    
    @retriable_with_discord(
        (Exception,),
        discord_config,
        max_attempts=3
    )
    def load_sales_data(self, data):
        """Load sales data to ClickHouse"""
        self.logger.info(f"Loading {len(data)} records to ClickHouse")
        
        # Create table if not exists
        create_table_query = """
            CREATE TABLE IF NOT EXISTS analytics.sales_fact (
                sale_id UInt64,
                customer_id UInt32,
                product_id UInt32,
                quantity UInt16,
                unit_price Decimal(10,2),
                total_amount Decimal(12,2),
                sale_date Date,
                created_at DateTime,
                customer_name String,
                customer_segment String,
                product_name String,
                product_category String,
                year UInt16,
                month UInt8,
                day UInt8,
                processed_at DateTime
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(sale_date)
            ORDER BY (sale_date, customer_id, product_id)
        """
        
        self.ch_connector.execute(create_table_query)
        
        # Insert data
        for record in data:
            insert_query = """
                INSERT INTO analytics.sales_fact VALUES (
                    %(sale_id)s, %(customer_id)s, %(product_id)s,
                    %(quantity)s, %(unit_price)s, %(total_amount)s,
                    %(sale_date)s, %(created_at)s, %(customer_name)s,
                    %(customer_segment)s, %(product_name)s, %(product_category)s,
                    %(year)s, %(month)s, %(day)s, %(processed_at)s
                )
            """
            self.ch_connector.execute(insert_query, record)
        
        self.logger.info("Data loaded successfully to ClickHouse")
    
    def run_pipeline(self, days_back: int = 7):
        """Run complete ETL pipeline"""
        try:
            start_time = time.time()
            
            # Calculate date range
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days_back)
            
            self.send_notification(
                "🚀 Sales ETL Pipeline Started",
                f"Processing sales data from {start_date} to {end_date}",
                {
                    "Start Date": str(start_date),
                    "End Date": str(end_date),
                    "Days Back": str(days_back),
                    "Status": "RUNNING"
                },
                "info"
            )
            
            # Extract
            raw_data = self.extract_sales_data(str(start_date), str(end_date))
            
            if not raw_data:
                self.send_notification(
                    "⚠️ No Data Found",
                    "No sales data found for the specified date range",
                    {"Status": "WARNING"},
                    "warning"
                )
                return
            
            # Transform
            transformed_data = self.transform_sales_data(raw_data)
            
            # Load
            self.load_sales_data(transformed_data)
            
            # Calculate metrics
            duration = time.time() - start_time
            total_amount = sum(record['total_amount'] for record in transformed_data)
            unique_customers = len(set(record['customer_id'] for record in transformed_data))
            unique_products = len(set(record['product_id'] for record in transformed_data))
            
            self.send_notification(
                "✅ Sales ETL Pipeline Completed",
                "Data processing completed successfully",
                {
                    "Records Processed": f"{len(transformed_data):,}",
                    "Total Sales Amount": f"${total_amount:,.2f}",
                    "Unique Customers": f"{unique_customers:,}",
                    "Unique Products": f"{unique_products:,}",
                    "Duration": f"{duration:.2f} seconds",
                    "Status": "SUCCESS"
                },
                "info"
            )
            
            self.logger.info("ETL pipeline completed successfully")
            
        except Exception as e:
            duration = time.time() - start_time
            self.send_notification(
                "❌ Sales ETL Pipeline Failed",
                f"Pipeline failed after {duration:.2f} seconds",
                {
                    "Error": str(e),
                    "Duration": f"{duration:.2f} seconds",
                    "Status": "FAILED"
                },
                "error"
            )
            self.logger.error(f"ETL pipeline failed: {e}")
            raise

# Sử dụng
if __name__ == "__main__":
    pipeline = SalesETLPipeline()
    pipeline.run_pipeline(days_back=7)
```

## 2. Real-time Data Processing với Spark

### Ví dụ: Xử lý dữ liệu log real-time

```python
"""
Real-time Log Processing: Xử lý log files với Spark
và lưu kết quả vào Hive table
"""

from ami_helpers.spark_hive_engines import make_engine
from ami_helpers.utils.log_info import setup_logging
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig, send_discord_alert
import logging
import time
from datetime import datetime

class LogProcessor:
    def __init__(self):
        setup_logging(level="INFO", logfile="log_processor.log")
        self.logger = logging.getLogger(__name__)
        
        self.discord_config = DiscordAlertConfig(
            webhook_url="your-discord-webhook-url",
            environment="production",
            service="log-processor"
        )
    
    def process_logs(self, log_path: str, output_table: str):
        """Process log files with Spark"""
        try:
            with make_engine(
                kind="hive",
                app_name="log_processor",
                conf={
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                }
            ) as engine:
                
                self.logger.info(f"Processing logs from {log_path}")
                
                # Read log files
                log_df = engine.spark.read.text(log_path)
                
                # Parse log entries
                parsed_df = engine.sql("""
                    SELECT 
                        regexp_extract(value, '^([^\\s]+)', 1) as ip_address,
                        regexp_extract(value, '\\[([^\\]]+)\\]', 1) as timestamp,
                        regexp_extract(value, '\"([^\"]+)\"', 1) as request,
                        regexp_extract(value, '\\s(\\d{3})\\s', 1) as status_code,
                        regexp_extract(value, '\\s(\\d+)\\s', 1) as response_size,
                        regexp_extract(value, '\"([^\"]+)\"', 2) as user_agent,
                        current_timestamp() as processed_at
                    FROM log_data
                    WHERE value IS NOT NULL AND value != ''
                """)
                
                # Filter valid entries
                valid_df = parsed_df.filter(
                    "ip_address IS NOT NULL AND status_code IS NOT NULL"
                )
                
                # Calculate statistics
                stats_df = engine.sql("""
                    SELECT 
                        ip_address,
                        status_code,
                        COUNT(*) as request_count,
                        AVG(CAST(response_size AS INT)) as avg_response_size,
                        MAX(processed_at) as last_seen
                    FROM parsed_logs
                    GROUP BY ip_address, status_code
                """)
                
                # Write to Hive table
                engine.write_table(
                    stats_df,
                    output_table,
                    mode="overwrite",
                    partition_by=["status_code"]
                )
                
                # Get summary statistics
                total_requests = valid_df.count()
                unique_ips = valid_df.select("ip_address").distinct().count()
                error_requests = valid_df.filter("status_code >= '400'").count()
                
                # Send notification
                send_discord_alert(
                    self.discord_config,
                    "📊 Log Processing Completed",
                    f"Processed {total_requests:,} log entries",
                    {
                        "Total Requests": f"{total_requests:,}",
                        "Unique IPs": f"{unique_ips:,}",
                        "Error Requests": f"{error_requests:,}",
                        "Error Rate": f"{(error_requests/total_requests*100):.2f}%",
                        "Output Table": output_table,
                        "Status": "SUCCESS"
                    },
                    "info"
                )
                
                self.logger.info(f"Log processing completed: {total_requests} requests processed")
                
        except Exception as e:
            send_discord_alert(
                self.discord_config,
                "❌ Log Processing Failed",
                f"Error processing logs: {str(e)}",
                {"Error": str(e), "Status": "FAILED"},
                "error"
            )
            self.logger.error(f"Log processing failed: {e}")
            raise

# Sử dụng
if __name__ == "__main__":
    processor = LogProcessor()
    processor.process_logs("/path/to/logs/*.log", "analytics.log_stats")
```

## 3. Database Migration Tool

### Ví dụ: Migrate dữ liệu từ PostgreSQL sang ClickHouse

```python
"""
Database Migration Tool: Migrate dữ liệu từ PostgreSQL sang ClickHouse
với progress tracking và error handling
"""

from ami_helpers.database_engines import create_connector, DBConfig
from ami_helpers.utils.retry import retriable_with_discord
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig, send_discord_alert
from ami_helpers.utils.log_info import setup_logging
import logging
import time
from typing import List, Dict, Any

class DatabaseMigrator:
    def __init__(self, pg_dsn: str, ch_dsn: str):
        setup_logging(level="INFO", logfile="migration.log")
        self.logger = logging.getLogger(__name__)
        
        self.discord_config = DiscordAlertConfig(
            webhook_url="your-discord-webhook-url",
            environment="production",
            service="database-migrator"
        )
        
        # PostgreSQL configuration
        self.pg_config = DBConfig(
            dsn=pg_dsn,
            max_pool_size=5
        )
        self.pg_connector = create_connector(self.pg_config)
        
        # ClickHouse configuration
        self.ch_config = DBConfig(
            dsn=ch_dsn,
            ch_database="migrated_data",
            ch_user="migrator",
            ch_password="password",
            max_pool_size=10
        )
        self.ch_connector = create_connector(self.ch_config)
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Get table schema from PostgreSQL"""
        query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_name = %(table_name)s
            ORDER BY ordinal_position
        """
        
        return self.pg_connector.fetchall(query, {"table_name": table_name})
    
    def create_clickhouse_table(self, table_name: str, schema: List[Dict[str, Any]]):
        """Create ClickHouse table based on PostgreSQL schema"""
        columns = []
        
        for col in schema:
            col_name = col[0]
            data_type = col[1]
            is_nullable = col[2] == 'YES'
            
            # Map PostgreSQL types to ClickHouse types
            ch_type = self._map_data_type(data_type, is_nullable)
            columns.append(f"{col_name} {ch_type}")
        
        create_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            ) ENGINE = MergeTree()
            ORDER BY tuple()
        """
        
        self.ch_connector.execute(create_query)
        self.logger.info(f"Created ClickHouse table: {table_name}")
    
    def _map_data_type(self, pg_type: str, is_nullable: bool) -> str:
        """Map PostgreSQL data type to ClickHouse data type"""
        type_mapping = {
            'integer': 'Int32',
            'bigint': 'Int64',
            'smallint': 'Int16',
            'real': 'Float32',
            'double precision': 'Float64',
            'numeric': 'Decimal(10,2)',
            'boolean': 'UInt8',
            'character varying': 'String',
            'text': 'String',
            'timestamp without time zone': 'DateTime',
            'timestamp with time zone': 'DateTime',
            'date': 'Date',
            'uuid': 'String'
        }
        
        ch_type = type_mapping.get(pg_type, 'String')
        
        if is_nullable and ch_type not in ['String', 'Date', 'DateTime']:
            ch_type = f'Nullable({ch_type})'
        
        return ch_type
    
    @retriable_with_discord(
        (Exception,),
        discord_config,
        max_attempts=3,
        notify_every_retry=True
    )
    def migrate_table(self, table_name: str, batch_size: int = 1000):
        """Migrate table data from PostgreSQL to ClickHouse"""
        self.logger.info(f"Starting migration for table: {table_name}")
        
        # Get table schema
        schema = self.get_table_schema(table_name)
        
        # Create ClickHouse table
        self.create_clickhouse_table(table_name, schema)
        
        # Get total row count
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        total_rows = self.pg_connector.fetchall(count_query)[0][0]
        
        self.logger.info(f"Total rows to migrate: {total_rows:,}")
        
        # Migrate data in batches
        offset = 0
        migrated_rows = 0
        
        while offset < total_rows:
            # Fetch batch from PostgreSQL
            select_query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
            batch_data = self.pg_connector.fetchall(select_query)
            
            if not batch_data:
                break
            
            # Insert batch to ClickHouse
            self._insert_batch_to_clickhouse(table_name, batch_data, schema)
            
            migrated_rows += len(batch_data)
            offset += batch_size
            
            # Progress notification
            progress = (migrated_rows / total_rows) * 100
            self.logger.info(f"Migration progress: {progress:.1f}% ({migrated_rows:,}/{total_rows:,})")
            
            # Send progress notification every 10%
            if int(progress) % 10 == 0 and int(progress) > 0:
                send_discord_alert(
                    self.discord_config,
                    f"📊 Migration Progress: {table_name}",
                    f"Migration progress: {progress:.1f}%",
                    {
                        "Table": table_name,
                        "Progress": f"{progress:.1f}%",
                        "Migrated Rows": f"{migrated_rows:,}",
                        "Total Rows": f"{total_rows:,}",
                        "Status": "IN_PROGRESS"
                    },
                    "info"
                )
        
        self.logger.info(f"Migration completed for table: {table_name}")
        return migrated_rows
    
    def _insert_batch_to_clickhouse(self, table_name: str, batch_data: List[tuple], schema: List[Dict[str, Any]]):
        """Insert batch data to ClickHouse"""
        if not batch_data:
            return
        
        # Prepare insert query
        columns = [col[0] for col in schema]
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Insert data
        for row in batch_data:
            self.ch_connector.execute(insert_query, row)
    
    def migrate_database(self, tables: List[str]):
        """Migrate entire database"""
        try:
            start_time = time.time()
            
            send_discord_alert(
                self.discord_config,
                "🚀 Database Migration Started",
                f"Starting migration of {len(tables)} tables",
                {
                    "Tables": ', '.join(tables),
                    "Status": "STARTED"
                },
                "info"
            )
            
            total_migrated = 0
            
            for table in tables:
                try:
                    migrated_rows = self.migrate_table(table)
                    total_migrated += migrated_rows
                    
                    self.logger.info(f"Table {table} migrated: {migrated_rows:,} rows")
                    
                except Exception as e:
                    self.logger.error(f"Failed to migrate table {table}: {e}")
                    send_discord_alert(
                        self.discord_config,
                        f"❌ Migration Failed: {table}",
                        f"Failed to migrate table {table}",
                        {
                            "Table": table,
                            "Error": str(e),
                            "Status": "FAILED"
                        },
                        "error"
                    )
                    continue
            
            duration = time.time() - start_time
            
            send_discord_alert(
                self.discord_config,
                "✅ Database Migration Completed",
                f"Migration completed in {duration:.2f} seconds",
                {
                    "Total Tables": str(len(tables)),
                    "Total Rows Migrated": f"{total_migrated:,}",
                    "Duration": f"{duration:.2f} seconds",
                    "Status": "COMPLETED"
                },
                "info"
            )
            
        except Exception as e:
            duration = time.time() - start_time
            send_discord_alert(
                self.discord_config,
                "❌ Database Migration Failed",
                f"Migration failed after {duration:.2f} seconds",
                {
                    "Error": str(e),
                    "Duration": f"{duration:.2f} seconds",
                    "Status": "FAILED"
                },
                "error"
            )
            raise

# Sử dụng
if __name__ == "__main__":
    migrator = DatabaseMigrator(
        pg_dsn="postgresql://user:pass@pg-host:5432/source_db",
        ch_dsn="http://clickhouse-host:8123"
    )
    
    tables_to_migrate = ["users", "orders", "products", "order_items"]
    migrator.migrate_database(tables_to_migrate)
```

## 4. System Health Monitor

### Ví dụ: Monitor hệ thống với Discord alerts

```python
"""
System Health Monitor: Monitor hệ thống và gửi cảnh báo qua Discord
"""

from ami_helpers.database_engines import create_connector, DBConfig
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig, send_discord_alert
from ami_helpers.utils.retry import retriable
from ami_helpers.utils.log_info import setup_logging
import logging
import time
import psutil
import requests
from datetime import datetime
from typing import Dict, Any

class SystemHealthMonitor:
    def __init__(self, discord_webhook_url: str):
        setup_logging(level="INFO", logfile="health_monitor.log")
        self.logger = logging.getLogger(__name__)
        
        self.discord_config = DiscordAlertConfig(
            webhook_url=discord_webhook_url,
            environment="production",
            service="health-monitor"
        )
        
        # Thresholds
        self.thresholds = {
            "cpu_percent": 80,
            "memory_percent": 85,
            "disk_percent": 90,
            "response_time_ms": 5000
        }
        
        # Database configurations (optional)
        self.db_configs = {}
    
    def add_database_monitor(self, name: str, dsn: str):
        """Add database to monitoring"""
        self.db_configs[name] = DBConfig(dsn=dsn, max_pool_size=2)
        self.logger.info(f"Added database monitor: {name}")
    
    def check_system_metrics(self) -> Dict[str, Any]:
        """Check system metrics"""
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory": psutil.virtual_memory(),
            "disk": psutil.disk_usage('/'),
            "load_avg": psutil.getloadavg() if hasattr(psutil, 'getloadavg') else None
        }
        
        return metrics
    
    @retriable(
        (requests.exceptions.RequestException,),
        max_attempts=3,
        max_seconds=30
    )
    def check_web_service(self, url: str) -> Dict[str, Any]:
        """Check web service health"""
        start_time = time.time()
        
        try:
            response = requests.get(url, timeout=10)
            response_time = (time.time() - start_time) * 1000  # Convert to ms
            
            return {
                "url": url,
                "status_code": response.status_code,
                "response_time_ms": response_time,
                "healthy": response.status_code == 200 and response_time < self.thresholds["response_time_ms"]
            }
        except Exception as e:
            return {
                "url": url,
                "status_code": None,
                "response_time_ms": None,
                "healthy": False,
                "error": str(e)
            }
    
    def check_database_health(self, name: str, config: DBConfig) -> Dict[str, Any]:
        """Check database health"""
        try:
            connector = create_connector(config)
            is_healthy = connector.health_check()
            
            return {
                "name": name,
                "healthy": is_healthy,
                "error": None
            }
        except Exception as e:
            return {
                "name": name,
                "healthy": False,
                "error": str(e)
            }
    
    def analyze_metrics(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze metrics and generate alerts"""
        alerts = []
        
        # CPU check
        if metrics["cpu_percent"] > self.thresholds["cpu_percent"]:
            alerts.append({
                "type": "warning",
                "title": "⚠️ High CPU Usage",
                "description": f"CPU usage is {metrics['cpu_percent']:.1f}%",
                "fields": {
                    "CPU Usage": f"{metrics['cpu_percent']:.1f}%",
                    "Threshold": f"{self.thresholds['cpu_percent']}%",
                    "Status": "WARNING"
                }
            })
        
        # Memory check
        memory = metrics["memory"]
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
        
        # Disk check
        disk = metrics["disk"]
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
        
        return alerts
    
    def send_health_report(self, metrics: Dict[str, Any], web_services: List[Dict[str, Any]], 
                          databases: List[Dict[str, Any]], alerts: List[Dict[str, Any]]):
        """Send comprehensive health report"""
        
        # Calculate overall health
        all_healthy = (
            len(alerts) == 0 and
            all(service["healthy"] for service in web_services) and
            all(db["healthy"] for db in databases)
        )
        
        if all_healthy:
            # Send healthy report
            send_discord_alert(
                self.discord_config,
                "✅ System Health Report",
                "All systems are operating normally",
                {
                    "CPU Usage": f"{metrics['cpu_percent']:.1f}%",
                    "Memory Usage": f"{metrics['memory'].percent:.1f}%",
                    "Disk Usage": f"{metrics['disk'].percent:.1f}%",
                    "Web Services": f"{len([s for s in web_services if s['healthy']])}/{len(web_services)} healthy",
                    "Databases": f"{len([d for d in databases if d['healthy']])}/{len(databases)} healthy",
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Status": "HEALTHY"
                },
                "info"
            )
        else:
            # Send alert summary
            alert_summary = f"Found {len(alerts)} system alerts"
            send_discord_alert(
                self.discord_config,
                "⚠️ System Health Alert",
                alert_summary,
                {
                    "CPU Usage": f"{metrics['cpu_percent']:.1f}%",
                    "Memory Usage": f"{metrics['memory'].percent:.1f}%",
                    "Disk Usage": f"{metrics['disk'].percent:.1f}%",
                    "Web Services": f"{len([s for s in web_services if s['healthy']])}/{len(web_services)} healthy",
                    "Databases": f"{len([d for d in databases if d['healthy']])}/{len(databases)} healthy",
                    "Alerts": str(len(alerts)),
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Status": "WARNING"
                },
                "warning"
            )
            
            # Send individual alerts
            for alert in alerts:
                send_discord_alert(
                    self.discord_config,
                    alert["title"],
                    alert["description"],
                    alert["fields"],
                    alert["type"]
                )
    
    def run_health_check(self, web_services: List[str] = None):
        """Run complete health check"""
        try:
            self.logger.info("Starting health check")
            
            # Check system metrics
            metrics = self.check_system_metrics()
            
            # Check web services
            web_services = web_services or []
            web_health = [self.check_web_service(url) for url in web_services]
            
            # Check databases
            db_health = []
            for name, config in self.db_configs.items():
                db_health.append(self.check_database_health(name, config))
            
            # Analyze metrics
            alerts = self.analyze_metrics(metrics)
            
            # Send health report
            self.send_health_report(metrics, web_health, db_health, alerts)
            
            self.logger.info("Health check completed")
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            send_discord_alert(
                self.discord_config,
                "❌ Health Check Failed",
                f"Health check encountered an error: {str(e)}",
                {"Error": str(e), "Status": "ERROR"},
                "error"
            )
            raise

# Sử dụng
if __name__ == "__main__":
    monitor = SystemHealthMonitor("your-discord-webhook-url")
    
    # Add database monitoring
    monitor.add_database_monitor("postgres", "postgresql://user:pass@host:5432/db")
    monitor.add_database_monitor("clickhouse", "http://ch-host:8123")
    
    # Run health check
    monitor.run_health_check([
        "https://api.example.com/health",
        "https://web.example.com/status"
    ])
```

## 5. Data Quality Checker

### Ví dụ: Kiểm tra chất lượng dữ liệu

```python
"""
Data Quality Checker: Kiểm tra chất lượng dữ liệu với Spark
và báo cáo kết quả qua Discord
"""

from ami_helpers.spark_hive_engines import make_engine
from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig, send_discord_alert
from ami_helpers.utils.log_info import setup_logging
import logging
from typing import Dict, Any, List

class DataQualityChecker:
    def __init__(self, discord_webhook_url: str):
        setup_logging(level="INFO", logfile="data_quality.log")
        self.logger = logging.getLogger(__name__)
        
        self.discord_config = DiscordAlertConfig(
            webhook_url=discord_webhook_url,
            environment="production",
            service="data-quality-checker"
        )
    
    def check_table_quality(self, table_name: str, checks: List[Dict[str, Any]]):
        """Check data quality for a table"""
        try:
            with make_engine(kind="hive", app_name="data_quality_checker") as engine:
                self.logger.info(f"Starting quality check for table: {table_name}")
                
                results = {
                    "table_name": table_name,
                    "total_records": 0,
                    "checks": [],
                    "overall_quality": "PASS"
                }
                
                # Get total record count
                count_df = engine.sql(f"SELECT COUNT(*) as total FROM {table_name}")
                total_records = count_df.collect()[0]["total"]
                results["total_records"] = total_records
                
                if total_records == 0:
                    self.logger.warning(f"Table {table_name} is empty")
                    return results
                
                # Run quality checks
                for check in checks:
                    check_result = self._run_quality_check(engine, table_name, check)
                    results["checks"].append(check_result)
                    
                    if not check_result["passed"]:
                        results["overall_quality"] = "FAIL"
                
                # Send quality report
                self._send_quality_report(results)
                
                return results
                
        except Exception as e:
            self.logger.error(f"Quality check failed for table {table_name}: {e}")
            send_discord_alert(
                self.discord_config,
                f"❌ Quality Check Failed: {table_name}",
                f"Error during quality check: {str(e)}",
                {"Table": table_name, "Error": str(e), "Status": "ERROR"},
                "error"
            )
            raise
    
    def _run_quality_check(self, engine, table_name: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Run a single quality check"""
        check_name = check["name"]
        check_type = check["type"]
        
        try:
            if check_type == "null_check":
                return self._check_nulls(engine, table_name, check)
            elif check_type == "duplicate_check":
                return self._check_duplicates(engine, table_name, check)
            elif check_type == "range_check":
                return self._check_range(engine, table_name, check)
            elif check_type == "format_check":
                return self._check_format(engine, table_name, check)
            elif check_type == "referential_integrity":
                return self._check_referential_integrity(engine, table_name, check)
            else:
                return {
                    "name": check_name,
                    "type": check_type,
                    "passed": False,
                    "message": f"Unknown check type: {check_type}",
                    "details": {}
                }
        except Exception as e:
            return {
                "name": check_name,
                "type": check_type,
                "passed": False,
                "message": f"Check failed with error: {str(e)}",
                "details": {}
            }
    
    def _check_nulls(self, engine, table_name: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Check for null values in specified columns"""
        columns = check["columns"]
        max_null_percentage = check.get("max_null_percentage", 0)
        
        null_checks = []
        for column in columns:
            null_count_df = engine.sql(f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT({column}) as non_null,
                    COUNT(*) - COUNT({column}) as null_count
                FROM {table_name}
            """)
            
            result = null_count_df.collect()[0]
            total = result["total"]
            null_count = result["null_count"]
            null_percentage = (null_count / total) * 100 if total > 0 else 0
            
            null_checks.append({
                "column": column,
                "null_count": null_count,
                "null_percentage": null_percentage,
                "passed": null_percentage <= max_null_percentage
            })
        
        overall_passed = all(check["passed"] for check in null_checks)
        
        return {
            "name": check["name"],
            "type": "null_check",
            "passed": overall_passed,
            "message": f"Null check {'passed' if overall_passed else 'failed'}",
            "details": {"columns": null_checks}
        }
    
    def _check_duplicates(self, engine, table_name: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Check for duplicate records"""
        columns = check["columns"]
        
        duplicate_df = engine.sql(f"""
            SELECT 
                {', '.join(columns)},
                COUNT(*) as duplicate_count
            FROM {table_name}
            GROUP BY {', '.join(columns)}
            HAVING COUNT(*) > 1
        """)
        
        duplicates = duplicate_df.collect()
        duplicate_count = len(duplicates)
        
        return {
            "name": check["name"],
            "type": "duplicate_check",
            "passed": duplicate_count == 0,
            "message": f"Found {duplicate_count} duplicate groups",
            "details": {"duplicate_count": duplicate_count}
        }
    
    def _check_range(self, engine, table_name: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Check if values are within specified range"""
        column = check["column"]
        min_value = check.get("min_value")
        max_value = check.get("max_value")
        
        range_conditions = []
        if min_value is not None:
            range_conditions.append(f"{column} >= {min_value}")
        if max_value is not None:
            range_conditions.append(f"{column} <= {max_value}")
        
        if not range_conditions:
            return {
                "name": check["name"],
                "type": "range_check",
                "passed": True,
                "message": "No range constraints specified",
                "details": {}
            }
        
        where_clause = " AND ".join(range_conditions)
        
        range_df = engine.sql(f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN {where_clause} THEN 1 ELSE 0 END) as in_range,
                COUNT(*) - SUM(CASE WHEN {where_clause} THEN 1 ELSE 0 END) as out_of_range
            FROM {table_name}
        """)
        
        result = range_df.collect()[0]
        total = result["total"]
        out_of_range = result["out_of_range"]
        out_of_range_percentage = (out_of_range / total) * 100 if total > 0 else 0
        
        return {
            "name": check["name"],
            "type": "range_check",
            "passed": out_of_range == 0,
            "message": f"{out_of_range} records out of range ({out_of_range_percentage:.2f}%)",
            "details": {
                "total_records": total,
                "out_of_range": out_of_range,
                "out_of_range_percentage": out_of_range_percentage
            }
        }
    
    def _check_format(self, engine, table_name: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Check if values match specified format"""
        column = check["column"]
        pattern = check["pattern"]
        
        format_df = engine.sql(f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN {column} RLIKE '{pattern}' THEN 1 ELSE 0 END) as matches,
                COUNT(*) - SUM(CASE WHEN {column} RLIKE '{pattern}' THEN 1 ELSE 0 END) as no_matches
            FROM {table_name}
        """)
        
        result = format_df.collect()[0]
        total = result["total"]
        no_matches = result["no_matches"]
        no_matches_percentage = (no_matches / total) * 100 if total > 0 else 0
        
        return {
            "name": check["name"],
            "type": "format_check",
            "passed": no_matches == 0,
            "message": f"{no_matches} records don't match format ({no_matches_percentage:.2f}%)",
            "details": {
                "total_records": total,
                "no_matches": no_matches,
                "no_matches_percentage": no_matches_percentage,
                "pattern": pattern
            }
        }
    
    def _check_referential_integrity(self, engine, table_name: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Check referential integrity"""
        foreign_key = check["foreign_key"]
        reference_table = check["reference_table"]
        reference_key = check["reference_key"]
        
        integrity_df = engine.sql(f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN r.{reference_key} IS NOT NULL THEN 1 ELSE 0 END) as valid,
                COUNT(*) - SUM(CASE WHEN r.{reference_key} IS NOT NULL THEN 1 ELSE 0 END) as invalid
            FROM {table_name} t
            LEFT JOIN {reference_table} r ON t.{foreign_key} = r.{reference_key}
        """)
        
        result = integrity_df.collect()[0]
        total = result["total"]
        invalid = result["invalid"]
        invalid_percentage = (invalid / total) * 100 if total > 0 else 0
        
        return {
            "name": check["name"],
            "type": "referential_integrity",
            "passed": invalid == 0,
            "message": f"{invalid} records have invalid references ({invalid_percentage:.2f}%)",
            "details": {
                "total_records": total,
                "invalid": invalid,
                "invalid_percentage": invalid_percentage,
                "foreign_key": foreign_key,
                "reference_table": reference_table
            }
        }
    
    def _send_quality_report(self, results: Dict[str, Any]):
        """Send data quality report via Discord"""
        table_name = results["table_name"]
        total_records = results["total_records"]
        overall_quality = results["overall_quality"]
        checks = results["checks"]
        
        # Count passed/failed checks
        passed_checks = len([c for c in checks if c["passed"]])
        failed_checks = len([c for c in checks if not c["passed"]])
        
        # Create detailed fields
        fields = {
            "Table": table_name,
            "Total Records": f"{total_records:,}",
            "Checks Passed": f"{passed_checks}/{len(checks)}",
            "Overall Quality": overall_quality
        }
        
        # Add failed check details
        if failed_checks > 0:
            failed_check_names = [c["name"] for c in checks if not c["passed"]]
            fields["Failed Checks"] = ", ".join(failed_check_names)
        
        # Determine alert level and title
        if overall_quality == "PASS":
            title = f"✅ Data Quality Check: {table_name}"
            level = "info"
        else:
            title = f"⚠️ Data Quality Issues: {table_name}"
            level = "warning"
        
        send_discord_alert(
            self.discord_config,
            title,
            f"Data quality check completed for {table_name}",
            fields,
            level
        )

# Sử dụng
if __name__ == "__main__":
    checker = DataQualityChecker("your-discord-webhook-url")
    
    # Define quality checks
    quality_checks = [
        {
            "name": "No Null IDs",
            "type": "null_check",
            "columns": ["id", "user_id"],
            "max_null_percentage": 0
        },
        {
            "name": "No Duplicate IDs",
            "type": "duplicate_check",
            "columns": ["id"]
        },
        {
            "name": "Valid Email Format",
            "type": "format_check",
            "column": "email",
            "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        },
        {
            "name": "Age Range Check",
            "type": "range_check",
            "column": "age",
            "min_value": 0,
            "max_value": 120
        },
        {
            "name": "User ID References",
            "type": "referential_integrity",
            "foreign_key": "user_id",
            "reference_table": "users",
            "reference_key": "id"
        }
    ]
    
    # Run quality check
    results = checker.check_table_quality("analytics.user_events", quality_checks)
    print(f"Quality check results: {results}")
```

## Kết luận

Các ví dụ trên minh họa cách sử dụng thư viện AMI Helpers trong các tình huống thực tế:

1. **ETL Pipeline**: Xử lý dữ liệu từ PostgreSQL sang ClickHouse với monitoring
2. **Real-time Processing**: Xử lý log files với Spark và Hive
3. **Database Migration**: Migrate dữ liệu giữa các database khác nhau
4. **System Monitoring**: Monitor hệ thống và gửi cảnh báo
5. **Data Quality**: Kiểm tra chất lượng dữ liệu với các rule tùy chỉnh

Mỗi ví dụ đều tích hợp đầy đủ các tính năng của thư viện:
- Logging và error handling
- Retry mechanisms
- Discord notifications
- Database operations
- Spark/Hive processing

Bạn có thể sử dụng các ví dụ này làm template cho các dự án của mình.

