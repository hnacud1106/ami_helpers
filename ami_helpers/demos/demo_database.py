#!/usr/bin/env python3
"""
Demo Script: Database Operations với AMI Helpers

Script này demo cách sử dụng database engines với các cấu hình khác nhau.
Chạy script này để test kết nối PostgreSQL và ClickHouse.

Cách chạy:
    python demo_database.py

Yêu cầu:
    - PostgreSQL server đang chạy
    - ClickHouse server đang chạy (optional)
    - Cài đặt dependencies: pip install -r requirements.txt
"""

import os
import sys
import time
from datetime import datetime
from typing import Dict, Any

# Add ami_helpers to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'ami_helpers'))

from ami_helpers.database_engines import create_connector, DBConfig
from ami_helpers.utils.log_info import setup_logging
from ami_helpers.utils.retry import retriable
import logging

# Setup logging
setup_logging(level="INFO", logfile="demo_database.log")
logger = logging.getLogger(__name__)

class DatabaseDemo:
    def __init__(self):
        self.results = {}
        
    def test_postgresql_connection(self):
        """Test PostgreSQL connection với các cấu hình khác nhau"""
        logger.info("=== Testing PostgreSQL Connection ===")
        
        # Cấu hình PostgreSQL
        pg_configs = {
            "development": DBConfig(
                dsn="postgresql://postgres:password@localhost:5432/testdb",
                max_pool_size=3,
                min_pool_size=1,
                connect_timeout_s=5,
                statement_timeout_ms=30000,
                ssl_require=False
            ),
            "production": DBConfig(
                dsn="postgresql://postgres:password@localhost:5432/testdb",
                max_pool_size=10,
                min_pool_size=2,
                connect_timeout_s=10,
                statement_timeout_ms=60000,
                ssl_require=True
            )
        }
        
        for env_name, config in pg_configs.items():
            try:
                logger.info(f"Testing {env_name} configuration...")
                
                # Tạo connector
                connector = create_connector(config)
                
                # Test health check
                is_healthy = connector.health_check()
                logger.info(f"Health check: {'PASS' if is_healthy else 'FAIL'}")
                
                if is_healthy:
                    # Test simple query
                    result = connector.fetchall("SELECT version()")
                    logger.info(f"PostgreSQL version: {result[0][0]}")
                    
                    # Test với parameters
                    result = connector.fetchall("SELECT %(test)s as test_value", {"test": "Hello World"})
                    logger.info(f"Parameter test: {result[0][0]}")
                    
                    # Test performance
                    start_time = time.time()
                    for i in range(10):
                        connector.fetchall("SELECT %(num)s", {"num": i})
                    duration = time.time() - start_time
                    logger.info(f"10 queries completed in {duration:.3f}s")
                    
                    self.results[f"postgresql_{env_name}"] = "SUCCESS"
                else:
                    self.results[f"postgresql_{env_name}"] = "FAILED"
                    
            except Exception as e:
                logger.error(f"PostgreSQL {env_name} test failed: {e}")
                self.results[f"postgresql_{env_name}"] = f"ERROR: {e}"
    
    def test_clickhouse_connection(self):
        """Test ClickHouse connection"""
        logger.info("=== Testing ClickHouse Connection ===")
        
        try:
            # Cấu hình ClickHouse
            ch_config = DBConfig(
                dsn="http://localhost:8123",
                ch_database="default",
                ch_user="default",
                ch_password="",
                max_pool_size=5,
                min_pool_size=1,
                connect_timeout_s=5
            )
            
            # Tạo connector
            connector = create_connector(ch_config)
            
            # Test health check
            is_healthy = connector.health_check()
            logger.info(f"Health check: {'PASS' if is_healthy else 'FAIL'}")
            
            if is_healthy:
                # Test simple query
                result = connector.fetchall("SELECT version()")
                logger.info(f"ClickHouse version: {result[0][0]}")
                
                # Test CREATE TABLE
                connector.execute("""
                    CREATE TABLE IF NOT EXISTS test_table (
                        id UInt32,
                        name String,
                        created_at DateTime
                    ) ENGINE = Memory
                """)
                logger.info("Test table created successfully")
                
                # Test INSERT
                connector.execute("""
                    INSERT INTO test_table VALUES 
                    (1, 'Test User 1', now()),
                    (2, 'Test User 2', now())
                """)
                logger.info("Test data inserted successfully")
                
                # Test SELECT
                result = connector.fetchall("SELECT * FROM test_table ORDER BY id")
                logger.info(f"Retrieved {len(result)} rows")
                for row in result:
                    logger.info(f"Row: {row}")
                
                # Test DROP TABLE
                connector.execute("DROP TABLE test_table")
                logger.info("Test table dropped successfully")
                
                self.results["clickhouse"] = "SUCCESS"
            else:
                self.results["clickhouse"] = "FAILED"
                
        except Exception as e:
            logger.error(f"ClickHouse test failed: {e}")
            self.results["clickhouse"] = f"ERROR: {e}"
    
    @retriable(
        (Exception,),
        max_attempts=3,
        max_seconds=30,
        initial_wait=1.0,
        max_wait=5.0
    )
    def test_retry_mechanism(self):
        """Test retry mechanism với simulated failures"""
        logger.info("=== Testing Retry Mechanism ===")
        
        import random
        
        # Simulate random failures
        if random.random() < 0.7:  # 70% chance of failure
            raise ConnectionError("Simulated connection error")
        
        logger.info("Retry test completed successfully")
        return "SUCCESS"
    
    def test_connection_pooling(self):
        """Test connection pooling performance"""
        logger.info("=== Testing Connection Pooling ===")
        
        try:
            # Cấu hình với pool size lớn
            config = DBConfig(
                dsn="postgresql://postgres:password@localhost:5432/testdb",
                max_pool_size=10,
                min_pool_size=2,
                connect_timeout_s=5,
                ssl_require=False
            )
            
            connector = create_connector(config)
            
            # Test concurrent connections
            import threading
            import queue
            
            results_queue = queue.Queue()
            
            def worker(worker_id):
                try:
                    start_time = time.time()
                    result = connector.fetchall("SELECT %(worker_id)s as worker", {"worker_id": worker_id})
                    duration = time.time() - start_time
                    results_queue.put((worker_id, "SUCCESS", duration))
                except Exception as e:
                    results_queue.put((worker_id, f"ERROR: {e}", 0))
            
            # Start multiple threads
            threads = []
            for i in range(5):
                thread = threading.Thread(target=worker, args=(i,))
                threads.append(thread)
                thread.start()
            
            # Wait for all threads
            for thread in threads:
                thread.join()
            
            # Collect results
            while not results_queue.empty():
                worker_id, status, duration = results_queue.get()
                logger.info(f"Worker {worker_id}: {status} in {duration:.3f}s")
            
            self.results["connection_pooling"] = "SUCCESS"
            
        except Exception as e:
            logger.error(f"Connection pooling test failed: {e}")
            self.results["connection_pooling"] = f"ERROR: {e}"
    
    def run_all_tests(self):
        """Chạy tất cả tests"""
        logger.info("Starting Database Demo Tests")
        logger.info("=" * 50)
        
        start_time = time.time()
        
        # Run tests
        self.test_postgresql_connection()
        self.test_clickhouse_connection()
        self.test_retry_mechanism()
        self.test_connection_pooling()
        
        duration = time.time() - start_time
        
        # Print summary
        logger.info("=" * 50)
        logger.info("TEST SUMMARY")
        logger.info("=" * 50)
        
        for test_name, result in self.results.items():
            logger.info(f"{test_name}: {result}")
        
        logger.info(f"Total duration: {duration:.2f}s")
        
        # Count successes
        successes = sum(1 for result in self.results.values() if result == "SUCCESS")
        total = len(self.results)
        
        logger.info(f"Tests passed: {successes}/{total}")
        
        if successes == total:
            logger.info("🎉 All tests passed!")
        else:
            logger.warning(f"⚠️ {total - successes} tests failed")

def main():
    """Main function"""
    print("AMI Helpers - Database Demo")
    print("=" * 50)
    
    # Check if required environment variables are set
    required_vars = ["POSTGRES_DSN"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these variables before running the demo:")
        print("export POSTGRES_DSN='postgresql://user:pass@host:port/db'")
        print("export CLICKHOUSE_DSN='http://host:8123'  # Optional")
        return
    
    # Run demo
    demo = DatabaseDemo()
    demo.run_all_tests()

if __name__ == "__main__":
    main()

