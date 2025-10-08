#!/usr/bin/env python3
"""
Demo Script: Spark/Hive Operations với AMI Helpers

Script này demo cách sử dụng Spark/Hive engines với các cấu hình khác nhau.
Chạy script này để test Spark operations và Hive integration.

Cách chạy:
    python demo_spark.py

Yêu cầu:
    - Java 8+ installed
    - Apache Spark 3.0+ installed
    - Hive Metastore (optional)
    - Cài đặt dependencies: pip install -r requirements.txt
"""

import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, List

# Add ami_helpers to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'ami_helpers'))

from ami_helpers.spark_hive_engines import make_engine
from ami_helpers.utils.log_info import setup_logging
import logging

# Setup logging
setup_logging(level="INFO", logfile="demo_spark.log")
logger = logging.getLogger(__name__)

class SparkDemo:
    def __init__(self):
        self.results = {}
        
    def test_spark_local_mode(self):
        """Test Spark trong local mode"""
        logger.info("=== Testing Spark Local Mode ===")
        
        try:
            with make_engine(
                kind="spark",
                app_name="demo_spark_local",
                master="local[2]",  # 2 cores
                conf={
                    "spark.executor.memory": "1G",
                    "spark.driver.memory": "1G",
                    "spark.sql.shuffle.partitions": "4"
                }
            ) as engine:
                
                logger.info(f"Spark UI: {engine.spark.sparkContext.uiWebUrl}")
                
                # Test basic SQL
                df = engine.sql("SELECT 1 as test_column")
                result = df.collect()
                logger.info(f"Basic SQL test: {result[0]['test_column']}")
                
                # Test data creation
                data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
                df = engine.spark.createDataFrame(data, ["id", "name", "age"])
                logger.info(f"Created DataFrame with {df.count()} rows")
                
                # Test SQL operations
                df.createOrReplaceTempView("people")
                result_df = engine.sql("""
                    SELECT 
                        name,
                        age,
                        CASE 
                            WHEN age < 30 THEN 'Young'
                            WHEN age < 40 THEN 'Middle'
                            ELSE 'Senior'
                        END as age_group
                    FROM people
                    ORDER BY age
                """)
                
                results = result_df.collect()
                logger.info("SQL query results:")
                for row in results:
                    logger.info(f"  {row['name']}: {row['age']} years old ({row['age_group']})")
                
                self.results["spark_local"] = "SUCCESS"
                
        except Exception as e:
            logger.error(f"Spark local mode test failed: {e}")
            self.results["spark_local"] = f"ERROR: {e}"
    
    def test_spark_performance(self):
        """Test Spark performance với large dataset"""
        logger.info("=== Testing Spark Performance ===")
        
        try:
            with make_engine(
                kind="spark",
                app_name="demo_spark_performance",
                master="local[4]",
                conf={
                    "spark.executor.memory": "2G",
                    "spark.driver.memory": "2G",
                    "spark.sql.shuffle.partitions": "8",
                    "spark.sql.adaptive.enabled": "true",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                }
            ) as engine:
                
                # Create large dataset
                logger.info("Creating large dataset...")
                start_time = time.time()
                
                # Generate 100,000 rows
                data = []
                for i in range(100000):
                    data.append((
                        i,
                        f"user_{i}",
                        i % 100,  # department_id
                        i % 10,   # region_id
                        i % 1000, # amount
                        datetime.now()
                    ))
                
                df = engine.spark.createDataFrame(data, [
                    "id", "username", "department_id", "region_id", "amount", "created_at"
                ])
                
                creation_time = time.time() - start_time
                logger.info(f"Dataset created in {creation_time:.2f}s")
                
                # Test aggregations
                logger.info("Testing aggregations...")
                start_time = time.time()
                
                agg_df = engine.sql("""
                    SELECT 
                        department_id,
                        region_id,
                        COUNT(*) as user_count,
                        SUM(amount) as total_amount,
                        AVG(amount) as avg_amount,
                        MAX(amount) as max_amount
                    FROM users
                    GROUP BY department_id, region_id
                    ORDER BY total_amount DESC
                """)
                
                agg_results = agg_df.collect()
                agg_time = time.time() - start_time
                logger.info(f"Aggregation completed in {agg_time:.2f}s")
                logger.info(f"Generated {len(agg_results)} summary rows")
                
                # Test joins
                logger.info("Testing joins...")
                start_time = time.time()
                
                # Create department lookup table
                dept_data = [(i, f"Dept_{i}") for i in range(100)]
                dept_df = engine.spark.createDataFrame(dept_data, ["dept_id", "dept_name"])
                dept_df.createOrReplaceTempView("departments")
                
                join_df = engine.sql("""
                    SELECT 
                        u.username,
                        d.dept_name,
                        u.amount
                    FROM users u
                    JOIN departments d ON u.department_id = d.dept_id
                    WHERE u.amount > 500
                    ORDER BY u.amount DESC
                """)
                
                join_results = join_df.collect()
                join_time = time.time() - start_time
                logger.info(f"Join completed in {join_time:.2f}s")
                logger.info(f"Retrieved {len(join_results)} joined rows")
                
                self.results["spark_performance"] = "SUCCESS"
                
        except Exception as e:
            logger.error(f"Spark performance test failed: {e}")
            self.results["spark_performance"] = f"ERROR: {e}"
    
    def test_hive_integration(self):
        """Test Hive integration (nếu có Hive Metastore)"""
        logger.info("=== Testing Hive Integration ===")
        
        try:
            with make_engine(
                kind="hive",
                app_name="demo_hive_integration",
                master="local[2]",
                conf={
                    "spark.sql.warehouse.dir": "file:///tmp/spark-warehouse",
                    "spark.sql.catalogImplementation": "hive",
                    "spark.sql.legacy.allowCreatingManagedTableInExternalDb": "true"
                }
            ) as engine:
                
                logger.info("Testing Hive table operations...")
                
                # Create test data
                data = [
                    (1, "Product A", 100.0, "2024-01-01"),
                    (2, "Product B", 200.0, "2024-01-02"),
                    (3, "Product C", 150.0, "2024-01-03")
                ]
                
                df = engine.spark.createDataFrame(data, ["id", "name", "price", "date"])
                
                # Write to Hive table
                engine.write_table(df, "demo.products", mode="overwrite")
                logger.info("Data written to Hive table: demo.products")
                
                # Read from Hive table
                read_df = engine.read_table("demo.products")
                count = read_df.count()
                logger.info(f"Read {count} rows from Hive table")
                
                # Test Hive SQL
                result_df = engine.sql("""
                    SELECT 
                        name,
                        price,
                        CASE 
                            WHEN price > 150 THEN 'Expensive'
                            ELSE 'Affordable'
                        END as price_category
                    FROM demo.products
                    ORDER BY price DESC
                """)
                
                results = result_df.collect()
                logger.info("Hive SQL results:")
                for row in results:
                    logger.info(f"  {row['name']}: ${row['price']} ({row['price_category']})")
                
                # Clean up
                engine.sql("DROP TABLE IF EXISTS demo.products")
                logger.info("Test table cleaned up")
                
                self.results["hive_integration"] = "SUCCESS"
                
        except Exception as e:
            logger.error(f"Hive integration test failed: {e}")
            self.results["hive_integration"] = f"ERROR: {e}"
    
    def test_spark_configurations(self):
        """Test các cấu hình Spark khác nhau"""
        logger.info("=== Testing Spark Configurations ===")
        
        configurations = {
            "development": {
                "master": "local[2]",
                "conf": {
                    "spark.executor.memory": "1G",
                    "spark.driver.memory": "1G",
                    "spark.sql.shuffle.partitions": "4"
                }
            },
            "production": {
                "master": "local[4]",
                "conf": {
                    "spark.executor.memory": "4G",
                    "spark.driver.memory": "2G",
                    "spark.sql.shuffle.partitions": "200",
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                }
            }
        }
        
        for config_name, config in configurations.items():
            try:
                logger.info(f"Testing {config_name} configuration...")
                
                with make_engine(
                    kind="spark",
                    app_name=f"demo_{config_name}",
                    **config
                ) as engine:
                    
                    # Test basic operations
                    df = engine.sql("SELECT 1 as test")
                    result = df.collect()
                    
                    # Test configuration
                    conf = engine.spark.conf.getAll()
                    logger.info(f"Configuration applied: {len(conf)} settings")
                    
                    # Test performance
                    start_time = time.time()
                    for i in range(100):
                        engine.sql(f"SELECT {i} as num")
                    duration = time.time() - start_time
                    logger.info(f"100 queries completed in {duration:.3f}s")
                    
                    self.results[f"spark_config_{config_name}"] = "SUCCESS"
                    
            except Exception as e:
                logger.error(f"Spark {config_name} configuration test failed: {e}")
                self.results[f"spark_config_{config_name}"] = f"ERROR: {e}"
    
    def test_temporary_configuration(self):
        """Test temporary configuration changes"""
        logger.info("=== Testing Temporary Configuration ===")
        
        try:
            with make_engine(
                kind="spark",
                app_name="demo_temporary_config",
                master="local[2]"
            ) as engine:
                
                # Test temporary configuration
                with engine.temporary_conf({
                    "spark.sql.autoBroadcastJoinThreshold": "1MB",
                    "spark.sql.adaptive.enabled": "false"
                }):
                    logger.info("Temporary configuration applied")
                    
                    # Test with temporary config
                    df = engine.sql("SELECT 1 as test")
                    result = df.collect()
                    
                    # Check if config is applied
                    conf = engine.spark.conf.getAll()
                    logger.info(f"Temporary config active: {len(conf)} settings")
                
                # Test after temporary config
                logger.info("Temporary configuration reverted")
                
                df = engine.sql("SELECT 2 as test")
                result = df.collect()
                
                self.results["temporary_config"] = "SUCCESS"
                
        except Exception as e:
            logger.error(f"Temporary configuration test failed: {e}")
            self.results["temporary_config"] = f"ERROR: {e}"
    
    def run_all_tests(self):
        """Chạy tất cả tests"""
        logger.info("Starting Spark Demo Tests")
        logger.info("=" * 50)
        
        start_time = time.time()
        
        # Run tests
        self.test_spark_local_mode()
        self.test_spark_performance()
        self.test_hive_integration()
        self.test_spark_configurations()
        self.test_temporary_configuration()
        
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
    print("AMI Helpers - Spark Demo")
    print("=" * 50)
    
    # Check Java installation
    java_home = os.getenv("JAVA_HOME")
    if not java_home:
        print("⚠️ JAVA_HOME not set. Make sure Java 8+ is installed.")
    
    # Check Spark installation
    spark_home = os.getenv("SPARK_HOME")
    if not spark_home:
        print("⚠️ SPARK_HOME not set. Make sure Apache Spark is installed.")
    
    print("Starting Spark demo tests...")
    print("Note: Some tests may fail if Spark/Hive is not properly configured.")
    print()
    
    # Run demo
    demo = SparkDemo()
    demo.run_all_tests()

if __name__ == "__main__":
    main()

