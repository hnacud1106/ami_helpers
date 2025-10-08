#!/usr/bin/env python3
"""
Demo Script: Discord Alerts với AMI Helpers

Script này demo cách sử dụng Discord alert system với các cấu hình khác nhau.
Chạy script này để test Discord webhook integration.

Cách chạy:
    python demo_alerts.py

Yêu cầu:
    - Discord webhook URL
    - Cài đặt dependencies: pip install -r requirements.txt
"""

import os
import sys
import time
import random
from datetime import datetime
from typing import Dict, Any, List

# Add ami_helpers to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'ami_helpers'))

from ami_helpers.alerts.alert_engines.discord_alerts import DiscordAlertConfig, send_discord_alert
from ami_helpers.utils.log_info import setup_logging
from ami_helpers.utils.retry import retriable_with_discord
import logging

# Setup logging
setup_logging(level="INFO", logfile="demo_alerts.log")
logger = logging.getLogger(__name__)

class AlertDemo:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.results = {}
        
        # Tạo các cấu hình khác nhau
        self.configs = {
            "production": DiscordAlertConfig(
                webhook_url=webhook_url,
                username="Production Monitor",
                avatar_url="https://cdn-icons-png.flaticon.com/512/2920/2920277.png",
                timeout_s=5,
                environment="production",
                service="demo-service",
                include_args=False,
                include_traceback=True
            ),
            "development": DiscordAlertConfig(
                webhook_url=webhook_url,
                username="Dev Debug Bot",
                avatar_url="https://cdn-icons-png.flaticon.com/512/2920/2920277.png",
                timeout_s=10,
                environment="development",
                service="demo-service",
                include_args=True,
                include_traceback=True
            ),
            "staging": DiscordAlertConfig(
                webhook_url=webhook_url,
                username="Staging Monitor",
                avatar_url="https://cdn-icons-png.flaticon.com/512/2920/2920277.png",
                timeout_s=5,
                environment="staging",
                service="demo-service",
                include_args=True,
                include_traceback=True
            )
        }
    
    def test_basic_alerts(self):
        """Test các loại alert cơ bản"""
        logger.info("=== Testing Basic Alerts ===")
        
        config = self.configs["development"]
        
        try:
            # Info alert
            send_discord_alert(
                config,
                title="ℹ️ Demo Started",
                description="Discord alert demo has started successfully",
                fields={
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Environment": "development",
                    "Status": "RUNNING"
                },
                level="info"
            )
            logger.info("Info alert sent successfully")
            
            # Warning alert
            send_discord_alert(
                config,
                title="⚠️ High Resource Usage",
                description="System resources are above normal thresholds",
                fields={
                    "CPU Usage": "85%",
                    "Memory Usage": "90%",
                    "Disk Usage": "75%",
                    "Threshold": "80%",
                    "Status": "WARNING"
                },
                level="warning"
            )
            logger.info("Warning alert sent successfully")
            
            # Error alert
            send_discord_alert(
                config,
                title="🚨 System Error",
                description="A critical system error has occurred",
                fields={
                    "Error Code": "500",
                    "Error Message": "Internal server error",
                    "Service": "demo-service",
                    "Status": "ERROR"
                },
                level="error"
            )
            logger.info("Error alert sent successfully")
            
            self.results["basic_alerts"] = "SUCCESS"
            
        except Exception as e:
            logger.error(f"Basic alerts test failed: {e}")
            self.results["basic_alerts"] = f"ERROR: {e}"
    
    def test_environment_configurations(self):
        """Test các cấu hình môi trường khác nhau"""
        logger.info("=== Testing Environment Configurations ===")
        
        for env_name, config in self.configs.items():
            try:
                logger.info(f"Testing {env_name} configuration...")
                
                send_discord_alert(
                    config,
                    title=f"🔧 {env_name.title()} Configuration Test",
                    description=f"Testing Discord alert configuration for {env_name} environment",
                    fields={
                        "Environment": env_name,
                        "Username": config.username,
                        "Timeout": f"{config.timeout_s}s",
                        "Include Args": str(config.include_args),
                        "Include Traceback": str(config.include_traceback),
                        "Status": "TESTING"
                    },
                    level="info"
                )
                
                logger.info(f"{env_name} configuration test completed")
                
            except Exception as e:
                logger.error(f"{env_name} configuration test failed: {e}")
        
        self.results["environment_configs"] = "SUCCESS"
    
    def test_rich_embeds(self):
        """Test rich embeds với nhiều fields"""
        logger.info("=== Testing Rich Embeds ===")
        
        config = self.configs["development"]
        
        try:
            # Tạo rich embed với nhiều sections
            fields = {
                "**System Metrics**": "",
                "CPU Usage": "75.5%",
                "Memory Usage": "82.3%",
                "Disk Usage": "45.1%",
                "Network I/O": "125.7 MB/s",
                "",
                "**Configuration**": "",
                "Max Memory": "8GB",
                "CPU Cores": "4",
                "Disk Space": "500GB",
                "Network Speed": "1Gbps",
                "",
                "**Performance**": "",
                "Response Time": "125ms",
                "Throughput": "1,250 req/s",
                "Error Rate": "0.1%",
                "Uptime": "99.9%"
            }
            
            send_discord_alert(
                config,
                title="📊 System Status Report",
                description="Comprehensive system status report with detailed metrics",
                fields=fields,
                level="info"
            )
            
            logger.info("Rich embed sent successfully")
            self.results["rich_embeds"] = "SUCCESS"
            
        except Exception as e:
            logger.error(f"Rich embeds test failed: {e}")
            self.results["rich_embeds"] = f"ERROR: {e}"
    
    def test_dynamic_fields(self):
        """Test dynamic field generation"""
        logger.info("=== Testing Dynamic Fields ===")
        
        config = self.configs["development"]
        
        try:
            # Tạo fields động từ data
            def create_alert_fields(data: Dict[str, Any]) -> Dict[str, str]:
                fields = {}
                
                # Basic info
                fields["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                fields["Environment"] = data.get("environment", "unknown")
                
                # Metrics
                if "metrics" in data:
                    metrics = data["metrics"]
                    fields["CPU Usage"] = f"{metrics.get('cpu', 0):.1f}%"
                    fields["Memory Usage"] = f"{metrics.get('memory', 0):.1f}%"
                    fields["Disk Usage"] = f"{metrics.get('disk', 0):.1f}%"
                
                # Errors
                if "errors" in data:
                    fields["Error Count"] = str(len(data["errors"]))
                    fields["Last Error"] = data["errors"][-1] if data["errors"] else "None"
                
                # Performance
                if "performance" in data:
                    perf = data["performance"]
                    fields["Response Time"] = f"{perf.get('response_time', 0):.1f}ms"
                    fields["Throughput"] = f"{perf.get('throughput', 0):,} req/s"
                
                return fields
            
            # Test data
            test_data = {
                "environment": "development",
                "metrics": {
                    "cpu": random.uniform(50, 90),
                    "memory": random.uniform(60, 95),
                    "disk": random.uniform(30, 80)
                },
                "errors": ["Connection timeout", "Retry failed"],
                "performance": {
                    "response_time": random.uniform(100, 500),
                    "throughput": random.randint(1000, 5000)
                }
            }
            
            fields = create_alert_fields(test_data)
            
            send_discord_alert(
                config,
                title="🔄 Dynamic Fields Test",
                description="Testing dynamic field generation from data",
                fields=fields,
                level="info"
            )
            
            logger.info("Dynamic fields test completed")
            self.results["dynamic_fields"] = "SUCCESS"
            
        except Exception as e:
            logger.error(f"Dynamic fields test failed: {e}")
            self.results["dynamic_fields"] = f"ERROR: {e}"
    
    def test_retry_with_alerts(self):
        """Test retry mechanism với Discord alerts"""
        logger.info("=== Testing Retry with Alerts ===")
        
        config = self.configs["development"]
        
        @retriable_with_discord(
            (ConnectionError, TimeoutError),
            config,
            max_attempts=3,
            max_seconds=30,
            notify_every_retry=True
        )
        def simulated_failing_operation():
            """Simulate an operation that might fail"""
            if random.random() < 0.7:  # 70% chance of failure
                raise ConnectionError("Simulated connection error")
            return "Operation completed successfully"
        
        try:
            result = simulated_failing_operation()
            logger.info(f"Retry test result: {result}")
            self.results["retry_with_alerts"] = "SUCCESS"
            
        except Exception as e:
            logger.error(f"Retry with alerts test failed: {e}")
            self.results["retry_with_alerts"] = f"ERROR: {e}"
    
    def test_error_handling(self):
        """Test error handling và fallback"""
        logger.info("=== Testing Error Handling ===")
        
        config = self.configs["development"]
        
        try:
            # Test với invalid webhook URL
            invalid_config = DiscordAlertConfig(
                webhook_url="https://discord.com/api/webhooks/invalid",
                username="Test Bot"
            )
            
            result = send_discord_alert(
                invalid_config,
                title="Test Alert",
                description="This should fail",
                fields={"Status": "TEST"},
                level="info"
            )
            
            logger.info(f"Invalid webhook test result: {result}")
            
            # Test với valid webhook
            result = send_discord_alert(
                config,
                title="✅ Error Handling Test",
                description="Testing error handling and fallback mechanisms",
                fields={
                    "Test Type": "Error Handling",
                    "Status": "SUCCESS",
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                },
                level="info"
            )
            
            logger.info(f"Valid webhook test result: {result}")
            self.results["error_handling"] = "SUCCESS"
            
        except Exception as e:
            logger.error(f"Error handling test failed: {e}")
            self.results["error_handling"] = f"ERROR: {e}"
    
    def test_rate_limiting(self):
        """Test rate limiting"""
        logger.info("=== Testing Rate Limiting ===")
        
        config = self.configs["development"]
        
        try:
            # Send multiple alerts quickly
            for i in range(5):
                send_discord_alert(
                    config,
                    title=f"🔄 Rate Limit Test {i+1}",
                    description=f"Testing rate limiting - alert {i+1} of 5",
                    fields={
                        "Alert Number": str(i+1),
                        "Total Alerts": "5",
                        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    },
                    level="info"
                )
                time.sleep(0.5)  # Small delay between alerts
            
            logger.info("Rate limiting test completed")
            self.results["rate_limiting"] = "SUCCESS"
            
        except Exception as e:
            logger.error(f"Rate limiting test failed: {e}")
            self.results["rate_limiting"] = f"ERROR: {e}"
    
    def test_message_truncation(self):
        """Test message truncation"""
        logger.info("=== Testing Message Truncation ===")
        
        config = self.configs["development"]
        
        try:
            # Tạo message rất dài
            long_description = "This is a very long description that should be truncated if it exceeds Discord's limits. " * 100
            
            long_fields = {}
            for i in range(30):  # Tạo nhiều fields
                long_fields[f"Field {i+1}"] = f"This is a very long field value {i+1} " * 50
            
            result = send_discord_alert(
                config,
                title="📝 Message Truncation Test",
                description=long_description,
                fields=long_fields,
                level="info"
            )
            
            logger.info(f"Message truncation test result: {result}")
            self.results["message_truncation"] = "SUCCESS"
            
        except Exception as e:
            logger.error(f"Message truncation test failed: {e}")
            self.results["message_truncation"] = f"ERROR: {e}"
    
    def run_all_tests(self):
        """Chạy tất cả tests"""
        logger.info("Starting Alert Demo Tests")
        logger.info("=" * 50)
        
        start_time = time.time()
        
        # Run tests
        self.test_basic_alerts()
        self.test_environment_configurations()
        self.test_rich_embeds()
        self.test_dynamic_fields()
        self.test_retry_with_alerts()
        self.test_error_handling()
        self.test_rate_limiting()
        self.test_message_truncation()
        
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
        
        # Send final summary alert
        try:
            config = self.configs["development"]
            send_discord_alert(
                config,
                title="📋 Demo Test Summary",
                description="Discord alert demo tests completed",
                fields={
                    "Tests Passed": f"{successes}/{total}",
                    "Success Rate": f"{(successes/total)*100:.1f}%",
                    "Total Duration": f"{duration:.2f}s",
                    "Status": "COMPLETED"
                },
                level="info"
            )
        except Exception as e:
            logger.error(f"Failed to send summary alert: {e}")

def main():
    """Main function"""
    print("AMI Helpers - Discord Alert Demo")
    print("=" * 50)
    
    # Check for webhook URL
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        print("❌ DISCORD_WEBHOOK_URL environment variable not set")
        print("\nPlease set your Discord webhook URL:")
        print("export DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/YOUR_WEBHOOK_URL'")
        print("\nTo get a webhook URL:")
        print("1. Go to your Discord server")
        print("2. Server Settings → Integrations → Webhooks")
        print("3. Create a new webhook and copy the URL")
        return
    
    print(f"Using webhook URL: {webhook_url[:50]}...")
    print("Starting Discord alert demo tests...")
    print()
    
    # Run demo
    demo = AlertDemo(webhook_url)
    demo.run_all_tests()

if __name__ == "__main__":
    main()

