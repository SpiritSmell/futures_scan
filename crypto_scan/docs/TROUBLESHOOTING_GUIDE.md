# 🔧 Troubleshooting Guide - Crypto Futures Price Collector v5

## 📋 Table of Contents

1. [Quick Diagnostics](#quick-diagnostics)
2. [Common Issues](#common-issues)
3. [Configuration Problems](#configuration-problems)
4. [Exchange Connection Issues](#exchange-connection-issues)
5. [Performance Problems](#performance-problems)
6. [Database & Messaging Issues](#database--messaging-issues)
7. [Memory & Resource Issues](#memory--resource-issues)
8. [Debugging Tools](#debugging-tools)
9. [Error Code Reference](#error-code-reference)
10. [Recovery Procedures](#recovery-procedures)

---

## 🩺 Quick Diagnostics

### System Health Check

Run this comprehensive health check first:

```bash
# 1. Check service status
sudo systemctl status crypto-collector

# 2. Check recent logs
sudo journalctl -u crypto-collector --since "10 minutes ago" -n 50

# 3. Check system resources
python3 -c "
import psutil
print(f'CPU: {psutil.cpu_percent()}%')
print(f'Memory: {psutil.virtual_memory().percent}%')
print(f'Disk: {psutil.disk_usage(\"/\").percent}%')
"

# 4. Test configuration
python futures_price_collector_v5.py --config=./config/production.yaml --validate-config

# 5. Run health check script
python run_tests.py --check-deps
```

### Quick Status Commands

```bash
# Service status
systemctl is-active crypto-collector

# Check if process is running
pgrep -f "futures_price_collector_v5.py"

# Check listening ports
netstat -tlnp | grep python

# Check log file size
ls -lh logs/collector.log

# Check configuration syntax
python -c "import yaml; yaml.safe_load(open('config/production.yaml'))"
```

---

## ⚠️ Common Issues

### 1. Service Won't Start

**Symptoms:**
```
● crypto-collector.service - Crypto Futures Price Collector v5
   Loaded: loaded (/etc/systemd/system/crypto-collector.service; enabled)
   Active: failed (Result: exit-code) since...
```

**Diagnostic Steps:**
```bash
# Check detailed service status
sudo systemctl status crypto-collector -l

# Check service logs
sudo journalctl -u crypto-collector -f

# Try manual start
cd /opt/crypto_collector
sudo -u crypto_collector .venv/bin/python futures_price_collector_v5.py --config=./config/production.yaml
```

**Common Causes & Solutions:**

| Cause | Solution |
|-------|----------|
| **Permission Issues** | `sudo chown -R crypto_collector:crypto_collector /opt/crypto_collector/` |
| **Missing Dependencies** | `pip install -r requirements.txt` |
| **Invalid Configuration** | Validate YAML syntax and required fields |
| **Port Already in Use** | Check for conflicting processes: `netstat -tlnp \| grep :8080` |
| **Missing Environment Variables** | Check `.env` file and environment variable format |

### 2. High CPU Usage

**Symptoms:**
- CPU usage consistently above 80%
- System becomes sluggish
- High load average

**Diagnostic Commands:**
```bash
# Check CPU usage by process
top -p $(pgrep -f futures_price_collector_v5.py)

# Check thread usage
ps -eLf | grep futures_price_collector_v5.py | wc -l

# Monitor system load
uptime
```

**Solutions:**
```yaml
# Reduce concurrent requests
performance:
  max_concurrent_requests: 25  # Reduce from 50
  worker_threads: 2           # Match CPU cores

# Increase intervals
ticker_interval: 60.0         # Reduce frequency
funding_rate_interval: 600.0  # Reduce frequency
```

### 3. Memory Leaks

**Symptoms:**
- Memory usage continuously growing
- System runs out of memory
- OOM killer activates

**Diagnostic Commands:**
```bash
# Monitor memory usage over time
watch -n 5 'ps aux | grep futures_price_collector_v5.py'

# Check memory details
cat /proc/$(pgrep -f futures_price_collector_v5.py)/status | grep -E "VmRSS|VmSize"

# Check for memory leaks
valgrind --tool=memcheck --leak-check=full python futures_price_collector_v5.py
```

**Solutions:**
```yaml
# Reduce cache sizes
cache:
  max_size: 1000              # Reduce from 10000
  default_ttl: 60.0           # Shorter TTL

# Reduce connection pool
performance:
  connection_pool_size: 5     # Reduce from 20
  batch_size: 100            # Smaller batches
```

---

## ⚙️ Configuration Problems

### 1. YAML Syntax Errors

**Error Messages:**
```
yaml.scanner.ScannerError: mapping values are not allowed here
pydantic.ValidationError: 1 validation error for AppConfig
```

**Diagnostic Steps:**
```bash
# Validate YAML syntax
python -c "import yaml; yaml.safe_load(open('config/production.yaml'))"

# Check for common YAML issues
grep -n ":" config/production.yaml | head -10
```

**Common YAML Issues:**
- **Indentation**: Use spaces, not tabs
- **Colons**: Space required after colons
- **Quotes**: Use quotes for strings with special characters
- **Lists**: Proper list formatting with `-`

### 2. Environment Variable Issues

**Error Messages:**
```
KeyError: 'RABBITMQ_HOST'
pydantic.ValidationError: field required
```

**Diagnostic Steps:**
```bash
# Check environment variables
env | grep CRYPTO_COLLECTOR

# Test environment variable expansion
python -c "
import os
from config_manager import ConfigManager
cm = ConfigManager('./config/production.yaml')
config = cm.load_config()
print('Config loaded successfully')
"
```

**Solutions:**
```bash
# Check .env file format
cat .env | grep -v "^#" | grep -v "^$"

# Verify environment variable names
# Format: CRYPTO_COLLECTOR_SECTION__FIELD
# Example: CRYPTO_COLLECTOR_RABBITMQ__HOST=localhost
```

### 3. Validation Errors

**Error Messages:**
```
pydantic.ValidationError: 1 validation error for ExchangeConfig
  timeout
    ensure this value is greater than or equal to 1.0
```

**Common Validation Issues:**

| Field | Constraint | Fix |
|-------|------------|-----|
| `ticker_interval` | >= 1.0, <= 3600.0 | Use value between 1-3600 seconds |
| `timeout` | >= 1.0, <= 300.0 | Use value between 1-300 seconds |
| `rate_limit` | >= 100, <= 10000 | Use value between 100-10000 |
| `port` | 1-65535 | Use valid port number |

---

## 🏢 Exchange Connection Issues

### 1. API Connection Failures

**Error Messages:**
```
ccxt.NetworkError: binance GET https://api.binance.com/api/v3/exchangeInfo failed
ExchangeError: Exchange binance not available
```

**Diagnostic Steps:**
```bash
# Test network connectivity
ping api.binance.com
curl -I https://api.binance.com/api/v3/ping

# Test with different exchange
curl -I https://api.bybit.com/v2/public/time

# Check DNS resolution
nslookup api.binance.com
```

**Solutions:**

| Issue | Solution |
|-------|----------|
| **Network Connectivity** | Check firewall, proxy settings |
| **DNS Issues** | Use public DNS (8.8.8.8) or check /etc/resolv.conf |
| **Rate Limiting** | Reduce rate_limit in configuration |
| **API Maintenance** | Check exchange status pages |

### 2. Authentication Errors

**Error Messages:**
```
ccxt.AuthenticationError: binance {"code":-2014,"msg":"API-key format invalid."}
ccxt.PermissionDenied: binance {"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}
```

**Diagnostic Steps:**
```bash
# Check API key format
echo $CRYPTO_COLLECTOR_EXCHANGE_CONFIGS__BINANCE__API_KEY | wc -c

# Test API key manually
curl -H "X-MBX-APIKEY: YOUR_API_KEY" https://api.binance.com/api/v3/account
```

**Solutions:**
- Verify API key and secret are correct
- Check API key permissions (read-only sufficient)
- Verify IP whitelist settings
- Ensure API key is not expired

### 3. Rate Limiting Issues

**Error Messages:**
```
ccxt.RateLimitExceeded: binance {"code":-1003,"msg":"Way too many requests"}
```

**Solutions:**
```yaml
# Reduce rate limits
exchange_configs:
  binance:
    rate_limit: 800           # Reduce from 1200
    timeout: 60.0            # Increase timeout

# Increase intervals
ticker_interval: 45.0        # Increase from 30.0
```

---

## 🚀 Performance Problems

### 1. Slow Response Times

**Symptoms:**
- High average response times (>5 seconds)
- Timeouts occurring frequently
- Circuit breakers opening

**Diagnostic Commands:**
```bash
# Monitor response times
tail -f logs/collector.log | grep "response_time"

# Check network latency
ping -c 10 api.binance.com
```

**Solutions:**
```yaml
# Optimize timeouts
exchange_configs:
  binance:
    timeout: 45.0            # Increase timeout

# Reduce concurrent requests
performance:
  max_concurrent_requests: 20 # Reduce load

# Enable caching
cache:
  enabled: true
  default_ttl: 120.0         # Cache responses
```

### 2. Low Throughput

**Symptoms:**
- Processing fewer requests than expected
- Queues building up
- Data delays

**Solutions:**
```yaml
# Increase concurrency
performance:
  max_concurrent_requests: 75
  worker_threads: 6
  connection_pool_size: 30

# Optimize batching
batch_processing:
  batch_size: 2000
  flush_interval: 5.0
```

---

## 💾 Database & Messaging Issues

### 1. RabbitMQ Connection Issues

**Error Messages:**
```
ConnectionError: Failed to connect to RabbitMQ at localhost:5672
pika.exceptions.AMQPConnectionError: Connection to localhost:5672 failed
```

**Diagnostic Steps:**
```bash
# Check RabbitMQ service
sudo systemctl status rabbitmq-server

# Test connection
telnet localhost 5672

# Check RabbitMQ management
curl -u guest:guest http://localhost:15672/api/overview
```

**Solutions:**
```bash
# Start RabbitMQ
sudo systemctl start rabbitmq-server

# Check RabbitMQ logs
sudo journalctl -u rabbitmq-server -f

# Reset RabbitMQ (if needed)
sudo rabbitmqctl stop_app
sudo rabbitmqctl reset
sudo rabbitmqctl start_app
```

### 2. Database Connection Issues

**Error Messages:**
```
ClickHouseException: Code: 516, Connection refused
psycopg2.OperationalError: could not connect to server
```

**Diagnostic Steps:**
```bash
# Test ClickHouse connection
clickhouse-client --host localhost --port 8123 --query "SELECT 1"

# Test PostgreSQL connection
psql -h localhost -p 5432 -U crypto_user -d crypto_data -c "SELECT 1"

# Check database service
sudo systemctl status clickhouse-server
```

**Solutions:**
```yaml
# Increase connection timeout
database:
  timeout: 60.0
  connection_pool_size: 10

# Add retry configuration
retry:
  max_attempts: 5
  base_delay: 2.0
```

---

## 🧠 Memory & Resource Issues

### 1. Out of Memory Errors

**Error Messages:**
```
MemoryError: Unable to allocate memory
OSError: [Errno 12] Cannot allocate memory
```

**Diagnostic Steps:**
```bash
# Check available memory
free -h

# Check swap usage
swapon --show

# Monitor memory usage
watch -n 2 'cat /proc/meminfo | head -5'
```

**Solutions:**
```yaml
# Reduce memory usage
cache:
  max_size: 500              # Reduce cache
performance:
  connection_pool_size: 5    # Reduce connections
  batch_size: 100           # Smaller batches

# Enable swap (if needed)
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

### 2. File Descriptor Limits

**Error Messages:**
```
OSError: [Errno 24] Too many open files
```

**Solutions:**
```bash
# Check current limits
ulimit -n

# Increase limits temporarily
ulimit -n 65536

# Permanent fix in /etc/security/limits.conf
crypto_collector soft nofile 65536
crypto_collector hard nofile 65536
```

---

## 🔍 Debugging Tools

### 1. Enable Debug Mode

**config/debug.yaml:**
```yaml
debug: true
logging:
  level: DEBUG
  console: true
  file: true
  file_path: "./logs/debug.log"

# Reduce intervals for faster debugging
ticker_interval: 10.0
funding_rate_interval: 60.0
```

### 2. Performance Profiling

```python
# profile_collector.py
import cProfile
import pstats
import sys
import asyncio

# Add your main function import here
from futures_price_collector_v5 import main

def profile_main():
    """Profile the main application."""
    profiler = cProfile.Profile()
    profiler.enable()
    
    # Run the application
    asyncio.run(main())
    
    profiler.disable()
    
    # Save profile results
    profiler.dump_stats('collector_profile.prof')
    
    # Print top functions
    stats = pstats.Stats('collector_profile.prof')
    stats.sort_stats('cumulative')
    stats.print_stats(20)

if __name__ == "__main__":
    profile_main()
```

### 3. Memory Profiling

```python
# memory_profile.py
from memory_profiler import profile
import psutil
import os

@profile
def monitor_memory():
    """Monitor memory usage of the collector."""
    process = psutil.Process(os.getpid())
    
    print(f"Memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")
    print(f"Memory percent: {process.memory_percent():.2f}%")
    
    # Add your application logic here
    
if __name__ == "__main__":
    monitor_memory()
```

### 4. Network Debugging

```bash
# Monitor network connections
netstat -tulpn | grep python

# Monitor network traffic
sudo tcpdump -i any -n host api.binance.com

# Check SSL/TLS issues
openssl s_client -connect api.binance.com:443 -servername api.binance.com
```

---

## 📊 Error Code Reference

### Application Error Codes

| Code | Description | Severity | Action |
|------|-------------|----------|---------|
| **CONFIG_001** | Invalid configuration format | High | Fix YAML syntax |
| **CONFIG_002** | Missing required field | High | Add missing configuration |
| **CONFIG_003** | Validation error | High | Fix field values |
| **EXCHANGE_001** | Connection failed | Medium | Check network/API |
| **EXCHANGE_002** | Authentication failed | High | Check API keys |
| **EXCHANGE_003** | Rate limit exceeded | Low | Reduce request rate |
| **CIRCUIT_001** | Circuit breaker open | Medium | Wait for recovery |
| **RETRY_001** | Max retries exceeded | Medium | Check underlying issue |
| **CACHE_001** | Cache operation failed | Low | Check memory usage |
| **DB_001** | Database connection failed | High | Check database service |
| **MQ_001** | Message queue error | Medium | Check RabbitMQ |

### System Error Codes

| Code | Description | Action |
|------|-------------|---------|
| **Exit 0** | Normal termination | No action needed |
| **Exit 1** | General error | Check logs |
| **Exit 2** | Misuse of shell command | Check command syntax |
| **Exit 126** | Command not executable | Check permissions |
| **Exit 127** | Command not found | Check PATH |
| **Exit 130** | Script terminated by Ctrl+C | Normal interruption |

---

## 🔄 Recovery Procedures

### 1. Service Recovery

```bash
# Standard recovery procedure
sudo systemctl stop crypto-collector
sleep 5
sudo systemctl start crypto-collector
sudo systemctl status crypto-collector

# If service won't start
sudo systemctl reset-failed crypto-collector
sudo systemctl daemon-reload
sudo systemctl start crypto-collector
```

### 2. Database Recovery

```bash
# ClickHouse recovery
sudo systemctl restart clickhouse-server

# Check database integrity
clickhouse-client --query "SELECT COUNT(*) FROM crypto_data.tickers"

# Repair if needed (ClickHouse)
clickhouse-client --query "OPTIMIZE TABLE crypto_data.tickers FINAL"
```

### 3. Configuration Recovery

```bash
# Backup current config
cp config/production.yaml config/production.yaml.backup

# Restore from known good configuration
cp config/production.yaml.known_good config/production.yaml

# Validate restored configuration
python futures_price_collector_v5.py --config=./config/production.yaml --validate-config
```

### 4. Emergency Procedures

**Complete System Reset:**
```bash
# Stop all services
sudo systemctl stop crypto-collector
sudo systemctl stop rabbitmq-server
sudo systemctl stop clickhouse-server

# Clear temporary data
rm -rf logs/*.log
rm -rf /tmp/crypto_collector_*

# Start services in order
sudo systemctl start clickhouse-server
sudo systemctl start rabbitmq-server
sleep 10
sudo systemctl start crypto-collector
```

**Data Recovery:**
```bash
# Restore from backup
sudo systemctl stop crypto-collector
cp -r /backup/crypto_collector/config/* /opt/crypto_collector/config/
sudo systemctl start crypto-collector
```

---

## 📞 Getting Help

### Log Analysis

Always include these logs when seeking help:

```bash
# System logs
sudo journalctl -u crypto-collector --since "1 hour ago" > system.log

# Application logs
tail -n 1000 logs/collector.log > application.log

# System information
uname -a > system_info.txt
python --version >> system_info.txt
pip list > installed_packages.txt
```

### Support Information

When reporting issues, include:

1. **System Information**: OS, Python version, hardware specs
2. **Configuration**: Sanitized configuration files (remove secrets)
3. **Logs**: Recent application and system logs
4. **Error Messages**: Complete error messages and stack traces
5. **Steps to Reproduce**: Detailed steps that led to the issue
6. **Expected vs Actual Behavior**: What should happen vs what happens

### Performance Baseline

Normal performance metrics for reference:

- **Startup Time**: < 10 seconds
- **Memory Usage**: 50-200 MB (depending on cache size)
- **CPU Usage**: < 20% average
- **Response Time**: < 2 seconds average
- **Success Rate**: > 95% with resilience components

---

*This troubleshooting guide covers the most common issues and their solutions. For complex issues, enable debug mode and analyze the detailed logs for more specific error information.*
