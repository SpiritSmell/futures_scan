# 🚀 Deployment Guide - Crypto Futures Price Collector v5

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Environment Setup](#environment-setup)
5. [Docker Deployment](#docker-deployment)
6. [Production Deployment](#production-deployment)
7. [Monitoring & Maintenance](#monitoring--maintenance)
8. [Troubleshooting](#troubleshooting)

---

## 🔧 Prerequisites

### System Requirements

**Minimum Requirements:**
- **OS**: Linux (Ubuntu 20.04+, CentOS 8+) or macOS 10.15+
- **Python**: 3.9+ (recommended: 3.11+)
- **Memory**: 2GB RAM minimum, 4GB+ recommended
- **Storage**: 10GB free space minimum
- **Network**: Stable internet connection with low latency

**Recommended Production Requirements:**
- **OS**: Ubuntu 22.04 LTS or RHEL 9
- **Python**: 3.11+
- **Memory**: 8GB+ RAM
- **CPU**: 4+ cores
- **Storage**: 50GB+ SSD
- **Network**: Dedicated server with 1Gbps+ connection

### External Dependencies

**Required Services:**
- **RabbitMQ**: 3.10+ (message queue)
- **ClickHouse**: 22.0+ or **PostgreSQL**: 14+ (database)
- **Redis**: 6.0+ (optional, for advanced caching)

**Optional Services:**
- **Grafana**: Monitoring dashboards
- **Prometheus**: Metrics collection
- **ELK Stack**: Log aggregation and analysis

---

## 📦 Installation

### 1. Clone Repository

```bash
git clone <repository-url>
cd crypto_scan
```

### 2. Create Virtual Environment

```bash
# Create virtual environment
python3.11 -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # Linux/macOS
# or
.venv\Scripts\activate     # Windows
```

### 3. Install Dependencies

```bash
# Install production dependencies
pip install -r requirements.txt

# Install development dependencies (optional)
pip install -r requirements-dev.txt

# Install testing dependencies (for validation)
pip install pytest pytest-asyncio pytest-cov psutil
```

### 4. Verify Installation

```bash
# Check Python version
python --version

# Verify dependencies
python -c "import pydantic, ccxt, aiohttp, yaml; print('Dependencies OK')"

# Run dependency check
python run_tests.py --check-deps
```

---

## ⚙️ Configuration

### 1. Configuration Files

Create configuration directory structure:

```bash
mkdir -p config logs
```

### 2. Base Configuration

**config/production.yaml:**
```yaml
environment: production
debug: false
app_name: "Crypto Futures Collector"
version: "5.0.0"

# Exchange Configuration
exchanges:
  - binance
  - bybit
  - bitget
  - htx
  - gateio

# Data Collection Intervals
ticker_interval: 30.0        # seconds
funding_rate_interval: 300.0 # seconds

# Performance Settings
performance:
  max_concurrent_requests: 50
  connection_pool_size: 20
  worker_threads: 8
  batch_size: 1000
  flush_interval: 10.0

# Cache Configuration
cache:
  enabled: true
  default_ttl: 300.0
  max_size: 10000

# RabbitMQ Configuration
rabbitmq:
  host: "${RABBITMQ_HOST:localhost}"
  port: 5672
  user: "${RABBITMQ_USER:guest}"
  password: "${RABBITMQ_PASSWORD:guest}"
  exchange: "crypto_data"
  routing_key: "futures.data"

# Database Configuration
database:
  enabled: true
  host: "${DATABASE_HOST:localhost}"
  port: 8123
  database: "${DATABASE_NAME:crypto_data}"
  user: "${DATABASE_USER:default}"
  password: "${DATABASE_PASSWORD:}"

# Logging Configuration
logging:
  level: INFO
  console: true
  file: true
  file_path: "./logs/collector.log"
  max_file_size: "100MB"
  backup_count: 5

# Circuit Breaker Settings
circuit_breaker:
  failure_threshold: 5
  recovery_timeout: 60.0
  success_threshold: 3
  timeout: 30.0

# Retry Settings
retry:
  max_attempts: 3
  base_delay: 1.0
  max_delay: 30.0
  strategy: "adaptive"

# Health Check Settings
health_check:
  check_interval: 120.0
  timeout: 30.0
  failure_threshold: 3
  recovery_threshold: 2
```

### 3. Environment-Specific Overrides

**config/production.override.yaml:**
```yaml
# Production-specific overrides
debug: false
ticker_interval: 15.0
logging:
  level: WARNING
  console: false

performance:
  max_concurrent_requests: 100
  connection_pool_size: 50
```

### 4. Exchange-Specific Configuration

```yaml
exchange_configs:
  binance:
    name: "binance"
    enabled: true
    timeout: 30.0
    rate_limit: 1200
    sandbox: false
    api_key: "${BINANCE_API_KEY:}"
    api_secret: "${BINANCE_API_SECRET:}"
  
  bybit:
    name: "bybit"
    enabled: true
    timeout: 25.0
    rate_limit: 600
    sandbox: false
    api_key: "${BYBIT_API_KEY:}"
    api_secret: "${BYBIT_API_SECRET:}"
```

---

## 🌍 Environment Setup

### 1. Environment Variables

Create `.env` file:

```bash
# Application Settings
CRYPTO_COLLECTOR_ENVIRONMENT=production
CRYPTO_COLLECTOR_DEBUG=false

# RabbitMQ Configuration
CRYPTO_COLLECTOR_RABBITMQ__HOST=prod-rabbitmq.company.com
CRYPTO_COLLECTOR_RABBITMQ__PORT=5672
CRYPTO_COLLECTOR_RABBITMQ__USER=crypto_collector
CRYPTO_COLLECTOR_RABBITMQ__PASSWORD=secure_password_123

# Database Configuration
CRYPTO_COLLECTOR_DATABASE__HOST=prod-clickhouse.company.com
CRYPTO_COLLECTOR_DATABASE__PORT=8123
CRYPTO_COLLECTOR_DATABASE__DATABASE=crypto_production
CRYPTO_COLLECTOR_DATABASE__USER=crypto_user
CRYPTO_COLLECTOR_DATABASE__PASSWORD=secure_db_password

# Exchange API Keys (if needed)
CRYPTO_COLLECTOR_EXCHANGE_CONFIGS__BINANCE__API_KEY=your_binance_api_key
CRYPTO_COLLECTOR_EXCHANGE_CONFIGS__BINANCE__API_SECRET=your_binance_api_secret

# Performance Tuning
CRYPTO_COLLECTOR_TICKER_INTERVAL=15.0
CRYPTO_COLLECTOR_PERFORMANCE__MAX_CONCURRENT_REQUESTS=100
CRYPTO_COLLECTOR_PERFORMANCE__CONNECTION_POOL_SIZE=50

# Logging
CRYPTO_COLLECTOR_LOGGING__LEVEL=INFO
CRYPTO_COLLECTOR_LOGGING__FILE_PATH=/var/log/crypto_collector/collector.log
```

### 2. Security Considerations

```bash
# Set proper file permissions
chmod 600 .env
chmod 644 config/*.yaml

# Create log directory with proper permissions
sudo mkdir -p /var/log/crypto_collector
sudo chown $USER:$USER /var/log/crypto_collector
chmod 755 /var/log/crypto_collector
```

---

## 🐳 Docker Deployment

### 1. Dockerfile

```dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 crypto_collector && \
    chown -R crypto_collector:crypto_collector /app
USER crypto_collector

# Create logs directory
RUN mkdir -p logs

# Expose health check port (if applicable)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Run application
CMD ["python", "futures_price_collector_v5.py", "--config=./config/production.yaml"]
```

### 2. Docker Compose

**docker-compose.yml:**
```yaml
version: '3.8'

services:
  crypto-collector:
    build: .
    container_name: crypto_collector_v5
    restart: unless-stopped
    environment:
      - CRYPTO_COLLECTOR_ENVIRONMENT=production
      - CRYPTO_COLLECTOR_RABBITMQ__HOST=rabbitmq
      - CRYPTO_COLLECTOR_DATABASE__HOST=clickhouse
    volumes:
      - ./config:/app/config:ro
      - ./logs:/app/logs
      - ./.env:/app/.env:ro
    depends_on:
      - rabbitmq
      - clickhouse
    networks:
      - crypto_network

  rabbitmq:
    image: rabbitmq:3.11-management
    container_name: crypto_rabbitmq
    restart: unless-stopped
    environment:
      - RABBITMQ_DEFAULT_USER=crypto_collector
      - RABBITMQ_DEFAULT_PASS=secure_password_123
    ports:
      - "5672:5672"
      - "15672:15672"
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    networks:
      - crypto_network

  clickhouse:
    image: clickhouse/clickhouse-server:22.12
    container_name: crypto_clickhouse
    restart: unless-stopped
    environment:
      - CLICKHOUSE_DB=crypto_data
      - CLICKHOUSE_USER=crypto_user
      - CLICKHOUSE_PASSWORD=secure_db_password
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./database/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    networks:
      - crypto_network

volumes:
  rabbitmq_data:
  clickhouse_data:

networks:
  crypto_network:
    driver: bridge
```

### 3. Build and Deploy

```bash
# Build Docker image
docker build -t crypto-collector:v5 .

# Deploy with Docker Compose
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f crypto-collector
```

---

## 🏭 Production Deployment

### 1. Systemd Service

**/etc/systemd/system/crypto-collector.service:**
```ini
[Unit]
Description=Crypto Futures Price Collector v5
After=network.target
Wants=network.target

[Service]
Type=simple
User=crypto_collector
Group=crypto_collector
WorkingDirectory=/opt/crypto_collector
Environment=PATH=/opt/crypto_collector/.venv/bin
ExecStart=/opt/crypto_collector/.venv/bin/python futures_price_collector_v5.py --config=./config/production.yaml
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=crypto-collector

# Resource limits
LimitNOFILE=65536
LimitNPROC=32768

# Security settings
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/crypto_collector/logs
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

### 2. Installation Script

**install.sh:**
```bash
#!/bin/bash
set -e

# Configuration
APP_USER="crypto_collector"
APP_DIR="/opt/crypto_collector"
SERVICE_NAME="crypto-collector"

echo "🚀 Installing Crypto Futures Price Collector v5..."

# Create user
if ! id "$APP_USER" &>/dev/null; then
    sudo useradd -r -s /bin/false -d "$APP_DIR" "$APP_USER"
    echo "✅ Created user: $APP_USER"
fi

# Create directories
sudo mkdir -p "$APP_DIR"
sudo mkdir -p "$APP_DIR/logs"
sudo mkdir -p "$APP_DIR/config"

# Copy application files
sudo cp -r . "$APP_DIR/"
sudo chown -R "$APP_USER:$APP_USER" "$APP_DIR"

# Install Python dependencies
cd "$APP_DIR"
sudo -u "$APP_USER" python3.11 -m venv .venv
sudo -u "$APP_USER" .venv/bin/pip install -r requirements.txt

# Install systemd service
sudo cp deployment/crypto-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"

echo "✅ Installation complete!"
echo "📝 Next steps:"
echo "   1. Configure environment variables in $APP_DIR/.env"
echo "   2. Update configuration in $APP_DIR/config/production.yaml"
echo "   3. Start service: sudo systemctl start $SERVICE_NAME"
```

### 3. Service Management

```bash
# Install service
sudo chmod +x install.sh
sudo ./install.sh

# Start service
sudo systemctl start crypto-collector
sudo systemctl enable crypto-collector

# Check status
sudo systemctl status crypto-collector

# View logs
sudo journalctl -u crypto-collector -f

# Restart service
sudo systemctl restart crypto-collector

# Stop service
sudo systemctl stop crypto-collector
```

---

## 📊 Monitoring & Maintenance

### 1. Health Monitoring

**health_check.sh:**
```bash
#!/bin/bash

# Check service status
if ! systemctl is-active --quiet crypto-collector; then
    echo "❌ Service is not running"
    exit 1
fi

# Check log for errors
if journalctl -u crypto-collector --since "5 minutes ago" | grep -q "ERROR"; then
    echo "⚠️  Recent errors detected in logs"
    exit 1
fi

# Check memory usage
MEMORY_USAGE=$(ps -o pid,ppid,cmd,%mem --sort=-%mem -C python | grep crypto | head -1 | awk '{print $4}')
if (( $(echo "$MEMORY_USAGE > 80" | bc -l) )); then
    echo "⚠️  High memory usage: ${MEMORY_USAGE}%"
    exit 1
fi

echo "✅ Health check passed"
exit 0
```

### 2. Log Rotation

**/etc/logrotate.d/crypto-collector:**
```
/opt/crypto_collector/logs/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 644 crypto_collector crypto_collector
    postrotate
        systemctl reload crypto-collector
    endscript
}
```

### 3. Backup Script

**backup.sh:**
```bash
#!/bin/bash

BACKUP_DIR="/backup/crypto_collector"
DATE=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Backup configuration
tar -czf "$BACKUP_DIR/config_$DATE.tar.gz" /opt/crypto_collector/config/

# Backup logs (last 7 days)
find /opt/crypto_collector/logs/ -name "*.log" -mtime -7 -exec tar -czf "$BACKUP_DIR/logs_$DATE.tar.gz" {} +

# Cleanup old backups (keep 30 days)
find "$BACKUP_DIR" -name "*.tar.gz" -mtime +30 -delete

echo "✅ Backup completed: $BACKUP_DIR"
```

### 4. Performance Monitoring

**monitor.py:**
```python
#!/usr/bin/env python3
import psutil
import requests
import json
import time

def check_system_resources():
    """Check system resource usage."""
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    return {
        'cpu_percent': cpu_percent,
        'memory_percent': memory.percent,
        'disk_percent': disk.percent,
        'timestamp': time.time()
    }

def check_application_health():
    """Check application-specific metrics."""
    # This would integrate with your application's health endpoint
    # For now, check if process is running
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        if 'futures_price_collector_v5.py' in ' '.join(proc.info['cmdline'] or []):
            return {
                'status': 'running',
                'pid': proc.info['pid'],
                'memory_mb': proc.memory_info().rss / 1024 / 1024
            }
    
    return {'status': 'not_running'}

if __name__ == "__main__":
    system_metrics = check_system_resources()
    app_metrics = check_application_health()
    
    print(f"System CPU: {system_metrics['cpu_percent']:.1f}%")
    print(f"System Memory: {system_metrics['memory_percent']:.1f}%")
    print(f"System Disk: {system_metrics['disk_percent']:.1f}%")
    print(f"App Status: {app_metrics['status']}")
    
    if app_metrics['status'] == 'running':
        print(f"App Memory: {app_metrics['memory_mb']:.1f} MB")
```

---

## 🔧 Troubleshooting

### Common Issues

#### 1. Service Won't Start

**Symptoms:**
- Service fails to start
- Immediate exit after startup

**Solutions:**
```bash
# Check service status
sudo systemctl status crypto-collector

# Check logs
sudo journalctl -u crypto-collector -n 50

# Check configuration
python futures_price_collector_v5.py --config=./config/production.yaml --validate-config

# Check permissions
ls -la /opt/crypto_collector/
sudo chown -R crypto_collector:crypto_collector /opt/crypto_collector/
```

#### 2. High Memory Usage

**Symptoms:**
- Memory usage continuously growing
- System becomes unresponsive

**Solutions:**
```bash
# Monitor memory usage
watch -n 5 'ps aux | grep crypto | head -5'

# Check cache settings in configuration
# Reduce cache sizes if needed

# Restart service
sudo systemctl restart crypto-collector
```

#### 3. Exchange Connection Issues

**Symptoms:**
- "Exchange not available" errors
- High failure rates

**Solutions:**
```bash
# Check network connectivity
ping api.binance.com
curl -I https://api.binance.com/api/v3/ping

# Verify API keys
# Check rate limiting settings
# Review circuit breaker thresholds
```

#### 4. Database Connection Issues

**Symptoms:**
- Database insert failures
- Connection timeout errors

**Solutions:**
```bash
# Test database connection
clickhouse-client --host localhost --port 8123

# Check database configuration
# Verify credentials
# Check connection pool settings
```

### Debug Mode

Enable debug mode for troubleshooting:

```yaml
# config/debug.yaml
debug: true
logging:
  level: DEBUG
  console: true
  file: true
```

```bash
# Run in debug mode
python futures_price_collector_v5.py --config=./config/debug.yaml
```

### Performance Tuning

#### Memory Optimization
```yaml
cache:
  max_size: 5000  # Reduce if memory constrained

performance:
  connection_pool_size: 10  # Reduce for lower memory usage
  batch_size: 500          # Smaller batches
```

#### CPU Optimization
```yaml
performance:
  worker_threads: 4        # Match CPU cores
  max_concurrent_requests: 25  # Reduce if CPU bound
```

#### Network Optimization
```yaml
# Increase timeouts for slow networks
exchange_configs:
  binance:
    timeout: 60.0
    rate_limit: 800  # Reduce rate limit
```

---

## 📈 Scaling Considerations

### Horizontal Scaling

1. **Multiple Instances**: Run multiple collector instances with different exchange assignments
2. **Load Balancing**: Use load balancer for API endpoints
3. **Database Sharding**: Partition data by exchange or time
4. **Message Queue Clustering**: Use RabbitMQ clustering

### Vertical Scaling

1. **Increase Resources**: More CPU, memory, and storage
2. **Optimize Configuration**: Tune cache sizes and connection pools
3. **Database Optimization**: Optimize queries and indexes

### Monitoring at Scale

1. **Centralized Logging**: Use ELK stack or similar
2. **Metrics Collection**: Prometheus + Grafana
3. **Alerting**: Set up alerts for critical metrics
4. **Health Checks**: Automated health monitoring

---

## 🔒 Security Best Practices

### 1. API Key Management
- Use environment variables for sensitive data
- Rotate API keys regularly
- Use read-only API keys when possible
- Monitor API key usage

### 2. Network Security
- Use VPN or private networks
- Implement firewall rules
- Use TLS/SSL for all connections
- Regular security updates

### 3. Application Security
- Run with minimal privileges
- Use security-focused Docker images
- Regular dependency updates
- Security scanning

### 4. Data Protection
- Encrypt sensitive data at rest
- Use secure communication protocols
- Implement access controls
- Regular security audits

---

*This deployment guide provides comprehensive instructions for deploying the Crypto Futures Price Collector v5 in various environments. For additional support, refer to the API documentation and troubleshooting guides.*
