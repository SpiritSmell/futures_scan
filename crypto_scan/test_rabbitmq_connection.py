#!/usr/bin/env python3
"""
Тест подключения к RabbitMQ для диагностики проблем с отправкой данных.
"""

import asyncio
import json
import logging
from packages.rabbitmq_producer_2_async import AsyncRabbitMQClient

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_rabbitmq_connection():
    """Тест подключения и отправки данных в RabbitMQ."""
    
    # Конфигурация из base_config.yaml
    config = {
        'host': '192.168.192.42',
        'user': 'rmuser', 
        'password': 'rmpassword',
        'exchange': 'futures_price_collector_out'
    }
    
    logger.info("🚀 Starting RabbitMQ connection test...")
    logger.info(f"📡 Connecting to: {config['host']} with user: {config['user']}")
    
    # Создаем клиент
    client = AsyncRabbitMQClient(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        exchange=config['exchange']
    )
    
    try:
        # Тестовые данные
        test_data = {
            'type': 'test',
            'timestamp': '2025-09-14T09:59:00Z',
            'data': {
                'message': 'Test message from crypto collector',
                'test_id': 12345
            },
            'source': 'test_script',
            'environment': 'development'
        }
        
        logger.info("📦 Sending test data to RabbitMQ...")
        
        # Отправляем данные
        success = await client.send_to_rabbitmq(
            data=test_data,
            fanout=True
        )
        
        if success:
            logger.info("✅ Test data sent successfully!")
            logger.info(f"📊 Data sent to exchange: {config['exchange']}")
        else:
            logger.error("❌ Failed to send test data!")
            
        # Закрываем соединение
        await client.close_connection()
        
        return success
        
    except Exception as e:
        logger.error(f"💥 Error during RabbitMQ test: {e}")
        return False

async def main():
    """Главная функция теста."""
    logger.info("=" * 60)
    logger.info("🔍 RabbitMQ Connection Diagnostic Test")
    logger.info("=" * 60)
    
    success = await test_rabbitmq_connection()
    
    logger.info("=" * 60)
    if success:
        logger.info("✅ RabbitMQ test PASSED - connection works!")
    else:
        logger.info("❌ RabbitMQ test FAILED - check connection settings!")
    logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
