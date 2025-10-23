"""
Futures Price Collector v3 - Оптимизированная версия с компонентами производительности.
Включает кэширование, connection pooling, batch processing и детальный мониторинг.
"""

import asyncio
import logging
import sys
import os
from pathlib import Path

# Добавляем путь к пакетам
sys.path.append(str(Path(__file__).parent))

from packages.app_template import AppTemplate
from packages.orchestrator_v2 import OptimizedCryptoDataOrchestrator


def setup_logging():
    """Настройка расширенного логирования с метриками производительности."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('crypto_collector_v3.log')
        ]
    )
    
    # Устанавливаем уровень DEBUG для наших модулей
    logging.getLogger('packages').setLevel(logging.DEBUG)
    
    # Снижаем уровень для внешних библиотек
    logging.getLogger('ccxt').setLevel(logging.WARNING)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)


async def main():
    """Главная функция с оптимизированной архитектурой."""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 60)
        logger.info("Starting Crypto Futures Price Collector v3 (Optimized)")
        logger.info("=" * 60)
        
        # Загружаем конфигурацию
        logger.info("Loading configuration...")
        config_template = AppTemplate([('config=', "<config filename>")])
        
        # Получаем параметры из командной строки и окружения
        config_template.get_arguments_from_env()
        config_template.get_arguments()
        
        # Загружаем настройки из файла конфигурации
        config_template.load_settings_from_file()
        config = config_template.settings
        
        if not config:
            logger.error("Failed to load configuration")
            return 1
        
        # Добавляем настройки производительности
        performance_config = {
            # Интервалы сбора данных
            'ticker_interval': 30,      # секунды
            'funding_interval': 300,    # секунды
            'monitoring_interval': 60,  # секунды
            
            # Настройки batch processing
            'batch_size': 50,
            'batch_wait_time': 2.0,
            'batch_strategy': 'hybrid',
            'batch_compression': True,
            
            # Настройки кэширования
            'cache_enabled': True,
            'ticker_cache_ttl': 30,
            'funding_cache_ttl': 300,
            
            # Настройки connection pooling
            'max_connections_per_exchange': 10,
            'connection_timeout': 30,
            'enable_adaptive_rate_limiting': True,
            
            # Настройки RabbitMQ
            'host': config.get('host', 'localhost'),
            'user': config.get('user', 'rmuser'),
            'password': config.get('password', 'rmpassword'),
            'out_exchange': config.get('out_exchange', 'futures_price_collector_out')
        }
        
        config.update(performance_config)
        
        logger.info("Configuration loaded successfully")
        logger.info(f"Performance optimizations enabled:")
        logger.info(f"  - Caching: {config['cache_enabled']}")
        logger.info(f"  - Batch processing: {config['batch_size']} items max")
        logger.info(f"  - Connection pooling: {config['max_connections_per_exchange']} per exchange")
        logger.info(f"  - Adaptive rate limiting: {config['enable_adaptive_rate_limiting']}")
        
        # Создаем и запускаем оптимизированный оркестратор
        logger.info("Creating optimized orchestrator...")
        orchestrator = OptimizedCryptoDataOrchestrator(config)
        
        logger.info("Starting orchestrator with performance components...")
        await orchestrator.start()
        
        # Ждем некоторое время для демонстрации работы
        logger.info("System is running. Press Ctrl+C to stop...")
        
        try:
            # Основной цикл работы
            while True:
                await asyncio.sleep(60)  # Проверяем каждую минуту
                
                # Получаем и логируем статус системы
                status = orchestrator.get_status()
                logger.info(f"System Status - Efficiency: {status['performance_metrics']['overall_efficiency']:.1f}%, "
                           f"Memory: {status['performance_metrics']['memory_usage_mb']:.1f}MB, "
                           f"Exchanges: {status['exchanges']['healthy']}/{status['exchanges']['total']}")
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        
        # Graceful shutdown
        logger.info("Stopping orchestrator...")
        await orchestrator.stop()
        
        # Финальные метрики
        logger.info("=" * 60)
        logger.info("Final Performance Report:")
        logger.info("=" * 60)
        
        final_metrics = await orchestrator.get_detailed_metrics()
        
        # Логируем основные метрики
        perf_metrics = final_metrics.get('orchestrator', {}).get('performance_metrics', {})
        logger.info(f"Overall Efficiency: {perf_metrics.get('overall_efficiency', 0):.1f}%")
        logger.info(f"Cache Hit Rate: {perf_metrics.get('cache_hit_rate', 0):.1f}%")
        logger.info(f"Connection Pool Efficiency: {perf_metrics.get('connection_pool_efficiency', 0):.1f}%")
        logger.info(f"Batch Processing Efficiency: {perf_metrics.get('batch_processing_efficiency', 0):.1f}%")
        logger.info(f"Total Uptime: {perf_metrics.get('uptime_seconds', 0):.0f} seconds")
        
        # Логируем метрики кэша
        cache_metrics = final_metrics.get('cache', {})
        if cache_metrics:
            logger.info("Cache Performance:")
            for cache_type, stats in cache_metrics.items():
                if hasattr(stats, 'hit_ratio'):
                    logger.info(f"  {cache_type}: {stats.hit_ratio:.1f}% hit rate, {stats.size} items")
        
        # Логируем метрики connection pool
        pool_metrics = final_metrics.get('connection_pools', {})
        if pool_metrics:
            logger.info("Connection Pool Performance:")
            for exchange, stats in pool_metrics.items():
                if hasattr(stats, 'success_rate'):
                    logger.info(f"  {exchange}: {stats.success_rate:.1f}% success rate, "
                               f"{stats.avg_response_time:.3f}s avg response")
        
        logger.info("=" * 60)
        logger.info("Crypto Futures Price Collector v3 stopped successfully")
        logger.info("=" * 60)
        
        return 0
        
    except Exception as e:
        logger.error(f"Critical error in main: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    # Настраиваем логирование
    setup_logging()
    
    # Запускаем главную функцию
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
