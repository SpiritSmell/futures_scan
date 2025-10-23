"""
Новая версия futures_price_collector с модульной архитектурой.
Версия 2.0 - рефакторинг с разделением на специализированные модули.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Добавляем путь к пакетам
sys.path.append(str(Path(__file__).parent))

from packages.orchestrator import CryptoDataOrchestrator


async def main() -> None:
    """Главная функция приложения."""
    # Настройка логирования
    logging.basicConfig(
        level=logging.DEBUG,  # Увеличиваем детализацию для отладки
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('crypto_streamer_v2.log', mode='w')
        ]
    )
    
    # Подавление шумных логов библиотек
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("ccxt").setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    
    # Получение пути к конфигурации
    config_path = None
    if len(sys.argv) > 1:
        if sys.argv[1].startswith('--config='):
            config_path = sys.argv[1].split('=', 1)[1]
        else:
            config_path = sys.argv[1]
    
    if not config_path:
        logger.error("Configuration file not specified. Usage: python futures_price_collector_v2.py --config=path/to/config.cfg")
        sys.exit(1)
    
    # Создание и запуск оркестратора
    orchestrator = CryptoDataOrchestrator()
    
    try:
        logger.info("Starting Crypto Data Collector v2.0")
        
        # Инициализация системы
        await orchestrator.initialize(config_path)
        
        # Запуск системы
        await orchestrator.start()
        
    except KeyboardInterrupt:
        logger.info("Shutdown by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
