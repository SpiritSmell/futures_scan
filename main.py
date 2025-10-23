import logging
from utils.config_loader import load_config
from utils.logger import setup_logging


def main():
    # Загрузка конфигурации
    config = load_config()
    
    # Настройка логирования
    setup_logging(config.logging.level, config.logging.file)
    logger = logging.getLogger("main")
    
    logger.info("Futures Data Collector started")
    logger.info(f"Exchanges: {config.exchanges}")
    logger.info(f"Symbols: {config.symbols}")
    logger.info(f"Collection interval: {config.collection.interval_seconds}s")
    logger.info(f"RabbitMQ: {config.rabbitmq.host}:{config.rabbitmq.port}")


if __name__ == "__main__":
    main()
