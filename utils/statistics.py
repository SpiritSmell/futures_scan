import logging
from typing import Dict


class Statistics:
    def __init__(self):
        self.exchange_success: Dict[str, int] = {}
        self.exchange_errors: Dict[str, int] = {}
        self.rabbitmq_published: int = 0
        self.rabbitmq_failed: int = 0
        self.logger = logging.getLogger("statistics")
    
    def record_success(self, exchange: str):
        """Записывает успешный сбор данных"""
        self.exchange_success[exchange] = self.exchange_success.get(exchange, 0) + 1
    
    def record_error(self, exchange: str):
        """Записывает ошибку сбора данных"""
        self.exchange_errors[exchange] = self.exchange_errors.get(exchange, 0) + 1
    
    def record_published(self):
        """Записывает успешную публикацию в RabbitMQ"""
        self.rabbitmq_published += 1
    
    def record_publish_failed(self):
        """Записывает неудачную публикацию в RabbitMQ"""
        self.rabbitmq_failed += 1
    
    def print_and_reset(self):
        """Выводит статистику и сбрасывает счетчики"""
        self.logger.info("=== Statistics (last 60s) ===")
        
        # Собираем все биржи
        all_exchanges = sorted(set(list(self.exchange_success.keys()) + list(self.exchange_errors.keys())))
        
        if all_exchanges:
            for exchange in all_exchanges:
                success = self.exchange_success.get(exchange, 0)
                errors = self.exchange_errors.get(exchange, 0)
                self.logger.info(f"{exchange.capitalize()}: {success} success, {errors} errors")
        else:
            self.logger.info("No data collected yet")
        
        self.logger.info(f"RabbitMQ: {self.rabbitmq_published} published, {self.rabbitmq_failed} failed")
        self.logger.info("=============================")
        
        # Сбрасываем счетчики
        self.exchange_success.clear()
        self.exchange_errors.clear()
        self.rabbitmq_published = 0
        self.rabbitmq_failed = 0
