import logging
from typing import Optional
import aio_pika
from aio_pika import Connection, Channel, Exchange
from models.futures_data import FuturesData


class RabbitMQPublisher:
    def __init__(self, host: str, port: int, user: str, password: str, exchange_name: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.exchange_name = exchange_name
        self.connection: Optional[Connection] = None
        self.channel: Optional[Channel] = None
        self.exchange: Optional[Exchange] = None
        self.logger = logging.getLogger("rabbitmq")
    
    async def connect(self):
        """Подключается к RabbitMQ"""
        try:
            # Отключаем DEBUG логи от aiormq
            logging.getLogger("aiormq").setLevel(logging.WARNING)
            
            self.connection = await aio_pika.connect_robust(
                host=self.host,
                port=self.port,
                login=self.user,
                password=self.password
            )
            self.channel = await self.connection.channel()
            self.exchange = await self.channel.declare_exchange(
                self.exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            self.logger.info(f"Connected to RabbitMQ at {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    async def publish(self, data: FuturesData) -> bool:
        """Отправляет данные в RabbitMQ"""
        try:
            # Формируем routing key: futures.binance.BTCUSDT
            symbol_normalized = data.symbol.replace('/', '').replace(':', '')
            routing_key = f"futures.{data.exchange}.{symbol_normalized}"
            
            # Сериализуем данные в JSON
            message_body = data.model_dump_json()
            
            # Отправляем сообщение
            await self.exchange.publish(
                aio_pika.Message(
                    body=message_body.encode('utf-8'),
                    content_type='application/json'
                ),
                routing_key=routing_key
            )
            
            self.logger.info(f"Published to {routing_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to publish message: {e}")
            return False
    
    async def close(self):
        """Закрывает соединение с RabbitMQ"""
        if self.connection:
            await self.connection.close()
            self.logger.info("RabbitMQ connection closed")
