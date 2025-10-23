import aio_pika
import asyncio
import json
import logging


class AsyncRabbitMQClient:
    def __init__(self, host: str, exchange='my_exchange', user: str = 'rmuser', password: str = 'rmpassword'):
        self.host = host
        self.user = user
        self.password = password
        self.connection = None
        self.channel = None
        self.exchange = exchange

        # Настройка логгера
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        self.logger = logger

    async def create_connection(self) -> bool:
        if self.connection and not self.connection.is_closed:
            self.logger.debug("Using existing RabbitMQ connection")
            return True
        try:
            self.logger.info(f"Connecting to RabbitMQ at {self.host} with user {self.user}")
            self.connection = await aio_pika.connect_robust(
                host=self.host,
                login=self.user,
                password=self.password
            )
            self.channel = await self.connection.channel()
            self.logger.info(f"RabbitMQ connection established successfully to {self.host}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create RabbitMQ connection to {self.host}: {e}")
            return False

    async def close_connection(self):
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            self.logger.info("Connection closed")

    async def send_message(self, exchange: str, routing_key: str, data: dict) -> bool:
        if not await self.create_connection():
            return False
        try:
            serialized_data = json.dumps(data)
            await self.channel.default_exchange.publish(
                aio_pika.Message(body=serialized_data.encode()),
                routing_key=routing_key
            )
            self.logger.info(f"Message sent to exchange: {exchange}, routing key: {routing_key}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            return False

    async def send_to_exchange(self, exchange: str, data: dict) -> bool:
        self.logger.info(f"Attempting to send data to exchange: {exchange}")
        if not await self.create_connection():
            self.logger.error(f"Cannot send to exchange {exchange} - connection failed")
            return False
        try:
            self.logger.info(f"Declaring exchange: {exchange} as FANOUT")
            exchange_obj = await self.channel.declare_exchange(exchange, aio_pika.ExchangeType.FANOUT)
            serialized_data = json.dumps(data)
            data_size = len(serialized_data)
            self.logger.info(f"Sending {data_size} bytes to exchange {exchange}")
            await exchange_obj.publish(
                aio_pika.Message(body=serialized_data.encode()),
                routing_key=""
            )
            self.logger.info(f"✅ Message successfully sent to exchange: {exchange} ({data_size} bytes)")
            return True
        except Exception as e:
            self.logger.error(f"❌ Failed to declare exchange {exchange} or send message: {e}")
            return False

    async def send_to_queue(self, queue: str, data: dict) -> bool:
        """Отправка данных в очередь."""
        if not await self.create_connection():
            return False
        try:
            queue_obj = await self.channel.declare_queue(queue, durable=True)
            serialized_data = json.dumps(data)
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=serialized_data.encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=queue
            )
            self.logger.info(f"Message sent to queue: {queue}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send message to queue {queue}: {e}")
            return False

    async def send_to_rabbitmq(self, data, queue: str = 'hello', fanout: bool = False) -> bool:
        if fanout:
            return await self.send_to_exchange(self.exchange, data)
        else:
            return await self.send_to_queue(queue, data)


if __name__ == '__main__':
    async def main():
        client = AsyncRabbitMQClient(host='192.168.192.42')
        data = {'name': 'John', 'age': 30, 'city': 'New York'}

        await client.send_to_rabbitmq(data, fanout=True)

        await client.close_connection()

    asyncio.run(main())
