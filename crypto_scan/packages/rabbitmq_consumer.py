import pika
import pika.exceptions
import json
import time
import logging

USER = 'rmuser'
PASSWORD = 'rmpassword'
QUEUE = 'hello'
EXCHANGE = 'price_collector_out'
HOST = '192.168.56.107'

# Настройка логгера для текущего модуля
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Создание обработчика для вывода логов в консоль
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

# Создание форматирования для логов
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Добавление обработчика к логгеру
logger.addHandler(handler)


class RabbitMQClient:
    def __init__(self, user, password, host, exchange_name, heartbeat=60):
        self.credentials = pika.PlainCredentials(user, password)
        self.connection_params = pika.ConnectionParameters(
            host=host, credentials=self.credentials, heartbeat=heartbeat
        )
        self.exchange_name = exchange_name
        self.connection = None  # Атрибут connection инициализирован
        self.channel = None
        self.queue_name = None

    def __del__(self):
        # Безопасное завершение соединения
        if hasattr(self, "connection"):  # Проверяем наличие атрибута
            try:
                self.close()
            except Exception as e:
                logger.error(f"Error while closing RabbitMQClient in __del__: {e}")

    def connect(self):
        try:
            logger.info("Connecting to RabbitMQ...")
            self.connection = pika.BlockingConnection(self.connection_params)
            self.channel = self.connection.channel()

            # Объявление exchange и очереди
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='fanout')
            declared_queue = self.channel.queue_declare(queue='', exclusive=True)

            if declared_queue:
                self.queue_name = declared_queue.method.queue
                self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name)
                logger.info(
                    f"Connected to RabbitMQ, bound exchange '{self.exchange_name}' with queue '{self.queue_name}'"
                )
            else:
                logger.error("Failed to declare or bind the queue.")
        except Exception as e:
            logger.error(f"Error during connection to RabbitMQ: {e}")
            self.connection = None
            self.channel = None

    def close(self):
        if self.connection:
            try:
                self.connection.close()
                logger.info("RabbitMQ connection closed.")
            except Exception as e:
                logger.error(f"Error while closing RabbitMQ connection: {e}")
        else:
            logger.debug("No active RabbitMQ connection to close.")

    def ensure_connection(self):
        """Проверяет состояние соединения и восстанавливает его при необходимости."""
        if not self.connection or self.connection.is_closed:
            logger.warning("Connection is closed. Reconnecting...")
            self.connect()

    def read_latest_from_exchange(self):
        self.ensure_connection()
        if not self.channel or not self.queue_name:
            logger.error("Cannot consume messages. Ensure RabbitMQ is connected and queue is initialized.")
            return None

        result = None
        try:
            for method_frame, properties, body in self.channel.consume(self.queue_name, inactivity_timeout=0.1):
                if method_frame is None:
                    break
                else:
                    result = body
                    self.channel.basic_ack(method_frame.delivery_tag)
        except pika.exceptions.AMQPError as e:
            logger.error(f"AMQP error while consuming messages: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while consuming messages: {e}")

        if result is not None:
            try:
                return json.loads(result.decode('utf-8'))
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON: {e}")
                return None
        return None



if __name__ == '__main__':
    rabbitmq_client = RabbitMQClient(USER, PASSWORD, HOST, EXCHANGE)
    rabbitmq_client.connect()

    try:
        # Пауза в 3 секунды
        time.sleep(5)
        latest_exchange_data = rabbitmq_client.read_latest_from_exchange()
        logger.info("Latest exchange data: %s", latest_exchange_data)

        latest_queue_data = read_latest_from_queue()
        logger.info("Latest queue data: %s", latest_queue_data)
    except KeyboardInterrupt:
        pass
    finally:
        ...
