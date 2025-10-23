import pika
import json
import logging



class RabbitMQClient:
    def __init__(self, host: str, exchange ='my_exchange', user: str = 'rmuser', password: str = 'rmpassword'):
        self.host = host
        self.user = user
        self.password = password
        self.connection = None
        self.channel = None
        self.exchange = exchange

        # Настройка логгера

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
        self.logger = logger

    def create_connection(self) -> bool:
        if self.connection and self.connection.is_open:
            return True
        try:
            credentials = pika.PlainCredentials(self.user, self.password)
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host, credentials=credentials))
            self.channel = self.connection.channel()
            self.logger.info("RabbitMQ connection established")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create RabbitMQ connection: {e}")
            return False

    def close_connection(self):
        if self.connection:
            self.connection.close()
            #self.logger.info('Connection closed')

    def send_message(self, exchange: str, routing_key: str, data: dict) -> bool:
        if not self.create_connection():
            return False
        try:
            serialized_data = json.dumps(data)
            self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=serialized_data)
            return True
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            return False

    def send_to_queue(self, queue: str, data: dict):
        if not self.create_connection():
            return
        try:
            self.channel.queue_declare(queue=queue)
            result = self.send_message('', queue, data)
            if result:
                self.logger.info(f'{len(data)} records were sent to queue {queue}')
        except Exception as e:
            self.logger.error(f"Failed to declare queue {queue} or send message: {e}")

    def send_to_exchange(self, exchange: str, data: dict):
        if not self.create_connection():
            return
        try:
            self.channel.exchange_declare(exchange=exchange, exchange_type='fanout')
            result = self.send_message(exchange, '', data)
            if result:
                self.logger.info(f'{len(data)} records were sent to exchange {exchange}')
        except Exception as e:
            self.logger.error(f"Failed to declare exchange {exchange} or send message: {e}")

    def send_to_rabbitmq(self, data, queue: str = 'hello', fanout: bool = False):
        if fanout:
            self.send_to_exchange(self.exchange, data)
        else:
            self.send_to_queue(queue, data)


if __name__ == '__main__':
    client = RabbitMQClient(host='192.168.56.107')
    data = {'name': 'John', 'age': 30, 'city': 'New York'}

    client.send_to_rabbitmq(data, queue='not_fanout_queue')
    client.send_to_rabbitmq(data, fanout=True)

    # Пример ручного закрытия соединения
    client.close_connection()
