import pika
import json
import logging

USER = 'rmuser'
PASSWORD = 'rmpassword'
QUEUE = 'hello'
HOST = '192.168.56.107'

EXCHANGE = 'my_exchange'
NOT_FANOUT_QUEUE = 'not_fanout_queue'
FANOUT_QUEUE = 'fanout_queue'

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


def create_rabbitmq_connection(host, user=USER, password=PASSWORD):
    try:
        credentials = pika.PlainCredentials(user, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials))
        channel = connection.channel()
        return connection, channel
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Failed to create RabbitMQ connection: {str(e)}")
        return None, None


def send_message(channel, exchange, routing_key, data):
    try:
        serialized_data = json.dumps(data)
        result = channel.basic_publish(exchange=exchange, routing_key=routing_key, body=serialized_data)
        return result
    except pika.exceptions.AMQPError as e:
        logger.error(f"Failed to send message: {str(e)}")
        return None


def send_to_queue(json_data, host, queue):
    try:
        connection, channel = create_rabbitmq_connection(host)
        if connection and channel:
            channel.queue_declare(queue=queue)
            result = send_message(channel, '', queue, json_data)
            if not result:
                logger.info(f'{len(json_data)} records were sent to queue {queue}')
            connection.close()
        else:
            logger.error(f"Failed to send message to queue {queue}: Connection not established")
    except Exception as e:
        logger.error(f"Failed to send message to queue {queue}: {str(e)}")


def send_to_exchange(json_data, host, exchange, user=USER, password=PASSWORD):
    try:
        connection, channel = create_rabbitmq_connection(host, user, password)
        if connection and channel:
            channel.exchange_declare(exchange=exchange, exchange_type='fanout')

            # Создаем временную очередь с автоудалением, которая будет содержать только последнее сообщение
            queue_name = channel.queue_declare(queue='', auto_delete=True, exclusive=True).method.queue
            channel.queue_bind(exchange=exchange, queue=queue_name)

            # Отправляем сообщение в обменник
            result = send_message(channel, exchange, '', json_data)
            if not result:
                logger.info(f'{len(json_data)} records were sent to exchange {exchange}')

            # Удаляем временную очередь, чтобы предыдущее сообщение было удалено
            channel.queue_unbind(exchange=exchange, queue=queue_name)
            connection.close()
        else:
            logger.error(f"Failed to send message to fanout exchange {exchange}: Connection not established")
    except Exception as e:
        logger.error(f"Failed to send message to fanout exchange {exchange}: {str(e)}")



def send_to_rabbitmq(json_data, queue=QUEUE, fanout=False, host=HOST):
    if fanout:
        send_to_exchange(json_data, host, EXCHANGE)
    else:
        send_to_queue(json_data, host, queue)


if __name__ == '__main__':
    data = {'name': 'John', 'age': 30, 'city': 'New York'}

    send_to_rabbitmq(data, NOT_FANOUT_QUEUE)
    send_to_exchange(data, HOST, EXCHANGE)
