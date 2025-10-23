import logging
import aio_pika
import asyncio
import json
from packages.json_utils import load_data_from_json
from json_to_clickhouse import  ClickHouseJSONHandler, make_connection_string

# Максимальное количество одновременно выполняемых задач
MAX_CONCURRENT_TASKS = 500

# Задержка между основными циклами обработки
MAIN_LOOP_DELAY_TIME = 5

# Параметры подключения к RabbitMQ
HOST = "192.168.192.42"
USER = "rmuser"
PASSWORD = "rmpassword"
IN_EXCHANGE = "data_processor_test_in"  # Входной обменник
OUT_EXCHANGE = "data_processor_test_out"  # Выходной обменник
RECONNECT_DELAY = 5  # Задержка при переподключении в случае ошибки

DEBUG = False  # Флаг режима отладки


def url_from_parameters(host, user, password):
    """
    Формирует строку подключения к RabbitMQ.
    """
    return f"amqp://{user}:{password}@{host}/"


# Строка подключения к RabbitMQ
RABBITMQ_URL = url_from_parameters(HOST, USER, PASSWORD)


def setup_logger(logger_name, level=logging.DEBUG):
    """
    Настраивает логгер для записи сообщений в консоль.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    handler = logging.StreamHandler()
    handler.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger


# Настройка логгера
logger = setup_logger(__name__)


class DataReceiver:
    """
    Класс для получения данных из RabbitMQ.
    """

    def __init__(self, url, output_queue, loop, exchange):
        self.url = url
        self.output_queue = output_queue
        self.loop = loop
        self.exchange = exchange

    async def start(self):
        """
        Запускает процесс получения сообщений из обменника RabbitMQ.
        """
        while True:
            try:
                logger.info(f"[Receiver] Подключение к RabbitMQ, обменнику {self.exchange}")
                async with await aio_pika.connect_robust(self.url, loop=self.loop) as connection:
                    channel = await connection.channel()
                    input_exchange = await channel.declare_exchange(self.exchange, aio_pika.ExchangeType.FANOUT)
                    queue = await channel.declare_queue("", exclusive=True)
                    await queue.bind(input_exchange, routing_key="data")

                    logger.info("[Receiver] Ожидание сообщений...")

                    async for message in queue:
                        async with message.process():
                            data = json.loads(message.body.decode())
                            logger.info(f"[Receiver] Получено: {len(data)}")
                            await self.output_queue.put(data)
            except asyncio.CancelledError:
                logger.info("[Receiver] Завершение работы...")
                break
            except Exception as e:
                logger.info(f"[Receiver] Ошибка соединения: {e}. Повторная попытка через {RECONNECT_DELAY} секунд...")
                await asyncio.sleep(RECONNECT_DELAY)


class MockDataReceiver(DataReceiver):
    """
    Класс для получения данных из RabbitMQ.
    """

    def __init__(self, url, output_queue, loop, exchange, json_file, delay=RECONNECT_DELAY):
        super().__init__(url, output_queue, loop, exchange)
        self.data = load_data_from_json(filename=json_file)
        self.delay = delay

    async def start(self):
        """
        Запускает процесс получения сообщений из обменника RabbitMQ.
        """
        logger.info("[Receiver] Ожидание сообщений...")
        while True:
            try:
                logger.info(f"[Receiver] Получено: {len(self.data)}")
                await self.output_queue.put(self.data)
                logger.info(f"[Receiver] Пауза после отправки: {self.delay} секунд")
                await asyncio.sleep(self.delay)
            except asyncio.CancelledError:
                logger.info("[Receiver] Завершение работы...")
                break
            except Exception as e:
                logger.info(f"[Receiver] Ошибка соединения: {e}. Повторная попытка через {RECONNECT_DELAY} секунд...")
                await asyncio.sleep(RECONNECT_DELAY)


class DataProcessor:
    """
    Класс для обработки полученных данных.
    """

    def __init__(self, input_queue, output_queue):
        self.input_queue = input_queue
        self.output_queue = output_queue

    async def start(self):
        """
        Запускает процесс обработки данных.
        """
        logger.info("[Processor] Запуск обработки данных...")
        try:
            while True:
                data = await self.input_queue.get()
                logger.info(f"[Processor] Обработка данных: {len(data)}")
                processed_data = self.process_data(data)
                await self.output_queue.put(processed_data)
                logger.info(f"[Processor] Данные обработаны: {len(data)}")
        except asyncio.CancelledError:
            logger.info("[Processor] Завершение работы...")

    def process_data(self, data):
        """
        Метод обработки данных (заглушка, должен быть переопределён в потомках).
        """
        return None


class DataSender:
    """
    Класс для отправки обработанных данных в RabbitMQ.
    """

    def __init__(self, url, input_queue, loop, exchange):
        self.url = url
        self.input_queue = input_queue
        self.loop = loop
        self.exchange = exchange

    async def start(self):
        """
        Запускает процесс отправки данных в RabbitMQ.
        """
        while True:
            try:
                logger.info(f"[Sender] Подключение к RabbitMQ, обменнику {self.exchange}")
                async with await aio_pika.connect_robust(self.url, loop=self.loop) as connection:
                    channel = await connection.channel()
                    output_exchange = await channel.declare_exchange(self.exchange, aio_pika.ExchangeType.FANOUT)

                    logger.info("[Sender] Готов к отправке данных...")

                    while True:
                        data = await self.input_queue.get()
                        serialized_data = json.dumps(data)
                        logger.info(f"[Sender] Отправка данных: {len(serialized_data)}")
                        await output_exchange.publish(
                            aio_pika.Message(body=serialized_data.encode()),
                            routing_key="output"
                        )
            except asyncio.CancelledError:
                logger.info("[Sender] Завершение работы...")
                break
            except Exception as e:
                logger.info(f"[Sender] Ошибка соединения: {e}. Повторная попытка через {RECONNECT_DELAY} секунд...")
                await asyncio.sleep(RECONNECT_DELAY)



class CustomDataProcessor(DataProcessor):
    """
    Класс для пользовательской обработки данных.
    """

    def process_data(self, data):
        logger.info("[CustomProcessor] Пользовательская обработка данных")
        return f"custom_{data}"


async def send_test_data(loop):
    """
    Функция для отправки тестовых сообщений в RabbitMQ.
    """
    while True:
        try:
            async with await aio_pika.connect_robust(RABBITMQ_URL, loop=loop) as connection:
                channel = await connection.channel()
                input_exchange = await channel.declare_exchange(IN_EXCHANGE, aio_pika.ExchangeType.FANOUT)

                for i in range(5):
                    message = json.dumps({"test_message": i})
                    logger.info(f"[TestSender] Отправка: {message}")
                    await input_exchange.publish(
                        aio_pika.Message(body=message.encode()),
                        routing_key="data"
                    )
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            logger.info("[TestSender] Завершение работы...")
            break
        except Exception as e:
            logger.info(f"[TestSender] Ошибка соединения: {e}. Повторная попытка через {RECONNECT_DELAY} секунд...")
            await asyncio.sleep(RECONNECT_DELAY)


async def main():
    """
    Главная асинхронная функция для запуска всех сервисов.
    """
    loop = asyncio.get_event_loop()
    raw_data_queue = asyncio.Queue()
    processed_data_queue = asyncio.Queue()

    receiver = DataReceiver(RABBITMQ_URL, raw_data_queue, loop, IN_EXCHANGE)
    processor = CustomDataProcessor(raw_data_queue, processed_data_queue)
    sender = DataSender(RABBITMQ_URL, processed_data_queue, loop, OUT_EXCHANGE)

    tasks = [
        asyncio.create_task(receiver.start()),
        asyncio.create_task(processor.start()),
        asyncio.create_task(sender.start()),
        asyncio.create_task(send_test_data(loop))
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("[Main] Завершение работы...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
