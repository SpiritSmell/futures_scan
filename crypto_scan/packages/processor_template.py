import asyncio
import datetime
import logging
import packages.rabbitmq_consumer as rabbitmq_consumer
import packages.rabbitmq_producer as rabbitmq_producer
import copy
import json
from packages.app_template import AppTemplate
import pika


MAX_CONCURRENT_TASKS = 500

MAIN_LOOP_DELAY_TIME = 5

DEBUG = False
HOST = "192.168.192.42"
PORT = 8123
USERNAME = "default"
PASSWORD = ""
DATABASE_NAME = "raw"
TABLE_NAME = "crypto_scanner_data"
CONNECTION_STRING = f"http://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/"


# Функция для настройки логгера
def setup_logger(logger_name, level=logging.DEBUG):
    """
    Настраивает логгер с указанным именем и уровнем логирования.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    handler = logging.StreamHandler()
    handler.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger


class DataReceiver:
    def __init__(self, user, password, host, exchange, delay=5):
        self.received_data_lock = asyncio.Lock()
        self.received_data = []
        self.last_update_time_lock = asyncio.Lock()
        self.last_update_time = datetime.datetime.min  # Исправлено начальное значение
        self.delay = delay
        self.rabbitmq_client = None
        self.updated = False
        loop = asyncio.get_event_loop()
        loop.create_task(self.main_loop(user, password, host, exchange))

    async def get_data(self):
        async with self.received_data_lock:
            return copy.deepcopy(self.received_data)

    async def set_data(self, value):
        async with self.received_data_lock:
            self.received_data = copy.deepcopy(value)

    async def get_last_update_time(self):
        async with self.last_update_time_lock:
            return copy.deepcopy(self.last_update_time)

    async def set_last_update_time(self, value):
        async with self.last_update_time_lock:
            self.last_update_time = copy.deepcopy(value)

    async def main_loop(self, user, password, host, exchange):
        while True:
            try:
                # Устанавливаем соединение
                self.rabbitmq_client = rabbitmq_consumer.RabbitMQClient(
                    user, password, host, exchange, heartbeat=60
                )
                self.rabbitmq_client.connect()

                while True:
                    # Чтение данных из RabbitMQ
                    await self.receive_data_from_rabbitmq()
                    await asyncio.sleep(self.delay)

            except pika.exceptions.AMQPHeartbeatTimeout as e:
                logger.error(f"Connection lost due to heartbeat timeout: {e}. Reconnecting...")
                await asyncio.sleep(5)  # Небольшая пауза перед попыткой восстановления соединения
            except Exception as e:
                logger.error(f"Unexpected error in DataReceiver: {e}")
                await asyncio.sleep(5)

    async def receive_data_from_rabbitmq(self):
        try:
            # Процесс чтения данных из RabbitMQ
            new_data = self.rabbitmq_client.read_latest_from_exchange()
            if new_data:
                self.updated = True
                data_length = len(new_data)
                logger.debug(f"Latest received data length: {data_length}")
                await self.set_data(new_data)
                await self.set_last_update_time(datetime.datetime.now())
            else:
                logger.debug("No new data received")
        except Exception as e:
            logger.error(f"Error while receiving data: {e}")


class DataProcessor:
    def __init__(self):
        pass

    async def fetch_item(self, item_parameters):
        try:
            """
            exchange_instance = await self.get_exchange_instance(exchange_id)
            # Получаем ордербук для указанного символа
            orderbook = await exchange_instance.fetch_order_book(symbol)
            logger.debug(f"Received {symbol} from {exchange_id}")
            orderbook['exchange'] = exchange_id
            return orderbook
            
            logger.info(f"Fetched some values: {e}")
            """
            return None
        except Exception as e:
            # В случае ошибки выводим сообщение об ошибке и возвращаем None
            logger.error(f"Error fetching item: {e}")
            return None

    async def fetch_data_by_batch(self, batch):
        tasks = []
        for item_parameters in batch:
            orderbook = self.fetch_item(item_parameters)
            tasks.append(orderbook)
        return await asyncio.gather(*tasks)

    async def fetch_data(self, parameters_list):
        # Создаем список задач
        tasks = []

        # Разбиваем список на пакеты по MAX_CONCURRENT_TASKS задач
        batch_size = MAX_CONCURRENT_TASKS

        batched_parameters = []
        for i in range(0, len(parameters_list), batch_size):
            batch = parameters_list[i:i + batch_size]
            batched_parameters.append(batch)

        # Асинхронно выполняем задачи пакетами и получаем результаты
        results = []
        for batch in batched_parameters:
            batch_results = await self.fetch_data_by_batch(batch)
            results.extend(batch_results)
            logger.info(f"Finished {len(results)} batch tasks")

        # закрываем все что было открыто
        await self.fetch_data_cleanup()

        return results

    async def fetch_data_cleanup(self):
        logger.info(f"Cleaning up after onject is destructed batch tasks")

        return

    def print_results(self, pairs, results):
        logger.info(f"Number of order books {len(results)}")


class DataDispatcher:
    def __init__(self, user, password, host, exchange, delay=5):
        self.data_lock = asyncio.Lock()
        self.data = []
        self.delay = delay
        self.updated = False
        # Запускаем get_data асинхронно
        loop = asyncio.get_event_loop()
        task = loop.create_task(
            self.dispatch_data(user, password, host, exchange))

    async def get_data(self):
        async with self.data_lock:
            return copy.deepcopy(self.data)

    async def set_data(self, value):
        async with self.data_lock:
            self.updated = True
            self.data = copy.deepcopy(value)

    async def dispatch_data(self, user, password, host, exchange):
        while True:
            # в цикле отправляем данные каждые self.delay секунд
            # можно добавить чтоб данные отправлялись чаще, например, по факту обновления
            if self.updated:
                await self.send_data(user, password, host, exchange)
                self.updated = False
            # await self.set_data(self.extract_data(new_prices))

            await asyncio.sleep(self.delay)

    async def send_data(self, user, password, host, exchange):
        data = await self.get_data()
        if data:
            rabbitmq_producer.send_to_exchange(data, host, exchange, user, password)
            logger.debug(f'Sent to exchange {exchange}, {len(self.data)} records')
        else:
            logger.debug(f'No data to send ')




def convert_list_into_dictionary(source_list):
    result = {}
    if not source_list:
        return []
    for index, value in enumerate(source_list):
        symbol = value['symbol']
        exchange = value['exchange']
        if symbol in result:
            result[symbol][exchange] = value
        else:
            result[symbol] = {exchange: value}
        # insert = {symbol:{exchange:value}}
        # result.update(insert)

    return result


def enreach_source_data(received_data, orderbooks):
    orderbooks_dictionary = convert_list_into_dictionary(orderbooks)
    if not orderbooks_dictionary:
        return []
    enreached_data = copy.deepcopy(received_data)
    for name, value in enumerate(enreached_data):
        logger.debug(f"{name},{value}")
        symbol = value['analythics']['symbol']
        source = value['analythics']['source']
        destination = value['analythics']['destination']
        value['order_books'] = {}
        value['order_books']['symbol'] = symbol
        value['order_books']['source'] = source
        value['order_books']['destination'] = destination
        value['order_books']['source_orderbook'] = orderbooks_dictionary[symbol][source]
        value['order_books']['destination_orderbook'] = orderbooks_dictionary[symbol][destination]

    return enreached_data


def save_data_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f)


async def main():
    # Создаем экземпляр класса AppTemplate
    at = AppTemplate([('config=', "<config filename>")])
    at.get_arguments_from_env()
    at.get_arguments()
    at.load_settings_from_file(at.parameters['config'])

    # инициализируем и запускаем цикл получения данных
    dr = DataReceiver(at.settings["user"], at.settings["password"], at.settings["host"], at.settings["in_exchange"])
    dd = DataDispatcher(at.settings["user"], at.settings["password"], at.settings["host"], at.settings["out_exchange"])

    data_processor = DataProcessor()
    while True:

        # читаем входящие данные
        received_data = await dr.get_data()

        # преобразуем данные
        orderbooks = []
        enreached_data = []
        if received_data:
            # Получаем ордербуки
            test_data = []
            orderbooks = await data_processor.fetch_data(test_data)
            # добавляем ордербуки к исходным данным
            # enreached_data = enreach_source_data(received_data, orderbooks)
            enreached_data = received_data

        # устанавливаем данные на отправку
        if (enreached_data):
            await dd.set_data(enreached_data)
            # save_data_to_json(enreached_data[0]["order_books"], "..\\data\\processor_template_out.json")

        # Ждем некоторое время перед следующей итерацией
        await asyncio.sleep(MAIN_LOOP_DELAY_TIME)  # Можете установить другое значение времени ожидания


logger = setup_logger(__name__)

if __name__ == '__main__':
    asyncio.run(main())
