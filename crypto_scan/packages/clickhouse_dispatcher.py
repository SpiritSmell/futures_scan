import asyncio
import datetime
import logging
import packages.rabbitmq_consumer as rabbitmq_consumer
import packages.rabbitmq_producer as rabbitmq_producer
import copy
import json
from packages.app_template import AppTemplate
from packages.clickhouse import ClickhouseConnector
from packages.processor_template import DataDispatcher, DataReceiver, DataProcessor

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
def setup_logger(logger_name, level=logging.INFO):
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




class ClickhouseDispatcher(DataDispatcher):
    def __init__(self, delay = 5, connection_string = ""):
        # super().__init__(user, password, host, exchange, delay)
        self.data_lock = asyncio.Lock()
        self.data = []
        self.delay = delay
        self.connection_string = connection_string
        self.clickhouse_connector = ClickhouseConnector(connection_string=self.connection_string,
                                                        database_name=DATABASE_NAME,
                                                        table_name=TABLE_NAME)
        if self.clickhouse_connector:
            logger.info("Connection to Clickhouse is established")
        else:
            logger.info("Error connecting to Clickhouse")
        # Запускаем get_data асинхронно
        loop = asyncio.get_event_loop()
        task = loop.create_task(
            self.dispatch_data()
                                )
        logger.info(f'ClickhouseDispatcher has started')

    async def dispatch_data(self):
        while True:
            # в цикле отправляем данные каждые self.delay секунд
            # можно добавить чтоб данные отправлялись чаще, например, по факту обновления
            await self.send_data()
            await asyncio.sleep(self.delay)

    async def send_data(self):
        data = await self.get_data()
        if data:
            # rabbitmq_producer.send_to_exchange(data, host, exchange, user, password)
            self.clickhouse_connector.save_data(data, replace=False)
            logger.info(f'{len(data)} records were sent to Clickhouse')
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

level=logging.INFO
if DEBUG:
    level=logging.DEBUG

logger = setup_logger(__name__, level=level)

if __name__ == '__main__':
    asyncio.run(main())
