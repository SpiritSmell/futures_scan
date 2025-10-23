# откуда берутся пустые ордербуки ?
#if not orderbook:
#    # why ?
#    logger.debug(f"Empty orderbook")
# ошибки Error digifinex does not have market symbol PYTH/USDT fetching order book for PYTH/USDT on digifinex

import time

import ccxt.async_support as ccxt
import asyncio
import logging
import json
from packages import rabbitmq_producer, rabbitmq_consumer
import copy
from packages.app_template import AppTemplate
from datetime import datetime, timezone

MAX_CONCURRENT_TASKS = 500

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


def save_data_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f)


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


class OrderBookFetcher:
    def __init__(self,polls_delay = 0):
        self.polls_delay = polls_delay
        self.exchange_instances = {}

    async def get_exchange_instance(self, exchange_id):
        """
        Получает или создает экземпляр биржи по её идентификатору.
        """
        if exchange_id not in self.exchange_instances:
            self.exchange_instances[exchange_id] = getattr(ccxt, exchange_id)({'enableRateLimit': True})
        return self.exchange_instances[exchange_id]

    async def fetch_orderbook(self, exchange_id, symbol):
        """
        Асинхронно получает ордербук для указанной биржи и символа.
        """
        try:
            exchange_instance = await self.get_exchange_instance(exchange_id)
            logger.debug(f"receiving data for symbol{symbol} from {exchange_id}")
            orderbook = await exchange_instance.fetch_order_book(symbol)
            logger.debug(f"received data for symbol{symbol} from {exchange_id}")
            orderbook['exchange'] = exchange_id
            return orderbook
        except Exception as e:
            logger.error(f"Error {e} fetching order book for {symbol} on {exchange_id}")
            #logger.error(f"Error fetching order book for {symbol} on {exchange_id}: {e}")
            #logger.error(traceback.format_exc())  # Логирование трассировки ошибки
            return None

    async def fetch_orderbooks_batch(self, batch):
        tasks = []
        for ticker, exchange in batch:
            orderbook = self.fetch_orderbook(exchange, ticker)
            tasks.append(orderbook)
        return await asyncio.gather(*tasks)

    async def fetch_orderbooks(self, pairs):
        # Создаем список задач для получения ордербуков
        tasks = []
        logger.info("Collecting orderbooks")
        # Разбиваем список на пакеты по 10 задач
        batch_size = MAX_CONCURRENT_TASKS

        batched_pairs = []
        for i in range(0, len(pairs), batch_size):
            batch = pairs[i:i + batch_size]
            batched_pairs.append(batch)

        # Асинхронно выполняем задачи пакетами и получаем результаты
        orderbooks = []
        for batch in batched_pairs:
            batch_orderbooks = await self.fetch_orderbooks_batch(batch)
            orderbooks.extend(batch_orderbooks)
            logger.info(f"Order books processed {len(orderbooks)}")
            time.sleep(self.polls_delay)

        # Закрываем все экземпляры бирж после выполнения задач
        for exchange_id, exchange_instance in self.exchange_instances.items():
            await exchange_instance.close()

        exchanges_used = {}
        for orderbook in orderbooks:
            if not orderbook:
                # why ?
                logger.debug(f"Empty orderbook")
                continue
            exchange_name = orderbook.get("exchange",None)

            if exchange_name not in exchanges_used:
                exchanges_used[exchange_name] = 0
            exchanges_used[exchange_name] += 1



        logger.info(f"Orderbooks collected.")

        for exchange_used in exchanges_used:
            logger.info(f"{exchange_used} : {exchanges_used[exchange_used]}")

        return orderbooks

    def print_orderbooks(self, pairs, orderbooks):
        # Обрабатываем результаты и выводим ордербуки
        #logger.info(orderbooks)
        #save_data_to_json(orderbooks, 'data/get_order_book.json')
        logger.info(f"Number of order books {len(orderbooks)}")


class PairsFetcher:
    def __init__(self, user, password, host, exchange):
        self.pairs_data_lock = asyncio.Lock()
        self.pairs_data = []
        self.received_data_lock = asyncio.Lock()
        self.received_data = []
        # Запускаем get_pairs асинхронно
        loop = asyncio.get_event_loop()
        task = loop.create_task(
            self.fetch_pairs(user, password, host, exchange))

    def extract_pairs_data(self, input_data):
        if not input_data:
            return {}
        pairs = []
        for pair in input_data:
            analythics = pair["analythics"]
            pairs.append([analythics["symbol"], analythics["source"]])
            pairs.append([analythics["symbol"], analythics["destination"]])

        return pairs

    async def get_pairs(self):
        async with self.pairs_data_lock:
            return copy.deepcopy(self.pairs_data)

    async def set_pairs(self, value):
        async with self.pairs_data_lock:
            self.pairs_data = copy.deepcopy(value)

    async def get_received_data(self):
        async with self.received_data_lock:
            return copy.deepcopy(self.received_data)

    async def set_received_data(self, value):
        async with self.received_data_lock:
            self.received_data = copy.deepcopy(value)

    async def fetch_pairs(self, user, password, host, exchange):

        rabbitmq_client = rabbitmq_consumer.RabbitMQClient(user, password, host, exchange)
        rabbitmq_client.connect()

        while True:
            # чтение данных о парах из RabbitMQ
            new_prices = rabbitmq_client.read_latest_from_exchange()
            if not (new_prices is None):
                logger.info(f"Latest pairs received: {len(new_prices)}")
                await self.set_received_data(new_prices)
                await self.set_pairs(self.extract_pairs_data(new_prices))

            await asyncio.sleep(5)


class DataDispatcher:
    def __init__(self, user, password, host, exchange, delay=5):
        self.data_lock = asyncio.Lock()
        self.data = []
        self.delay = delay
        # Запускаем get_pairs асинхронно
        loop = asyncio.get_event_loop()
        task = loop.create_task(
            self.dispatch_data(user, password, host, exchange))

    async def get_data(self):
        async with self.data_lock:
            return copy.deepcopy(self.data)

    async def set_data(self, value):
        async with self.data_lock:
            self.data = copy.deepcopy(value)

    async def dispatch_data(self, user, password, host, exchange):
        while True:
            # отправка данных в RabbitMQ
            data = await self.get_data()
            if len(data)>0:
                rabbitmq_producer.send_to_exchange(data, host, exchange, user, password)
                logger.info(f'Sent to exchange {exchange}, {len(self.data)} records')
            # await self.set_pairs(self.extract_data(new_prices))

            await asyncio.sleep(self.delay)


def convert_list_into_dictionary(source_list):
    result = {}
    if not source_list:
        return []
    for index, value in enumerate(source_list):
        if not value:
            continue
        symbol = value['symbol']
        exchange = value['exchange']
        if symbol in result:
            result[symbol][exchange] = value
        else:
            result[symbol] = {exchange: value}
        #insert = {symbol:{exchange:value}}
        #result.update(insert)

    return result


def enreach_source_data(received_data, orderbooks):
    orderbooks_dictionary = convert_list_into_dictionary(orderbooks)
    if not orderbooks_dictionary:
        return []
    enreached_data = copy.deepcopy(received_data)
    for name, value in enumerate(enreached_data):
        #logger.info(f"{name},{value}")
        symbol = value['analythics']['symbol']
        source = value['analythics']['source']
        destination = value['analythics']['destination']
        value['order_books'] = {}
        value['order_books']['symbol'] = symbol
        value['order_books']['source'] = source
        value['order_books']['destination'] = destination
        value['order_books']['__timestamp'] = int(datetime.now(timezone.utc).timestamp())
        if not (symbol in orderbooks_dictionary):
            continue
        if source in orderbooks_dictionary[symbol]:
            value['order_books']['source_orderbook'] = orderbooks_dictionary[symbol][source]
        if destination in orderbooks_dictionary[symbol]:
            value['order_books']['destination_orderbook'] = orderbooks_dictionary[symbol][destination]

    return enreached_data


async def main():
    # Создаём экземпляр логгера
    #logger = setup_logger(__name__)  # Создаем экземпляр класса AppTemplate
    at = AppTemplate([('config=', "<config filename>")])
    at.get_arguments_from_env()
    at.get_arguments()
    at.load_settings_from_file(at.parameters['config'])

    # Запускаем get_pairs асинхронно

    pf = PairsFetcher(at.settings["user"], at.settings["password"], at.settings["host"], at.settings["in_exchange"])

    dd = DataDispatcher(at.settings["user"], at.settings["password"], at.settings["host"], at.settings["out_exchange"])

    order_book_fetcher = OrderBookFetcher(polls_delay = at.settings["polls_delay"])
    while True:
        # чтение данных о парах из RabbitMQ

        pairs = await pf.get_pairs()
        received_data = await pf.get_received_data()

        orderbooks = []
        enreached_data = []
        if pairs:
            # Получаем ордербуки
            #save_data_to_json(pairs, "./data/get_order_book_pairs.json")
            orderbooks = await order_book_fetcher.fetch_orderbooks(pairs)
            # Выводим ордербуки
            order_book_fetcher.print_orderbooks(pairs, orderbooks)
            # добавляем ордербуки к исходным данным
            enreached_data = enreach_source_data(received_data, orderbooks)

        if enreached_data:
            await dd.set_data(enreached_data)

        # Ждем некоторое время перед следующей итерацией
        logger.info(f"Sleeping for {at.settings['polls_delay']} seconds")
        await asyncio.sleep(at.settings['polls_delay'])  # Можете установить другое значение времени ожидания


if __name__ == '__main__':
    #logger = setup_logger(__name__)
    asyncio.run(main())
