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
            logger.info(f"receiving data for symbol{symbol}")
            orderbook = await exchange_instance.fetch_order_book(symbol)
            logger.info(f"receiving data for symbol{symbol}")
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

        logger.info("Orderbooks collected")

        return orderbooks

    def print_orderbooks(self, pairs, orderbooks):
        # Обрабатываем результаты и выводим ордербуки
        #logger.info(orderbooks)
        save_data_to_json(orderbooks, 'data/get_order_book.json')
        logger.info(f"Number of order books {len(orderbooks)}")

async def main():
    # Создаём экземпляр логгера

    at = AppTemplate([('config=', "<config filename>")])
    at.get_arguments_from_env()
    at.get_arguments()
    at.load_settings_from_file(at.parameters['config'])

    # Запускаем get_pairs асинхронно

    order_book_fetcher = OrderBookFetcher(polls_delay = at.settings["polls_delay"])
    while True:
        ...






if __name__ == '__main__':
    #logger = setup_logger(__name__)
    asyncio.run(main())