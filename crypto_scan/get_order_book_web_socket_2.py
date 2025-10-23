import concurrent.futures
import json
import ccxt.pro
import logging
import signal
from packages.measure_execution_time import measure_execution_time
from multiprocessing import Process, current_process
import asyncio
import os
from datetime import datetime, timedelta

# Настройка логгера
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
MAX_CONCURRENT_TASKS = 1500

class OrderBooksRetriever:
    def __init__(self, max_concurrent_tasks=10):
        """
        Инициализация TaskManager.

        :param max_concurrent_tasks: Максимальное количество одновременно выполняемых задач.
        """
        self.max_concurrent_tasks = max_concurrent_tasks
        self.task_queue = asyncio.Queue()
        self.running_tasks = set()
        self.orderbooks = []
        self.symbols = []
        self.symbols_updates = 0  # Счетчик обработанных символов
        self.processed_updates = {}  # Массив символов
        self.exchange_instances = {}

    async def get_exchange_instance(self, exchange_id):
        """
        Получает или создает экземпляр биржи по её идентификатору.

        :param exchange_id: Идентификатор биржи.
        :return: Экземпляр биржи.
        """
        if exchange_id not in self.exchange_instances:
            try:
                self.exchange_instances[exchange_id] = getattr(ccxt.pro, exchange_id)()
            except AttributeError:
                logger.error(f"Биржа '{exchange_id}' не поддерживается ccxt.pro.")
                return None
        return self.exchange_instances[exchange_id]

    async def symbol_loop(self, exchange_id, symbol):
        """
        Обработка ордербука для одной биржи и символа.

        :param exchange_id: Идентификатор биржи.
        :param symbol: Торговый символ.
        """
        logger.info(f"Запуск задачи для биржи {exchange_id}, символа {symbol}.")
        exchange = await self.get_exchange_instance(exchange_id)
        if not exchange:
            return
        while True:
            try:
                orderbook = await exchange.watch_order_book(symbol)
                orderbook['exchange'] = exchange_id
                self.orderbooks.append(orderbook)
                self.symbols_updates += 1
                self.processed_updates[f"{exchange_id}:{symbol}"] = 1
            except asyncio.TimeoutError:
                logger.warning(f"Таймаут для {exchange_id}, символа {symbol}.")
            except ccxt.NetworkError as e:
                logger.error(f"Сетевой сбой для {exchange_id}, символа {symbol}: {e}")
                await asyncio.sleep(5)  # Подождите перед повторной попыткой
            except ccxt.base.errors.BadSymbol as e:
                logger.error(f"Неподдерживаемый символ {symbol} биржа {exchange_id}: {e}")
                break
            except ccxt.BaseError as e:
                if "Cannot write to closing transport" in str(e):
                    logger.warning(f"Закрытое соединение для {exchange_id}, символа {symbol}: {e}")
                    await asyncio.sleep(5)  # Подождите перед повторным подключением
                    exchange = await self.get_exchange_instance(exchange_id)  # Реинициализация соединения
                else:
                    logger.error(f"Ошибка при обработке {exchange_id}, символа {symbol}: {e}")
                    break
            except Exception as e:
                logger.error(f"Неизвестная ошибка для {exchange_id}, символа {symbol}: {e}")
                break
            finally:
                await asyncio.sleep(0.1)  # Добавьте небольшую задержку для предотвращения перегрузки

    async def worker(self):
        """
        Асинхронный воркер для обработки задач из очереди.
        """
        while True:
            exchange_id, symbol = await self.task_queue.get()

            task = asyncio.create_task(self.symbol_loop(exchange_id, symbol))
            self.running_tasks.add(task)
            task.add_done_callback(self.running_tasks.discard)

            try:
                await task
            finally:
                self.task_queue.task_done()

    async def symbol_counter(self):
        """
        Счетчик обработанных символов в минуту.
        """
        delay = 10
        counter = 0
        while True:
            await asyncio.sleep(delay)
            counter += delay
            logger.info(f"Обработано обновлений за последние {delay} секунд: {self.symbols_updates}")
            logger.info(f"Обработано символов за последние {counter} секунд: {len(self.processed_updates)}")
            self.symbols_updates = 0
            #self.processed_updates = {}

    async def fetch_orderbooks(self):
        """
        Основная функция для запуска обработки задач.
        """
        workers = [asyncio.create_task(self.worker()) for _ in range(self.max_concurrent_tasks)]
        counter_task = asyncio.create_task(self.symbol_counter())

        for symbol, exchange_id in self.symbols:
            await self.task_queue.put((exchange_id, symbol))

        await self.task_queue.join()

        for worker in workers:
            worker.cancel()
        counter_task.cancel()

        # Закрытие всех экземпляров бирж
        for exchange in self.exchange_instances.values():
            await exchange.close()

    async def shutdown(self):
        """
        Завершение работы с закрытием всех соединений.
        """
        logger.info("Завершаем работу...")
        for task in self.running_tasks:
            task.cancel()
        for exchange in self.exchange_instances.values():
            await exchange.close()
        logger.info("Все соединения закрыты.")


    def get_orderbooks(self):
        """
        Возвращает собранные ордербуки.

        :return: Список ордербуков.
        """
        return self.orderbooks


def load_data_from_json(file_name):
    """
    Загрузка данных из JSON-файла.

    :param file_name: Путь к JSON-файлу.
    :return: Список данных из файла.
    """
    try:
        with open(file_name, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Файл {file_name} не найден.")
        return []
    except json.JSONDecodeError as e:
        logger.warning(f"Ошибка декодирования JSON в {file_name}: {e}")
        return []

async def main():
    symbols = load_data_from_json('data/get_order_book_pairs.json')

    filtered_symbols = []
    for symbol in symbols:
        if symbol[1]=='digifinex':
            continue
        filtered_symbols.append(symbol)

    symbols = filtered_symbols
    if symbols:
        manager = OrderBooksRetriever(max_concurrent_tasks=MAX_CONCURRENT_TASKS)
        manager.symbols = symbols

        loop = asyncio.get_running_loop()

        # Создаем пул потоков
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Используем run_in_executor для выполнения задачи в отдельном потоке
            await loop.run_in_executor(executor, lambda: asyncio.run(manager.fetch_orderbooks()))

        # Обработка сигналов завершения
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(manager.shutdown()))

        try:
            await measure_execution_time(manager.fetch_orderbooks)
        except asyncio.CancelledError:
            pass
        finally:
            await manager.shutdown()

        logger.info("Работа завершена. Все задачи обработаны, соединения закрыты.")
    else:
        logger.warning("Список символов пуст или не удалось загрузить данные.")


if __name__ == "__main__":
    asyncio.run(main())