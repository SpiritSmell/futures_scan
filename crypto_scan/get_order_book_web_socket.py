# -*- coding: utf-8 -*-

import asyncio
import time

import ccxt.pro
import ccxt
import json
import signal


class OrderBookWatcher:
    def __init__(self, config_file):
        self.config_file = config_file
        self.orderbooks = {}
        self.tasks = {}
        self.loop = asyncio.get_event_loop()
        self.symbols = self.load_data_from_json()

    def handle_all_orderbooks(self):
        """Функция для обработки и вывода всех ордербуков."""
        print('We have the following orderbooks:')
        for exchange_id, orderbooks_by_symbol in self.orderbooks.items():
            print(f"{exchange_id}: {len(orderbooks_by_symbol)}")
            # for symbol, orderbook in orderbooks_by_symbol.items():
            #     # Вывод времени, биржи, символа, первой цены продажи и покупки
            #     print(ccxt.pro.Exchange.iso8601(orderbook['timestamp']), exchange_id, symbol, orderbook['asks'][0],
            #           orderbook['bids'][0])

    async def symbol_loop(self, exchange, symbol):
        """Асинхронный цикл для отслеживания ордербука по символу на конкретной бирже."""
        #while True:
        try:
            # Получаем ордербук для символа
            orderbook = []
            try:
                # Ожидаем результат с таймаутом в 4 секунды
                orderbook = await asyncio.wait_for(exchange.watch_order_book(symbol), timeout=40)
                print("Orderbook received:", orderbook)
                return orderbook
            except asyncio.TimeoutError:
                print("Timeout! Orderbook not received within 4 seconds.")
            # Обновляем глобальный словарь ордербуков
            self.orderbooks.setdefault(exchange.id, {})[symbol] = orderbook
            print('===========================================================')
            # Обрабатываем все ордербуки (в данном случае просто выводим их)

            self.handle_all_orderbooks()
        except asyncio.CancelledError:
            print(f"Cancelled symbol loop for {exchange.id} {symbol}")
            #break
        except Exception as e:
            print(f"Error in symbol loop for {exchange.id} {symbol}: {str(e)}")
            #break  # Прерываем цикл для данного символа в случае ошибки

    async def exchange_loop(self, exchange_id, symbols):
        """Асинхронный цикл для отслеживания ордербуков по всем символам на конкретной бирже."""
        try:
            exchange = getattr(ccxt.pro, exchange_id)()
        except AttributeError:
            print(f"Exchange '{exchange_id}' is not supported by ccxt.pro.")
            return

        try:
            # Запускаем асинхронные задачи для каждого символа
            loops = [self.symbol_loop(exchange, symbol) for symbol in symbols]
            self.tasks[exchange_id] = asyncio.gather(*loops)
            await self.tasks[exchange_id]
        except asyncio.CancelledError:
            print(f"Cancelled exchange loop for {exchange_id}")
        except Exception as e:
            print(f"Error in exchange loop for {exchange_id}: {str(e)}")
        finally:
            # Закрываем соединение с биржей
            await exchange.close()

    def load_data_from_json(self):
        """Загружает данные из JSON файла и возвращает их в виде списка."""
        with open(self.config_file, 'r') as f:
            data = json.load(f)
        return data

    def stop_all_tasks(self):
        """Функция для отмены всех задач и завершения работы программы."""
        for task in self.tasks.values():
            task.cancel()

    def add_ticker(self, exchange_id, symbol):
        """Метод для добавления нового тикера."""
        if exchange_id in self.tasks:
            asyncio.ensure_future(self.symbol_loop(getattr(ccxt.pro, exchange_id)(), symbol))
        else:
            asyncio.ensure_future(self.exchange_loop(exchange_id, [symbol]))

    def remove_ticker(self, exchange_id, symbol):
        """Метод для удаления существующего тикера."""
        if exchange_id in self.orderbooks and symbol in self.orderbooks[exchange_id]:
            del self.orderbooks[exchange_id][symbol]
            # Останавливаем задачу отслеживания тикера
            if exchange_id in self.tasks:
                self.tasks[exchange_id].cancel()
                del self.tasks[exchange_id]
                symbols = [s for s in self.orderbooks[exchange_id].keys() if s != symbol]
                if symbols:
                    asyncio.ensure_future(self.exchange_loop(exchange_id, symbols))

    async def main(self):
        """
        Основная асинхронная функция для запуска процессов по всем биржам и их символам.

        Эта функция:
        1. Определяет список бирж, с которыми будет вестись работа.
        2. Создаёт асинхронные задачи для каждой биржи, связывая их с соответствующими символами.
        3. Одновременно запускает все задачи.
        """
        # Словарь для хранения бирж и их символов
        exchanges = {}


        # Заполняем словарь, связывая каждую биржу с общим списком символов

        for item in self.symbols:
            if not(item[1] in exchanges):
                exchanges[item[1]] = []
            exchanges[item[1]].append(item[0])

        # Создаём асинхронные задачи для обработки каждой биржи
        # Создаём пустой словарь для хранения задач
        self.tasks = {}

        # Проходим по всем биржам и их символам
        for exchange_id, symbols in exchanges.items():
            # Создаём асинхронную задачу для текущей биржи и символов
            task = self.exchange_loop(exchange_id, symbols)

            # Сохраняем задачу в словарь с идентификатором биржи в качестве ключа
            self.tasks[exchange_id] = task

        # Параллельно выполняем все задачи
        await asyncio.gather(*self.tasks.values())

    def run(self):
        """Запуск основного цикла."""
        # Устанавливаем обработчики сигналов для грациозного завершения
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.loop.add_signal_handler(sig, self.stop_all_tasks)
        try:
            self.loop.run_until_complete(self.main())
        except asyncio.CancelledError:
            print("Tasks have been cancelled.")
        finally:
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()
            print("Event loop closed.")


if __name__ == '__main__':
    watcher = OrderBookWatcher('./data/get_order_book_pairs.json')
    watcher.run()
