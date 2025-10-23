import asyncio
from datetime import datetime, timezone
import ccxt.pro as ccxt
import json
from packages.rabbitmq_producer_2_async import AsyncRabbitMQClient
from packages import processor_template
from packages.app_template import AppTemplate, logger

def save_data_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f)

class CryptoDataStreamer:
    def __init__(self):
        """
        Инициализация класса.
        """
        self.symbol_exchange_map = {}
        self.tasks = []
        self.active_tasks = {}
        self.exchange_instances = {}
        self.ticker_data = {}
        self.order_book_data = {}
        self.ohlcv_data = {}
        self.dd = None
        self.dr = None
        self.received_data = None
        self.client = None

    async def initialize_exchanges(self):
        """Создание экземпляров бирж и проверка их возможностей."""
        for exchange_id in set(exchange for exchanges in self.symbol_exchange_map.values() for exchange in exchanges):
            try:
                exchange = getattr(ccxt, exchange_id)({
                    'enableRateLimit': True,  # Ограничение скорости запросов
                    'options': {'defaultType': 'spot'},  # Тип рынка (спот)
                })
                if not exchange.has['watchOrderBook'] or not exchange.has['fetchTicker']:
                    print(f"Биржа {exchange_id} не поддерживает необходимые функции.")
                    continue

                self.exchange_instances[exchange_id] = exchange
                print(f"Биржа {exchange_id} успешно инициализирована.")
            except Exception as e:
                print(f"Ошибка при инициализации биржи {exchange_id}: {e}")

    async def add_symbol(self, symbol, exchanges):
        """Добавление новой пары символов и списка бирж с учетом текущих задач."""
        if symbol not in self.symbol_exchange_map:
            self.symbol_exchange_map[symbol] = exchanges
            print(f"Добавлена пара {symbol} с биржами {exchanges}.")

            for exchange_id in exchanges:
                if exchange_id in self.exchange_instances:
                    exchange = self.exchange_instances[exchange_id]
                    task_key = f"{exchange_id}-{symbol}"
                    if task_key not in self.active_tasks:
                        task = asyncio.create_task(self.monitor_exchange(exchange_id, symbol, exchange))
                        self.active_tasks[task_key] = task
                    else:
                        print(f"Задача для {task_key} уже запущена.")
                else:
                    print(f"Биржа {exchange_id} не инициализирована для символа {symbol}.")
        else:
            print(f"Пара {symbol} уже существует. Добавление пропущено.")

    def remove_symbol(self, symbol):
        """Удаление пары символов."""
        if symbol in self.symbol_exchange_map:
            del self.symbol_exchange_map[symbol]
            self.ticker_data.pop(symbol, None)
            self.order_book_data.pop(symbol, None)
            self.ohlcv_data.pop(symbol, None)
            print(f"Пара {symbol} удалена.")
        else:
            print(f"Пара {symbol} не найдена.")

    def remove_exchange_from_symbol(self, symbol, exchange):
        """Удаление биржи из списка для указанного символа."""
        if symbol in self.symbol_exchange_map and exchange in self.symbol_exchange_map[symbol]:
            self.symbol_exchange_map[symbol].remove(exchange)
            print(f"Биржа {exchange} удалена из списка для {symbol}.")
        else:
            print(f"Биржа {exchange} не найдена в списке для {symbol}.")

    async def monitor_ticker(self, exchange_id, symbol, exchange):
        """Мониторинг тикеров для указанного символа."""
        try:
            while True:
                ticker = await exchange.watch_ticker(symbol)
                if symbol not in self.ticker_data:
                    self.ticker_data[symbol] = {}
                self.ticker_data[symbol][exchange_id] = ticker
                print(f"{exchange_id} - {symbol} - Ticker: {ticker['symbol']} | Bid: {ticker['bid']} | Ask: {ticker['ask']}")
        except Exception as e:
            print(f"Ошибка при мониторинге тикера {exchange_id} для {symbol}: {e}")

    async def monitor_order_book(self, exchange_id, symbol, exchange):
        """Мониторинг ордербука для указанного символа."""
        try:
            while True:
                order_book = await exchange.watch_order_book(symbol)
                if symbol not in self.order_book_data:
                    self.order_book_data[symbol] = {}
                self.order_book_data[symbol][exchange_id] = order_book
                print(f"{exchange_id} - {symbol} - Order Book: {len(order_book)} records")
        except Exception as e:
            print(f"Ошибка при мониторинге ордербука {exchange_id} для {symbol}: {e}")

    async def monitor_ohlcv(self, exchange_id, symbol, exchange):
        """Мониторинг OHLCV данных для указанного символа."""
        try:
            since = exchange.milliseconds() - 600000  # Временная метка начала (полчаса назад)
            while True:
                ohlcv_data = await exchange.fetch_ohlcv(symbol, timeframe='1m', since=since)
                if symbol not in self.ohlcv_data:
                    self.ohlcv_data[symbol] = {}
                self.ohlcv_data[symbol][exchange_id] = ohlcv_data
                print(f"{exchange_id} - {symbol} - OHLCV Data (Last 30 min): {len(ohlcv_data)}")
        except Exception as e:
            print(f"Ошибка при мониторинге OHLCV {exchange_id} для {symbol}: {e}")

    def fill_data_to_send(self):
        data = []

        for symbol in self.symbol_exchange_map:
            item = {}
            item["symbol"] = symbol
            item["tickers"] = self.ticker_data.get(symbol, {})
            item["order_books"] = self.order_book_data.get(symbol, {})
            item["ohlcvs"] = self.ohlcv_data.get(symbol, {})
            item['__timestamp'] = int(datetime.now(timezone.utc).timestamp())
            data.append(item)
        return data

    async def send_data(self):
        """Мониторинг тикеров для указанного символа."""
        try:
            while True:
                data = self.fill_data_to_send()
                await self.dd.set_data(data)
                await asyncio.sleep(5)
        except Exception as e:
            print(f"Ошибка сохранения: {e}")

    async def receive_data(self):
        """загрузка данных"""
        try:
            while True:
                updated = False
                received_data_buffer = await self.dr.get_data()
                if received_data_buffer:
                    updated = True
                    self.received_data = received_data_buffer
                    for item in received_data_buffer:
                        symbol = item['analythics']['symbol']
                        exchanges = list(item['data'][symbol].keys())
                        await self.add_symbol(symbol, exchanges)
                await asyncio.sleep(5)
        except Exception as e:
            print(f"Ошибка загрузки данных: {e}")

    async def monitor_exchange(self, exchange_id, symbol, exchange):
        """Запуск мониторинга тикеров, ордербуков и OHLCV данных параллельно для символа на бирже."""
        tasks = [
            self.monitor_ticker(exchange_id, symbol, exchange),
            self.monitor_order_book(exchange_id, symbol, exchange),
            self.monitor_ohlcv(exchange_id, symbol, exchange),
        ]
        await asyncio.gather(*tasks)

    async def run(self):
        at = AppTemplate([('config=', "<config filename>")])
        at.get_arguments_from_env()
        at.get_arguments()
        at.load_settings_from_file(at.parameters['config'])

        self.dr = processor_template.DataReceiver(at.settings["user"], at.settings["password"], at.settings["host"],
                                                  at.settings["in_exchange"])
        self.dd = processor_template.DataDispatcher(at.settings["user"], at.settings["password"], at.settings["host"],
                                                    at.settings["out_exchange"])
        await self.initialize_exchanges()

        self.client = AsyncRabbitMQClient(host='192.168.192.42')

async def main():
    streamer = CryptoDataStreamer()
    await streamer.add_symbol('LTC/USDT', ['binance', 'kraken'])
    await streamer.run()

if __name__ == "__main__":
    asyncio.run(main())
