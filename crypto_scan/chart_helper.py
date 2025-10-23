import asyncio
from datetime import datetime, timezone
import ccxt.pro as ccxt_async
import ccxt as ccxt_sync
import json
from packages.rabbitmq_producer_2_async import AsyncRabbitMQClient
from packages import processor_template
from packages.app_template import AppTemplate, logger


def save_data_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f)


class CryptoDataStreamer:
    def __init__(self, symbol_exchange_map=None):

        """
        Инициализация класса с парой символов и списками бирж.
        :param symbol_exchange_map: Словарь, где ключи — символы, а значения — списки бирж.
        """
        # Инициализация других атрибутов...
        self.api_keys = {}
        self.tasks = []
        self.active_tasks = {}  # Новый словарь для отслеживания активных задач
        self.symbol_exchange_map = symbol_exchange_map if symbol_exchange_map else {}
        self.async_exchange_instances = {}
        self.sync_exchange_instances = {}
        # Словари для хранения данных тикеров, ордербуков и OHLCV
        self.ticker_data = {}
        self.order_book_data = {}
        self.ohlcv_data = {}
        self.networks_data_storage = {}
        self.networks_data = {}
        self.dd = None
        self.dr = None
        self.received_data = None
        self.client = None
        self.latest_timestamps = {}

    async def initialize_exchanges(self):
        """Создание экземпляров бирж и проверка их возможностей."""
        for exchange_id in set(exchange for exchanges in self.symbol_exchange_map.values() for exchange in exchanges):
            await self.initialize_exchange(exchange_id)

    async def initialize_exchange(self, exchange_id):
        """Создание экземпляров бирж и проверка их возможностей."""
        if exchange_id in self.async_exchange_instances:
            logger.info(f"Биржа {exchange_id} уже была успешно инициализирована.")
        try:
            exchange_async = getattr(ccxt_async, exchange_id)(
                self.api_keys.get(exchange_id, {
                'enableRateLimit': True,  # Ограничение скорости запросов
                'options': {'defaultType': 'spot'},  # Тип рынка (спот)
                })
            )
            if not exchange_async.has['watchOrderBook'] or not exchange_async.has['fetchTicker']:
                logger.error(f"Биржа {exchange_id} не поддерживает необходимые функции.")
                return

            exchange_sync = getattr(ccxt_sync, exchange_id)(
                self.api_keys.get(exchange_id, {
                'enableRateLimit': True,  # Ограничение скорости запросов
                'options': {'defaultType': 'spot'},  # Тип рынка (спот)
                })
            )
            if not exchange_sync.has['fetchCurrencies']:
                logger.error(f"Биржа {exchange_id} не поддерживает необходимые функции.")
                return
            # Проверяем, запущена ли уже задача мониторинга данных биржи
            task_key = f"{exchange_id}-networks"
            if task_key not in self.active_tasks:
                # Добавляем задачу для мониторинга данных сетей биржи
                task = asyncio.create_task(self.monitor_currency_info(exchange_id, exchange_sync))
                self.active_tasks[task_key] = task
            self.async_exchange_instances[exchange_id] = exchange_async
            logger.info(f"Биржа {exchange_id} успешно инициализирована.")
        except Exception as e:
            logger.error(f"Ошибка при инициализации биржи {exchange_id}: {e}")
            return None

    async def add_symbol(self, symbol, exchanges):
        """Добавление новой пары символов и списка бирж с учетом текущих задач."""
        if symbol not in self.symbol_exchange_map:
            self.symbol_exchange_map[symbol] = exchanges
            logger.info(f"Добавлена пара {symbol} с биржами {exchanges}.")

            # Инициализация мониторинга для новых символов и бирж
            for exchange_id in exchanges:
                if not (exchange_id in self.async_exchange_instances):
                    await self.initialize_exchange(exchange_id)
                if not (exchange_id in self.async_exchange_instances):
                    logger.error(f"Биржа {exchange_id} не инициализирована для символа {symbol}.")
                    continue
                exchange = self.async_exchange_instances[exchange_id]

                # Проверяем, запущена ли уже задача мониторинга
                task_key = f"{exchange_id}-{symbol}"
                if task_key not in self.active_tasks:
                    # Создаем и добавляем задачи для нового символа
                    task = asyncio.create_task(self.monitor_exchange_symbol(exchange_id, symbol, exchange))
                    self.active_tasks[task_key] = task
                else:
                    logger.info(f"Задача для {task_key} уже запущена.")

        else:
            logger.debug(f"Пара {symbol} уже существует. Добавление пропущено.")

    def remove_symbol(self, symbol):
        """Удаление пары символов."""
        if symbol in self.symbol_exchange_map:
            del self.symbol_exchange_map[symbol]
            self.ticker_data.pop(symbol, None)
            self.order_book_data.pop(symbol, None)
            self.ohlcv_data.pop(symbol, None)
            self.networks_data.pop(symbol, None)
            logger.info(f"Пара {symbol} удалена.")
        else:
            logger.error(f"Пара {symbol} не найдена.")

    def remove_exchange_from_symbol(self, symbol, exchange):
        """Удаление биржи из списка для указанного символа."""
        if symbol in self.symbol_exchange_map and exchange in self.symbol_exchange_map[symbol]:
            self.symbol_exchange_map[symbol].remove(exchange)
            logger.info(f"Биржа {exchange} удалена из списка для {symbol}.")
        else:
            logger.error(f"Биржа {exchange} не найдена в списке для {symbol}.")

    def add_timestamp(self,data_type,symbol,exchange_id,timestamp):
        if timestamp is None:
            timestamp = int(datetime.now(timezone.utc).timestamp()*1000)
        self.latest_timestamps[data_type] = self.latest_timestamps.get(data_type,{})
        self.latest_timestamps[data_type][symbol] = self.latest_timestamps[data_type].get(symbol,{})
        self.latest_timestamps[data_type][symbol][exchange_id] = timestamp

    def remove_timestamp(self,data_type,symbol,exchange_id):
        self.latest_timestamps[data_type][symbol].pop(exchange_id)

    async def monitor_ticker(self, exchange_id, symbol, exchange):
        """Мониторинг тикеров для указанного символа."""
        try:
            while True:
                # условие окончания мониторинга
                if not(symbol in self.symbol_exchange_map):
                    break
                ticker = await exchange.watch_ticker(symbol)
                if symbol not in self.ticker_data:
                    self.ticker_data[symbol] = {}
                self.ticker_data[symbol][exchange_id] = ticker
                self.add_timestamp("ticker_data",symbol,exchange_id,ticker.get("timestamp",0))

                network = self.networks_data_storage[exchange_id]
                if not network:
                    continue
                currency = symbol.split("/")[0]
                symbol_networks = network.get(currency, None)
                if not symbol_networks:
                    continue

                if symbol not in self.networks_data:
                    self.networks_data[symbol] = {}

                if exchange_id not in self.networks_data[symbol]:
                    self.networks_data[symbol][exchange_id] = {}

                self.networks_data[symbol][exchange_id] = symbol_networks

                logger.info(
                    f"{exchange_id} - {symbol} - Ticker: {ticker['symbol']} | Bid: {ticker['bid']} | Ask: {ticker['ask']}")
        except Exception as e:
            logger.error(f"Ошибка при мониторинге тикера {exchange_id} для {symbol}: {e}")

    async def monitor_order_book(self, exchange_id, symbol, exchange):
        """Мониторинг ордербука для указанного символа."""
        try:
            while True:
                # условие окончания мониторинга
                if not (symbol in self.symbol_exchange_map):
                    break
                order_book = await exchange.watch_order_book(symbol)
                if symbol not in self.order_book_data:
                    self.order_book_data[symbol] = {}
                self.order_book_data[symbol][exchange_id] = order_book
                self.add_timestamp("order_book_data", symbol, exchange_id, order_book.get("timestamp", 0))
                logger.info(f"{exchange_id} - {symbol} - Order Book: {len(order_book)} records")
        except Exception as e:
            logger.error(f"Ошибка при мониторинге ордербука {exchange_id} для {symbol}: {e}")

    async def monitor_ohlcv(self, exchange_id, symbol, exchange):
        """Мониторинг OHLCV данных для указанного символа."""
        try:
            since = exchange.milliseconds() - 600000  # Временная метка начала (полчаса назад)
            while True:
                # условие окончания мониторинга
                if not (symbol in self.symbol_exchange_map):
                    break
                ohlcv_data = await exchange.fetch_ohlcv(symbol, timeframe='1m', since=since)
                if symbol not in self.ohlcv_data:
                    self.ohlcv_data[symbol] = {}
                self.ohlcv_data[symbol][exchange_id] = ohlcv_data
                self.add_timestamp("ohlcv_data", symbol, exchange_id, ohlcv_data.get("timestamp", 0))
                logger.info(f"{exchange_id} - {symbol} - OHLCV Data (Last 30 min): {len(ohlcv_data)}")
                #logger.info(f"{exchange.timeframes}")
        except Exception as e:
            logger.error(f"Ошибка при мониторинге OHLCV {exchange_id} для {symbol}: {e}")

    def fetch_currency_info(self, exchange_id, exchange):
        """Мониторинг ордербука для указанного символа."""
        try:
            networks = exchange.fetchCurrencies()
            if exchange_id not in self.networks_data_storage:
                self.networks_data_storage[exchange_id] = {}
            self.networks_data_storage[exchange_id] = networks

            logger.info(f"Загружены данные о сетях биржи {exchange_id} : {len(networks)} записей.")
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных о сетях {exchange_id} : {e}")

    async def monitor_currency_info(self, exchange_id, exchange):
        """Мониторинг ордербука для указанного символа."""
        try:
            while True:
                logger.debug("Запуск блокирующей синхронной функции.")
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, lambda: (self.fetch_currency_info(exchange_id, exchange)))
                logger.debug("Возврат в асинхронную функцию.")
                # ждем 120 секунд перед повторным запросом данных
                await asyncio.sleep(120)
        except Exception as e:
            logger.info(f"Ошибка при загрузке данных о сетях {exchange_id} : {e}")

    def fix_missing_tickers_data(self,ticker, orders):
        data = {}
        for exchange in ticker.keys():
            ticker_item = ticker[exchange]
            data[exchange] = ticker_item
            if not (ticker_item.get("bid") is None) or not (ticker_item.get("ask") is None) :
                # если все нормально
                continue
            # если какие-то данные пустые
            orders_item = orders.get(exchange,{})
            bids = orders_item.get("bids",[])
            asks = orders_item.get("asks",[])
            if len(bids)== 0:
                bid = None
            else:
                bid = bids[0][0]
            if len(asks)== 0:
                ask = None
            else:
                ask = asks[0][0]

            data[exchange]["bid"] = bid
            data[exchange]["ask"] = ask

        return data
    def fill_data_to_send(self):
        data = []

        for symbol in self.symbol_exchange_map:
            item = {}
            fixed_tickers_data = self.fix_missing_tickers_data(self.ticker_data.get(symbol, {}),
                                                          self.order_book_data.get(symbol, {}))
            item["symbol"] = symbol
            item["tickers"] = self.ticker_data.get(symbol, {})
            item["order_books"] = self.order_book_data.get(symbol, {})
            item["ohlcvs"] = self.ohlcv_data.get(symbol, {})
            item["networks"] = self.networks_data.get(symbol, {})
            item['__timestamp'] = int(datetime.now(timezone.utc).timestamp())
            if len(item["tickers"])==0 or len(item["order_books"])==0:
                logger.info(f"Неполные данне по {symbol}, данные не отправляем")
                continue
            data.append(item)
        return data

    async def send_data(self):
        """Мониторинг тикеров для указанного символа."""
        try:
            while True:
                data = self.fill_data_to_send()
                #save_data_to_json(data=data, filename="./data/chart_helper.json")
                await self.dd.set_data(data)
                await asyncio.sleep(5)
        except Exception as e:
            logger.info(f"Ошибка сохранения: {e}")

    async def receive_data(self):
        """загрузка данных"""
        try:

            while True:
                updated = False
                # добавляем тестовые данные
                #if 'LTC/USDT' not in self.symbol_exchange_map:
                #    await self.add_symbol('LTC/USDT', ['binance', 'kraken'])
                # читаем входящие данные
                received_data_buffer = await self.dr.get_data()
                if not (received_data_buffer is None):
                    if len(received_data_buffer) > 0:
                        updated = True
                        symbols = []
                        self.received_data = received_data_buffer
                        for item in received_data_buffer:
                            symbol = item['analythics']['symbol']
                            symbols.append(symbol)
                            exchanges = list(item['data'][symbol].keys())
                            await self.add_symbol(symbol, exchanges)

                        symbols_to_delete = list(set(self.symbol_exchange_map.keys()) - set(symbols))
                        logger.info(f"Символы к исключению: {symbols_to_delete}")

                        for symbol_to_delete in symbols_to_delete:
                            self.remove_symbol(symbol_to_delete)

                await asyncio.sleep(5)
        except Exception as e:
            logger.info(f"Ошибка загрузки данных: {e}")

    async def monitor_exchange_symbol(self, exchange_id, symbol, exchange):
        """Запуск мониторинга тикеров, ордербуков и OHLCV данных параллельно для символа на бирже."""
        tasks = [
            self.monitor_ticker(exchange_id, symbol, exchange),
            self.monitor_order_book(exchange_id, symbol, exchange),
            #self.monitor_ohlcv(exchange_id, symbol, exchange),
        ]
        await asyncio.gather(*tasks)

    async def run(self):
        # Создаем экземпляр класса AppTemplate
        at = AppTemplate([('config=', "<config filename>")])
        at.get_arguments_from_env()
        at.get_arguments()
        at.load_settings_from_file(at.parameters['config'])
        self.api_keys = at.settings.get("api_keys",{})
        """Инициализация бирж и запуск мониторинга для всех символов."""
        self.dr = processor_template.DataReceiver(at.settings["user"], at.settings["password"], at.settings["host"],
                                                  at.settings["in_exchange"])

        self.dd = processor_template.DataDispatcher(at.settings["user"], at.settings["password"], at.settings["host"],
                                                    at.settings["out_exchange"])
        """Инициализация бирж и запуск мониторинга для всех символов."""
        await self.initialize_exchanges()
        self.client = AsyncRabbitMQClient(host='192.168.192.42')
        self.tasks = []

        # добавляем системные задачи
        self.tasks.append(self.send_data())
        self.tasks.append(self.receive_data())

        if self.tasks:
            await asyncio.gather(*self.tasks)
        else:
            logger.info("Нет задач для запуска.")


# Пример использования
if __name__ == "__main__":
    symbol_exchange_map = {}
    data_streamer = CryptoDataStreamer(symbol_exchange_map)
    #data_streamer.add_symbol('LTC/USDT', ['binance', 'kraken'])
    asyncio.run(data_streamer.run())
