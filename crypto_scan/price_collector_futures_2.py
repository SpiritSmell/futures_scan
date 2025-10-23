import asyncio
import time
from datetime import datetime, timezone
import ccxt.async_support as ccxt
import logging
import signal
from packages import rabbitmq_producer_2
from packages.app_template import AppTemplate

class PriceCollector:
    def __init__(self):
        self.logger = self._setup_logger()
        self.prices = {"data": {}}
        self.exchanges_data = {}
        self.exchanges_data_futures = {}
        self.lock = asyncio.Lock()
        self.app_template = None
        self.rabbitmq_client = None

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    async def fetch_tickers(self, exchange_id):
        exchange = getattr(ccxt, exchange_id)({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
        try:
            tickers = await exchange.fetch_tickers()
            async with self.lock:
                self.exchanges_data[exchange_id] = tickers
            self.logger.info(f'Собрана информация с биржи {exchange_id}')
        except ccxt.RequestTimeout as e:
            self.logger.error(f'Истекло время запроса для биржи {exchange_id}: {e}')
        except ccxt.NetworkError as e:
            self.logger.error(f'Сетевая ошибка для биржи {exchange_id}: {e}')
        except Exception as e:
            self.logger.error(f'Ошибка для биржи {exchange_id}: {e}')
        return exchange

    async def fetch_funding_rates(self, exchange_id):
        try:
            exchange = getattr(ccxt, exchange_id)({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
            all_markets = await exchange.fetch_markets()
            symbols_list = []
            for market in all_markets:
                if market['swap']:
                    symbols_list.append(market['symbol'])

            tickers = await exchange.fetch_funding_rates(symbols_list)
            async with self.lock:
                self.exchanges_data_futures[exchange_id] = tickers
            self.logger.info(f'Собрана информация с биржи {exchange_id}')
        except ccxt.RequestTimeout as e:
            self.logger.error(f'Истекло время запроса для биржи {exchange_id}: {e}')
        except ccxt.NetworkError as e:
            self.logger.error(f'Сетевая ошибка для биржи {exchange_id}: {e}')
        except Exception as e:
            self.logger.error(f'Ошибка для биржи {exchange_id}: {e}')
        return exchange

    def reformat_exchanges_data_futures(self):
        result = {}
        for exchange_id in self.exchanges_data_futures:
            exchange_data = self.exchanges_data_futures[exchange_id]
            for futures_id in exchange_data:
                futures = exchange_data[futures_id]
                if futures_id not in result:
                    result[futures_id] = {}
                result[futures_id][exchange_id] = {
                    'fundingRate': futures['fundingRate'],
                }
        return result

    def signal_handler(self, signum, frame):
        self.logger.info('Получен сигнал прерывания. Грациозно завершаем скрипт.')
        loop = asyncio.get_event_loop()
        loop.stop()

    async def main(self):
        self.logger.info('Price collector запущен.')
        self.app_template = AppTemplate([('config=', "<config filename>")])
        self.app_template.get_arguments_from_env()
        self.app_template.get_arguments()
        self.app_template.load_settings_from_file(self.app_template.parameters['config'])

        signal.signal(signal.SIGINT, self.signal_handler)

        self.rabbitmq_client = rabbitmq_producer_2.RabbitMQClient(
            host=self.app_template.settings["host"],
            exchange=self.app_template.settings["out_exchange"],
            user=self.app_template.settings["user"],
            password=self.app_template.settings["password"]
        )

        exchanges = self.app_template.settings["exchanges"]

        while True:
            tasks = [self.fetch_tickers(exchange_id) for exchange_id in exchanges]
            exchanges_results = await asyncio.gather(*tasks)

            tasks = [self.fetch_funding_rates(exchange_id) for exchange_id in exchanges]
            exchanges_results = await asyncio.gather(*tasks)

            futures = self.reformat_exchanges_data_futures()

            max_funding_rate_range = 0
            max_funding_rate_exchange = ''
            min_funding_rate_exchange = ''
            max_funding_rate_range_symbol = ''

            for future_id in futures:
                future = futures[future_id]
                max_funding_rate = -1000000
                min_funding_rate = 1000000
                current_min_funding_rate_exchange = ''
                current_max_funding_rate_exchange = ''

                for exchange_id in future:
                    current_funding_rate = future[exchange_id]['fundingRate']
                    if current_funding_rate is None:
                        current_funding_rate = 0
                    if current_funding_rate > max_funding_rate:
                        max_funding_rate = current_funding_rate
                        current_max_funding_rate_exchange = exchange_id
                    if current_funding_rate < min_funding_rate:
                        min_funding_rate = current_funding_rate
                        current_min_funding_rate_exchange = exchange_id
                if abs(max_funding_rate - min_funding_rate) > max_funding_rate_range:
                    max_funding_rate_range = abs(max_funding_rate - min_funding_rate)
                    max_funding_rate_range_symbol = future_id
                    max_funding_rate_exchange = current_max_funding_rate_exchange
                    min_funding_rate_exchange = current_min_funding_rate_exchange

            self.logger.info(f"Max funding rate is {max_funding_rate_range}, symbol {max_funding_rate_range_symbol}, "
                            f"min  {min_funding_rate_exchange} "
                            f"max  {max_funding_rate_exchange} ")

            self.prices['__timestamp'] = int(datetime.now(timezone.utc).timestamp())
            try:
                self.rabbitmq_client.send_to_rabbitmq(self.prices, fanout=True)
            except Exception as e:
                self.logger.error(f'Ошибка при отправке данных в RabbitMQ: {e}')

            for exchange in exchanges_results:
                await exchange.close()

            self.logger.info(f'Спим {self.app_template.settings["polls_delay"]} секунд.')
            await asyncio.sleep(self.app_template.settings["polls_delay"])  # Используем asyncio.sleep вместо time.sleep

if __name__ == '__main__':
    collector = PriceCollector()
    asyncio.run(collector.main())