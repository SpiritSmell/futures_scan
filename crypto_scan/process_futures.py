import asyncio
import json
from copy import deepcopy
from datetime import datetime, timezone
import ccxt.pro as ccxt_async
import ccxt as ccxt_sync
from packages.rabbitmq_producer_2_async import AsyncRabbitMQClient
from packages import processor_template
from packages.app_template import AppTemplate, logger
from packages.clickhouse import ClickhouseConnector
import hashlib


class CryptoDataProcessor:
    def __init__(self) -> None:
        self.api_keys: dict = {}
        self.tasks: list = []
        self.dd = None
        self.dr = None
        self.polls_delay = 1

    async def close(self) -> None:
        """Закрывает все асинхронные экземпляры бирж."""

    def __del__(self) -> None:
        """Закрывает ресурсы при уничтожении объекта."""


    async def process_data(self) -> None:
        """Обрабатывает данные с бирж."""
        tasks = []
        tasks.extend([self.send_data()])
        await asyncio.gather(*tasks)

    def fill_data_to_send(self):
        data = {}
        data['__timestamp'] = int(datetime.now(timezone.utc).timestamp())

        return data

    async def send_data(self):
        """Мониторинг тикеров для указанного символа."""
        try:
            old_tickers_hash = None
            old_futures_hash = None

            while True:
                data = self.fill_data_to_send()
                new_tickers_hash = self.calculate_tickers_hash(data.get("tickers",{}))
                new_futures_hash = self.calculate_futures_hash(data.get("futures",{}))

                if old_tickers_hash != new_tickers_hash or old_futures_hash != new_futures_hash:
                    await self.dd.set_data(data)
                    old_tickers_hash = new_tickers_hash
                    old_futures_hash = new_futures_hash

                await asyncio.sleep(1)
        except Exception as e:
            logger.info(f"Ошибка сохранения: {e}")

    def calculate_tickers_hash(self, data):
        """Вычисляет хэш данных тикеров."""
        tickers_data = [item["tickers"] for item in data]
        return hashlib.md5(json.dumps(tickers_data, sort_keys=True).encode()).hexdigest()

    def calculate_futures_hash(self, data):
        """Вычисляет хэш данных фьючерсов."""
        futures_data = [item["futures"] for item in data]
        return hashlib.md5(json.dumps(futures_data, sort_keys=True).encode()).hexdigest()

    async def run(self) -> None:
        """Запускает процесс сбора данных."""
        at = AppTemplate([('config=', "<config filename>")])
        at.get_arguments_from_env()
        at.get_arguments()
        at.load_settings_from_file(at.parameters['config'])
        # инициализируем и запускаем цикл получения данных
        self.dr = processor_template.DataReceiver(at.settings["user"], at.settings["password"], at.settings["host"],
                                             at.settings["in_exchange"])


        self.dd = processor_template.DataDispatcher(at.settings["user"], at.settings["password"], at.settings["host"],
                                                    at.settings["out_exchange"])

        self.polls_delay = at.settings.get("polls_delay", 10)

        #clickhouse_connector = ClickhouseConnector(connection_string=CONNECTION_STRING,
        #                                           database_name=at.settings["clickhouse_database_name"],
        #                                           table_name=at.settings["clickhouse_table_name"])

        try:
            await self.process_data()
        finally:
            await self.close()




# Пример использования
if __name__ == "__main__":
    symbol_exchange_map = {}
    data_streamer = CryptoDataProcessor()
    asyncio.run(data_streamer.run())
