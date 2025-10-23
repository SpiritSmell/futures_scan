import asyncio
import logging
from json_to_clickhouse import infer_table_structure, ClickHouseJSONHandler, make_connection_string

# Задержка между основными циклами обработки
MAIN_LOOP_DELAY_TIME = 5

def setup_logger(logger_name, level=logging.DEBUG):
    """
    Настраивает логгер для записи сообщений в консоль.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    handler = logging.StreamHandler()
    handler.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger


# Настройка логгера
logger = setup_logger(__name__)

class ClickhouseSaver:
    """
    Класс для отправки обработанных данных в RabbitMQ.
    """

    def __init__(self, url, input_queue, loop, database, table_name):
        self.url = url
        self.input_queue = input_queue
        self.loop = loop
        self.database = database
        connection_string = make_connection_string(host='192.168.192.42')
        self.handler = ClickHouseJSONHandler(connection_string, database=self.database)
        self.table_name = table_name

    async def start(self):
        """
        Запускает процесс отправки данных в Clickhouse.
        """
        while True:
            try:
                logger.info(f"[Sender] Подключение к Clickhouse {self.database}")
                # Создаем таблицу в ClickHouse
                table_name = "cryptoscan_snap_futures_futurespricecollector_v1_1"
                # Вставляем данные в таблицу
                logger.info("[Savior] Готов к отправке данных...")

                while True:
                    data = await self.input_queue.get()
                    logger.info(f"[Savior] Отправка данных: {len(data)}")
                    self.handler.insert_json_data(table_name, data)
            except asyncio.CancelledError:
                logger.info("[Savior] Завершение работы...")
                break
            except Exception as e:
                logger.info(f"[Sender] Ошибка соединения: {e}. Повторная попытка через {MAIN_LOOP_DELAY_TIME} секунд...")
                await asyncio.sleep(MAIN_LOOP_DELAY_TIME)

async def main():
    """
    Главная асинхронная функция для запуска всех сервисов.
    """
    loop = asyncio.get_event_loop()
    raw_data_queue = asyncio.Queue()
    processed_data_queue = asyncio.Queue()

    sender = ClickhouseSaver(RABBITMQ_URL, processed_data_queue, loop, OUT_EXCHANGE)

    tasks = [
        asyncio.create_task(receiver.start()),
        asyncio.create_task(processor.start()),
        asyncio.create_task(sender.start()),
        asyncio.create_task(send_test_data(loop))
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("[Main] Завершение работы...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())