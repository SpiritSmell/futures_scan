import asyncio
from packages.data_pipeline import DataReceiver, DataProcessor, DataSender, url_from_parameters
from packages.app_template import AppTemplate, logger
from packages.futures_utils import calculate_funding_spread


class FuturesDataProcessor(DataProcessor):
    def process_data(self, data):
        """
        Переопределяет метод обработки данных.
        Рассчитывает спред финансирования для данных о фьючерсах.

        :param data: Входные данные в формате словаря
        :return: Рассчитанные спреды финансирования
        """
        logger.info("[CustomProcessor] Пользовательская обработка данных")
        spreads = calculate_funding_spread(data.get("futures", {}))
        return spreads


async def main():
    """
    Запускает процесс сбора, обработки и отправки данных.
    """
    # Инициализация приложения с параметрами конфигурации
    at = AppTemplate([('config=', "<config filename>")])
    at.get_arguments_from_env()  # Получение аргументов из переменных окружения
    at.get_arguments()  # Получение аргументов командной строки
    at.load_settings_from_file(at.parameters['config'])  # Загрузка настроек из файла

    # Получаем текущий событийный цикл
    loop = asyncio.get_event_loop()

    # Создаем асинхронные очереди для передачи данных между компонентами
    raw_data_queue = asyncio.Queue()  # Очередь для необработанных данных
    processed_data_queue = asyncio.Queue()  # Очередь для обработанных данных

    # Создаем экземпляры классов для получения, обработки и отправки данных
    receiver = DataReceiver(
        url_from_parameters(at.settings["host"], at.settings["user"], at.settings["password"]),
        raw_data_queue, loop, at.settings["in_exchange"]
    )
    processor = FuturesDataProcessor(raw_data_queue, processed_data_queue)
    sender = DataSender(
        url_from_parameters(at.settings["host"], at.settings["user"], at.settings["password"]),
        processed_data_queue, loop, at.settings["out_exchange"]
    )

    # Запускаем все три компонента параллельно
    await asyncio.gather(
        receiver.start(),  # Запуск получения данных
        processor.start(),  # Запуск обработки данных
        sender.start()  # Запуск отправки данных
    )


if __name__ == "__main__":
    # Запуск асинхронного процесса
    asyncio.run(main())
