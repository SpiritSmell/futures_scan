import asyncio
from packages.data_pipeline import (DataReceiver, DataProcessor, DataSender,
                                    url_from_parameters, MockDataReceiver)
from packages.clickhouse_saver import ClickhouseSaver
from packages.app_template import AppTemplate, logger
from packages.json_utils import load_data_from_json
from json_to_clickhouse import infer_table_structure, ClickHouseJSONHandler, make_connection_string
from jsonpath_ng import parse

DEBUG = False


def transform_futures_data(data):
    result = []
    record = {}
    for exchange_name, exchange_value in data.items():
        for future_name, future_value in exchange_value.items():
            record = future_value
            record["exchange"] = exchange_name
            record.pop("info", None)
            result.append(record)
    return result


class FuturesDataProcessor(DataProcessor):
    def __init__(self, input_queue, output_queue,json_path_filter=None):
        super().__init__(input_queue, output_queue)
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.jsonpath_expression = None
        if json_path_filter:
            self.jsonpath_expression = parse(json_path_filter)

    def process_data(self, data):
        """
        Переопределяет метод обработки данных.
        Рассчитывает спред финансирования для данных о фьючерсах.

        :param data: Входные данные в формате словаря
        :return: Рассчитанные спреды финансирования
        """
        logger.info("[CustomProcessor] Пользовательская обработка данных")
        # Определяем структуру таблицы на основе JSON
        json_data = data
        if self.jsonpath_expression:
            json_data = self.jsonpath_expression.find(data)[0].value
        transformed_futures = transform_futures_data(json_data)

        return transformed_futures


async def async_main():
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
    if DEBUG:
        receiver = MockDataReceiver(
            url_from_parameters(at.settings["host"], at.settings["user"], at.settings["password"]),
            raw_data_queue, loop, at.settings["in_exchange"], "./data/futures_price_collector_2.json"
        )
    else:
        receiver = DataReceiver(
            url_from_parameters(at.settings["host"], at.settings["user"], at.settings["password"]),
            raw_data_queue, loop, at.settings["in_exchange"]
        )
    processor = FuturesDataProcessor(raw_data_queue, processed_data_queue,at.settings.get("json_path_filter",None))

    url = make_connection_string(at.settings["clickhouse_host"],
                                 at.settings["clickhouse_user"],
                                 at.settings["clickhouse_password"])
    sender = ClickhouseSaver(
        url, processed_data_queue, loop, at.settings["database"],
        at.settings["table_name"]
    )

    # Запускаем все три компонента параллельно
    await asyncio.gather(
        receiver.start(),  # Запуск получения данных
        processor.start(),  # Запуск обработки данных
        sender.start()  # Запуск отправки данных
    )


def create_table(handler, data,table_name):
    # Определяем структуру таблицы на основе JSON
    #first_record = data[0]
    #futures_df = pd.DataFrame(data, columns=list(first_record.keys()))
    structure = infer_table_structure(data)
    # Создаем таблицу в ClickHouse
    handler.create_table(table_name, structure)
    # Вставляем данные в таблицу
    print(f"Таблица {table_name} успешно создана")

def main():
    # Определяем структуру таблицы на основе JSON
    json_data = load_data_from_json("./data/futures_price_collector_2.json")
    #transformed_futures = transform_futures_data(json_data["futures"])

    json_path_filter = None
    json_path_filter = '$.tickers'
    if json_path_filter:
        jsonpath_expression = parse(json_path_filter)
        json_data = jsonpath_expression.find(json_data)[0].value
    transformed_futures = transform_futures_data(json_data)

    # Создаем экземпляр обработчика с указанием хоста и базы данных
    connection_string = make_connection_string(host='192.168.192.42')
    handler = ClickHouseJSONHandler(connection_string, database='mart')
    # Создаем таблицу в ClickHouse
    #table_name = "cryptoscan_snap_futures_futurespricecollector_v1_1"
    table_name = "cryptoscan_snap_tickers_futurespricecollector_v1_1"
    create_table(handler,transformed_futures,table_name)
    # Вставляем данные в таблицу
    handler.insert_json_data(table_name, transformed_futures)
    print("Данные успешно вставлены в ClickHouse")
    pass


if __name__ == "__main__":
    # Запуск асинхронного процесса
    if DEBUG:
        main()
    else:
        asyncio.run(async_main())
