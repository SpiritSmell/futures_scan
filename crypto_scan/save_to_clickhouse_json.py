import asyncio
import logging
from packages.data_pipeline import (DataReceiver, DataProcessor, DataSender,
                                    url_from_parameters, MockDataReceiver)
from packages.clickhouse_saver_json import ClickhouseSaver
from packages.app_template import AppTemplate, logger
from packages.json_utils import load_data_from_json
from jsonpath_ng import parse
from json_to_clickhouse import ClickHouseJSONHandler , make_connection_string
from typing import Any, Dict, List, Optional

DEBUG = False


def setup_logging():
    """
    Настраивает логирование для вывода сообщений уровня INFO и выше.
    """
    # Настройка корневого логгера
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Вывод в консоль
            logging.FileHandler('save_to_clickhouse_json.log', encoding='utf-8')  # Запись в файл
        ]
    )
    
    # Настройка конкретного логгера для этого модуля
    module_logger = logging.getLogger(__name__)
    module_logger.setLevel(logging.INFO)
    
    # Настройка логгера из app_template
    app_logger = logging.getLogger('packages.app_template')
    app_logger.setLevel(logging.INFO)
    
    return module_logger


# Инициализируем логирование при импорте модуля
module_logger = setup_logging()


def safe_get_nested_data(data: Dict[str, Any], path: List[str], default: Any = None) -> Any:
    """
    Безопасно извлекает вложенные данные из словаря по указанному пути.
    
    Args:
        data: Исходный словарь с данными
        path: Список ключей для навигации по вложенной структуре
        default: Значение по умолчанию, если путь не найден
    
    Returns:
        Извлеченные данные или значение по умолчанию
    
    Example:
        safe_get_nested_data(data, ['data', 'items', 0, 'data'])
    """
    try:
        current = data

        for key in path:
            if isinstance(current, dict):
                current = current.get(key)
            elif isinstance(current, list) and isinstance(key, int):
                if 0 <= key < len(current):
                    current = current[key]
                else:
                    return default
            else:
                return default
            
            if current is None:
                return default
        
        return current
    except (KeyError, IndexError, TypeError) as e:
        module_logger.warning(f"Ошибка при извлечении данных по пути {path}: {e}")
        return default


def extract_json_data_safely(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Безопасно извлекает данные из JSON структуры.
    Пробует несколько возможных путей к данным.
    
    Args:
        data: Исходные данные
    
    Returns:
        Извлеченные данные или None, если данные не найдены
    """
    # Возможные пути к данным (в порядке приоритета)
    possible_paths = [
        ['data', 'items', 0, 'data'],  # Оригинальный путь
        ['data', 'items', 0],          # Без последнего 'data'
        ['data', 'items'],             # Массив items целиком
        ['data'],                      # Только data
        [],                            # Корневой уровень
    ]
    
    for path in possible_paths:
        result = safe_get_nested_data(data, path)
        if result is not None:
            module_logger.info(f"Данные найдены по пути: {path}")
            return result
    
    module_logger.error("Не удалось найти данные ни по одному из известных путей")
    return None


def extract_json_data_type(data: Dict[str, Any]) -> Optional[str]:
    """
    Безопасно извлекает данные из JSON структуры.
    Пробует несколько возможных путей к данным.

    Args:
        data: Исходные данные

    Returns:
        Извлеченные данные или None, если данные не найдены
    """
    # Возможные пути к данным (в порядке приоритета)
    possible_paths = [
        ['data', 'type'],  # Оригинальный путь
    ]
    for path in possible_paths:
        result = safe_get_nested_data(data, path)
        if result is not None:
            module_logger.info(f"Данные найдены по пути: {path}")
            return result

    module_logger.error("Не удалось найти данные ни по одному из известных путей")
    return None

def transform_futures_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Трансформирует данные о фьючерсах в плоский список записей.
    
    Args:
        data: Словарь с данными о фьючерсах по биржам
    
    Returns:
        Список трансформированных записей
    
    Raises:
        ValueError: Если входные данные имеют неправильный формат
    """
    if not isinstance(data, dict):
        raise ValueError(f"Ожидался словарь, получен {type(data)}")
    
    result = []
    
    for exchange_name, exchange_value in data.items():
        if not isinstance(exchange_value, dict):
            module_logger.warning(f"Пропускаем биржу {exchange_name}: данные не являются словарем")
            continue
            
        for future_name, future_value in exchange_value.items():
            if not isinstance(future_value, dict):
                module_logger.warning(f"Пропускаем фьючерс {future_name} на бирже {exchange_name}: данные не являются словарем")
                continue
                
            # Создаем копию записи, чтобы не изменять исходные данные
            record = future_value.copy()
            record["exchange"] = exchange_name
            record["symbol"] = future_name  # Добавляем имя символа для удобства
            
            # Безопасно удаляем поле info если оно существует
            record.pop("info", None)
            
            result.append(record)
    
    module_logger.info(f"Трансформировано {len(result)} записей из {len(data)} бирж")
    return result


class FuturesDataProcessor(DataProcessor):
    def __init__(self, input_queue, output_queue,jsonpath_filter=None):
        super().__init__(input_queue, output_queue)
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.jsonpath_filter=None
        self.jsonpath_expression = None
        if jsonpath_filter:
            self.jsonpath_filter = jsonpath_filter
            self.jsonpath_expression = parse(jsonpath_filter)

    def process_data(self, data):
        """
        Переопределяет метод обработки данных.
        Рассчитывает спред финансирования для данных о фьючерсах.

        :param data: Входные данные в формате словаря
        :return: Рассчитанные спреды финансирования
        """
        module_logger.info("[CustomProcessor] Начало пользовательской обработки данных")
        
        # Валидация входных данных
        if not isinstance(data, dict):
            module_logger.error(f"Ожидался словарь, получен {type(data)}")
            return []
        
        # Извлечение типа данных для фильтрации
        json_data_type = extract_json_data_type(data)
        module_logger.info(f"Обнаружен тип данных: {json_data_type}")
        
        # Проверка соответствия типа данных фильтру
        if self.jsonpath_filter and json_data_type and json_data_type != self.jsonpath_filter:
            module_logger.info(f"Тип данных '{json_data_type}' не соответствует фильтру '{self.jsonpath_filter}', пропускаем")
            return []
        
        # Извлечение данных с приоритетом JSONPath
        json_data = self._extract_data_with_fallback(data)
        
        if json_data is None:
            module_logger.error("Не удалось извлечь данные для обработки")
            return []
        
        # Трансформация данных с обработкой ошибок
        return self._transform_data_safely(json_data)
    
    def _extract_data_with_fallback(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Извлекает данные с приоритетом JSONPath и fallback на безопасное извлечение.
        
        Args:
            data: Исходные данные
            
        Returns:
            Извлеченные данные или None
        """
        if self.jsonpath_expression:
            try:
                matches = self.jsonpath_expression.find(data)
                if matches:
                    json_data = matches[0].value
                    module_logger.info("Данные успешно извлечены с помощью JSONPath")
                    return json_data
                else:
                    module_logger.warning("JSONPath не нашел совпадений, переходим к безопасному извлечению")
            except Exception as e:
                module_logger.error(f"Ошибка при использовании JSONPath: {e}, переходим к безопасному извлечению")
        
        # Fallback на безопасное извлечение
        return extract_json_data_safely(data)
    
    def _transform_data_safely(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Безопасно трансформирует данные с детальным логированием.
        
        Args:
            json_data: Данные для трансформации
            
        Returns:
            Список трансформированных записей
        """
        try:
            if not isinstance(json_data, dict):
                module_logger.error(f"Ожидался словарь для трансформации, получен {type(json_data)}")
                return []
            
            transformed_futures = transform_futures_data(json_data)
            module_logger.info(f"Успешно обработано {len(transformed_futures)} записей")
            return transformed_futures
            
        except ValueError as e:
            module_logger.error(f"Ошибка валидации при трансформации данных: {e}")
            return []
        except Exception as e:
            module_logger.error(f"Неожиданная ошибка при трансформации данных: {e}")
            return []


async def async_main():
    """
    Запускает процесс сбора, обработки и отправки данных.
    """
    # Убеждаемся, что логирование настроено
    module_logger.info("Запуск async_main - логирование активировано")
    
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



def main():
    # Убеждаемся, что логирование настроено
    module_logger.info("Запуск main - логирование активировано")
    
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
    table_name = 'complex_json_data_test'
    handler = ClickHouseJSONHandler(connection_string, database='mart', table_name=table_name, json_as_string=False)
    # Создаем таблицу в ClickHouse
    #table_name = "cryptoscan_snap_futures_futurespricecollector_v1_1"
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
