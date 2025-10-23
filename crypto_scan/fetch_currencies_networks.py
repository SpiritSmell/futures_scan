import json
import time

from packages.mongo import MongoConnector
from packages.app_template import AppTemplate
import logging
from packages.clickhouse import ClickhouseConnector
import pandas as pd
import ccxt
from packages import rabbitmq_producer_2


# Список бирж для обработки
EXCHANGES = [
    'binance', 'bitget', 'bybit', 'gateio','mexc',
    'htx', 'kucoin', 'kraken', 'poloniex','bitget'
]

# Ваши API-ключи и секреты для каждой биржи (при необходимости)
API_KEYS = {
    'binance': {
        'apiKey': 'p6QjdoLPjWGTmSTQekaaUMocFLIPKiNIidkEeh7YMslolON0199d37n8JlqNJyeW',
        'secret': 'xuvszRFMJEaCDoE5DWRXV8FbXnLE3uvKI5L4CJXkzcyuSbiTQwjATXKancyvI5K5'
    },
    'bybit': {
        'apiKey': 'mr4L4DA3iNaoc6R7dv',
        'secret': 'YjFGTumcETsRVlPNzuQk5SZCzqESsz7nxiIm'
    },
    'mexc': {
        'apiKey': 'mx0vglRHG3gvae6Ffj',
        'secret': 'ad5f97b69af04b05afb38bf15db9b1ce'
    },
    'okx': {
        'apiKey': 'be043c33-10f2-41b4-924a-efc09dba2e8e',
        'secret': '3EBDD396CF740FB723A41F9D035EAA47'
    }

}

# Словарь для хранения данных о валютах
exchange_data= {}

# Настройка логгера для текущего модуля
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Создание обработчика для вывода логов в консоль
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

# Создание форматирования для логов
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Добавление обработчика к логгеру
logger.addHandler(handler)




def load_json_from_file(file_path):
    """
    Загружает JSON-данные из файла.

    :param file_path: Путь к файлу с JSON.
    :return: Данные в формате Python (обычно словарь или список).
    :raises FileNotFoundError: Если файл не найден.
    :raises json.JSONDecodeError: Если содержимое файла не является корректным JSON.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Ошибка: Файл '{file_path}' не найден.")
        raise
    except json.JSONDecodeError as e:
        print(f"Ошибка: Некорректный JSON в файле '{file_path}'.\nДетали: {e}")
        raise


def transform_data(data):
    """
    Преобразует структуру JSON, перемещая валюты на верхний уровень.

    Args:
        data (dict): Оригинальный JSON-объект.

    Returns:
        dict: Преобразованный JSON-объект.
    """
    result = {}
    for exchange, currencies in data.items():
        for currency, details in currencies.items():
            if currency not in result:
                result[currency] = {}
            result[currency][exchange] = details
    return result


def save_to_file(data, filename):
    """
    Сохраняет данные в JSON файл.

    Args:
        data (dict): Данные для сохранения.
        filename (str): Имя файла для сохранения.
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False, sort_keys=True)
    print(f"Данные успешно сохранены в файл: {filename}")

def fetch_currency_info(exchange_name: str) -> None:
    """
    Получает информацию о валютах с указанной биржи и сохраняет в словарь.

    :param exchange_name: Название биржи из списка ccxt.
    """
    currencies = {}
    try:
        logger.debug(f"Инициализация {exchange_name}")
        exchange = getattr(ccxt, exchange_name)(API_KEYS.get(exchange_name, {'enableRateLimit': True}))

        # Загрузка информации о валютах
        currencies = exchange.fetch_currencies()
        logger.info(f"Успешно получены данные с {exchange_name}: {len(currencies)} записей")

    except Exception as e:
        logger.error(f"Ошибка при обработке {exchange_name}: {e}")
        currencies = {}
    finally:
        return currencies


# Пример использования:
if __name__ == "__main__":
    logger.info('transform_currencies_network_data запущен')
    # Создаем экземпляр класса AppTemplate
    at = AppTemplate([('config=', "<config filename>")])
    at.get_arguments_from_env()
    at.get_arguments()
    at.load_settings_from_file(at.parameters['config'])

    CONNECTION_STRING = (f'http://{at.settings["clickhouse_user"]}:{at.settings["clickhouse_password"]}@'
                         f'{at.settings["clickhouse_host"]}:{at.settings["clickhouse_port"]}/')
    clickhouse_connector = ClickhouseConnector(connection_string=CONNECTION_STRING,
                                               database_name=at.settings["clickhouse_database_name"],
                                               table_name=at.settings["clickhouse_table_name"])

    client = rabbitmq_producer_2.RabbitMQClient(host=at.settings["host"],
                                                exchange=at.settings["out_exchange"],
                                                user=at.settings["user"],
                                                password=at.settings["password"])

    json_data = {}

    while True:

        try:

            for exchange_name in EXCHANGES:
                json_data[exchange_name] = fetch_currency_info(exchange_name)
            #json_data = load_json_from_file("./data/exchange_usdt_data.json")
            #logger.info(f"{len(json_data)} records loaded")
        except Exception as e:
            logger.error(f"Ошибка при загрузке JSON: {e}")

        transformed_data = transform_data(json_data)

        try:
            client.send_to_rabbitmq(transformed_data, fanout=True)
        except Exception as e:
            logger.error(f'Ошибка при отправке данных в RabbitMQ: {e}')



        df = pd.DataFrame(
            [(key, json.dumps(value)) for key, value in transformed_data.items()],
            columns=["symbol", "json"]
        )

        clickhouse_connector.save_dataframe(df, replace=False)

        query = f"""
            ALTER TABLE {at.settings["clickhouse_database_name"]}.{at.settings["clickhouse_table_name"]} 
            DELETE WHERE __metatimestamp_timestamp NOT IN (
            SELECT DISTINCT __metatimestamp_timestamp
            FROM {at.settings["clickhouse_database_name"]}.{at.settings["clickhouse_table_name"]} 
            ORDER BY __metatimestamp_timestamp DESC
            LIMIT 10
            )
            """
        # очищаем лишние записи
        clickhouse_connector.query(query)


        logger.info(f"Sleeping {at.settings['polls_delay']} seconds")
        time.sleep(at.settings['polls_delay'])

