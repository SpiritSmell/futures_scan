# takes json from price_collector and filter all records with tolerance less than TOLERANCE
# save remaining records and some analythics to
import json
import copy
import os
import sys
import logging
from packages import rabbitmq_producer, rabbitmq_consumer
from packages import rabbitmq_producer_2
from datetime import datetime, timezone
from packages.app_template import AppTemplate, logger
from packages.measure_execution_time import measure_execution_time
import pandas as pd
import numpy as np



TOLERANCE = 0.01

HOST = '192.168.192.42'
HOST = '192.168.56.107'
USER = 'rmuser'
PASSWORD = 'rmpassword'

SOURCE_FILE = 'prices_price_collector_out.json'
RAW_FILE = 'prices_price_filter_raw.json'
OUT_FILE = 'prices_price_filter_out.json'
IN_QUEUE = "price_collector_out"
OUT_QUEUE = "price_filter_out"
IN_EXCHANGE = "price_collector_out"
OUT_EXCHANGE = "price_filter_out"

#OUT_QUEUE = "hello"
FILE_MODE = False

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


def load_data_from_json(filename):
    with open(filename, 'r') as f:
        prices = json.load(f)

    return prices



def save_data_to_json(data, temp_filename, final_filename):
    with open(temp_filename, 'w') as f:
        json.dump(data, f)
    os.remove(final_filename)
    os.rename(temp_filename, final_filename)


def is_futures(input_string):
    return '3' in input_string


import sys
import copy
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

def filter_prices(input_prices, tolerance=0.0):
    """
    Фильтрует цены на основе заданной толерантности, определяя валюты с выгодным спредом между минимальным ask и максимальным bid.

    :param prices: Словарь цен в формате {currency: {exchange: {'ask': str, 'bid': str}}}
    :param tolerance: Толерантность, выраженная в долях (например, 0.01 для 1%)
    :return: Список отфильтрованных цен с аналитической информацией
    """
    if input_prices is None:
        return []

    prices = input_prices["data"]

    filtered_prices = []

    for currency, exchanges in prices.items():
        # Пропускаем валюты, относящиеся к фьючерсам
        if is_futures(currency):
            continue

        min_ask = sys.float_info.max
        max_bid = 0.0
        source = destination = None

        # Обрабатываем данные по всем биржам для данной валюты
        for exchange, data in exchanges.items():
            try:
                ask = float(data.get('ask', sys.float_info.max) or sys.float_info.max)
                bid = float(data.get('bid', 0.0) or 0.0)
            except ValueError:
                logger.warning(f"Некорректные данные для {currency} на бирже {exchange}: {data}")
                continue

            if ask > 0.0 and ask < min_ask:
                min_ask = ask
                source = exchange

            if bid > max_bid:
                max_bid = bid
                destination = exchange

        # Проверяем, стоит ли включать валюту в результаты
        if min_ask * (1 + tolerance) < max_bid:
            spread = max_bid / min_ask - 1
            item = {
                'data': {
                         currency: exchanges,
                         '__timestamp': input_prices['__timestamp']
                         },
                'analythics': {
                    'symbol': currency,
                    'destination': destination,
                    'max_bid': max_bid,
                    'source': source,
                    'min_ask': min_ask,
                    'spread': spread,
                    '__timestamp': int(datetime.now(timezone.utc).timestamp())
                }
            }
            filtered_prices.append(item)

            logger.debug(f"Найдено: {currency} spread: {spread:.2%} bid: {max_bid} ask: {min_ask}")

    logger.debug(f"Обнаружено {len(filtered_prices)} валютных пар") if filtered_prices else logger.debug("Подходящих валютных пар не найдено")
    return filtered_prices



def transform_data(data, tolerance):
    # Преобразуем в DataFrame и транспонируем
    df = pd.DataFrame(data).T

    tolerance_multiplier = 1.0+tolerance

    # Функция для безопасного преобразования в float
    def safe_convert(value):
        if value is None:
            return np.nan
        try:
            return float(value)
        except ValueError:
            return np.nan  # Возвращаем NaN в случае ошибки преобразования

    # Применяем функцию ко всем значениям в DataFrame
    df = df.map(safe_convert)

    # Устанавливаем максимальное значение для ask и 0 для bid, если есть NaN
    df['ask'] = df['ask'].fillna(df['ask'].max())
    df['bid'] = df['bid'].fillna(0)

    max_bid = df['bid'].max()
    min_ask = df['ask'].min()

    # Фильтрация данных: оставляем строки, у которых bid > min_ask или ask < max_bid
    df_filtered = df[(df['bid'] > min_ask*tolerance_multiplier) |
                     (df['ask']*tolerance_multiplier < max_bid)]

    if len(df_filtered)<2:
        return df_filtered, None, None, None, None

    df_max_bid = df_filtered[(df_filtered['bid'] == max_bid)]
    df_min_ask = df_filtered[(df_filtered['ask'] == min_ask)]

    max_bid_exchange = df_max_bid.index[0]
    min_ask_exchange = df_min_ask.index[0]

    return df_filtered, max_bid_exchange, min_ask_exchange, max_bid,  min_ask

def transform_data_numpy(data, tolerance):
    # Преобразуем словарь в массив NumPy
    exchanges = np.array(list(data.keys()))
    values = np.array([[entry.get('ask', np.nan), entry.get('bid', np.nan)] for entry in data.values()], dtype=float)

    tolerance_multiplier = 1.0 + tolerance

    # Обрабатываем NaN: заменяем NaN в 'ask' на максимум, в 'bid' на 0
    asks = np.nan_to_num(values[:, 0], nan=np.nanmax(values[:, 0]))
    bids = np.nan_to_num(values[:, 1], nan=0)

    # Вычисляем максимальный bid и минимальный ask
    max_bid = np.max(bids)
    min_ask = np.min(asks)

    # Фильтруем данные
    mask = (bids > min_ask * tolerance_multiplier) | (asks * tolerance_multiplier < max_bid)
    filtered_indices = np.where(mask)[0]

    if len(filtered_indices) < 2:
        return None, None, None, None, None

    # Определяем индексы максимального bid и минимального ask
    max_bid_index = np.argmax(bids)
    min_ask_index = np.argmin(asks)

    return exchanges[filtered_indices],exchanges[max_bid_index],exchanges[min_ask_index],max_bid,min_ask

def filter_pairs(prices, tolerance=0.0):
    filtered_prices = []

    if prices is None:
        return []

    for currency in prices["data"]:
        min_ask = sys.float_info.max
        max_bid = 0.0
        current_currency = prices[currency]
        if len(current_currency)<=1:
            # Оптимизация. Не обрабатываем валюты, которые представлены только на одной бирже.
            continue
        # check if token belong to futures
        if is_futures(currency):
            continue

        # преобразуем в DataFrame, отфильтровываем нерентабельные биржи
        df, source, destination, max_bid, min_ask = transform_data_numpy(current_currency, tolerance)
        if df is None:
            continue
        #print(df)


        item = {'data': {currency: current_currency},
                'analythics': {'symbol': currency,
                               'destination': destination,
                               'max_bid': max_bid,
                               'source': source,
                               'min_ask': min_ask,
                               'spread': max_bid / min_ask - 1,
                               '__timestamp': int(datetime.now(timezone.utc).timestamp()),
                               'dataframe': df
                               }
                }
        filtered_prices.append(copy.deepcopy(item))
        logger.debug(
            f"found {currency} {max_bid / min_ask - 1} bid:{max_bid} ask:{min_ask} spread:{max_bid / min_ask - 1}%")

    if len(filtered_prices) > 0:
        logger.debug(f"found {len(filtered_prices)} currency pairs")

    return filtered_prices

def main():
    # init RabbitMQ consumer
    logger.info('Price collector запущен.')
    # Создаем экземпляр класса AppTemplate
    at = AppTemplate([('config=', "<config filename>")])
    at.get_arguments_from_env()
    at.get_arguments()
    at.load_settings_from_file(at.parameters['config'])

    rabbitmq_producer = rabbitmq_producer_2.RabbitMQClient(host=at.settings["host"],
                                                           exchange=at.settings["out_exchange"],
                                                           user=at.settings["user"],
                                                           password=at.settings["password"])
    rabbitmq_client = rabbitmq_consumer.RabbitMQClient(user=at.settings["user"],
                                                       password=at.settings["password"],
                                                       host=at.settings["host"],
                                                       exchange_name=at.settings["in_exchange"])
    rabbitmq_client.connect()
    prices = None
    updated = False
    while True:
        if FILE_MODE:
            new_prices = load_data_from_json(SOURCE_FILE)
        else:
            # new_prices = load_data_from_rabbitmq(IN_QUEUE,HOST)
            new_prices = rabbitmq_client.read_latest_from_exchange()
            logger.debug("Latest exchange data: %s", new_prices)
        # если новых данных не поступало, то работаем с тем, что было в прошлый раз
        updated = bool(new_prices)
        if not (new_prices is None):
            prices = new_prices
        if updated:
            filtered_prices = measure_execution_time(filter_prices, prices, at.settings["tolerance"])
            #filtered_pairs = measure_execution_time(filter_pairs, prices, TOLERANCE)
            if FILE_MODE:
                save_data_to_json(filtered_prices, RAW_FILE, OUT_FILE)
            else:
                rabbitmq_producer.send_to_rabbitmq(filtered_prices, fanout=True)
        else:
            pass
            #print("Цены не поменялись")


if __name__ == '__main__':
    main()
