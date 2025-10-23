import asyncio
import time
from datetime import datetime, timezone
import ccxt.async_support as ccxt
import logging
import signal
from packages import rabbitmq_producer
from packages.app_template import AppTemplate, logger
from packages import rabbitmq_producer_2

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

# Создаем список бирж
#exchanges = "exchanges" : ["binance", "bitget", "bybit", "gateio", "htx",  "bingx", "phemex", "mexc", "hyperliquid"]

# Создаем пустой словарь для хранения цен
prices = {"data": {}}
exchanges_data = {}
exchanges_data_futures = {}
exchanges_old_data = {}
lock = asyncio.Lock()  # Создаем экземпляр блокировки


# Определяем асинхронную функцию для получения данных о тикерах с биржи
async def fetch_tickers(exchange_id):
    # Создаем объект биржи по ее идентификатору
    exchange = getattr(ccxt, exchange_id)({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
    # Пытаемся получить данные о всех тикерах с биржи

    try:
        tickers = await exchange.fetch_tickers()
        # Перебираем каждый тикер и получаем цены на покупку (bid) и продажу (ask)
        async with lock:  # Захватываем блокировку перед доступом к общим данным
            exchanges_data[exchange_id] = tickers
        logger.info(f'Собрана информация с биржи {exchange_id}')
    # Обрабатываем конкретные исключения
    except ccxt.RequestTimeout as e:
        logger.error(f'Истекло время запроса для биржи {exchange_id}: {e}')
    except ccxt.NetworkError as e:
        logger.error(f'Сетевая ошибка для биржи {exchange_id}: {e}')
    except Exception as e:
        logger.error(f'Ошибка для биржи {exchange_id}: {e}')
        # Возможно, выполнить повторный запрос или пропустить его
    # Возвращаем объект биржи
    return exchange

async def fetch_funding_rates(exchange_id):
    # Создаем объект биржи по ее идентификатору
    exchange = getattr(ccxt, exchange_id)({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
    # Пытаемся получить данные о всех тикерах с биржи

    symbols_list = list(exchanges_data.get(exchange_id,{}).keys())
    try:
        tickers = await exchange.fetch_funding_rates(symbols_list)
        # Перебираем каждый тикер и получаем цены на покупку (bid) и продажу (ask)
        async with lock:  # Захватываем блокировку перед доступом к общим данным
            exchanges_data_futures[exchange_id] = tickers
        logger.info(f'Собрана информация с биржи {exchange_id}')
    # Обрабатываем конкретные исключения
    except ccxt.RequestTimeout as e:
        logger.error(f'Истекло время запроса для биржи {exchange_id}: {e}')
    except ccxt.NetworkError as e:
        logger.error(f'Сетевая ошибка для биржи {exchange_id}: {e}')
    except Exception as e:
        logger.error(f'Ошибка для биржи {exchange_id}: {e}')
        # Возможно, выполнить повторный запрос или пропустить его
    # Возвращаем объект биржи
    return exchange

def signal_handler(signum, frame):
    logger.info('Получен сигнал прерывания. Грациозно завершаем скрипт.')
    loop = asyncio.get_event_loop()
    loop.stop()

def reformat_exchanges_data_futures(exchanges_data_futures):
    result = {}
    for exchange_id in exchanges_data_futures:
        exchange_data = exchanges_data_futures[exchange_id]
        for futures_id in exchange_data:
            futures = exchange_data[futures_id]
            if not (futures_id in result):
                result[futures_id] = {}
            result[futures_id][exchange_id] = {
                    'fundingRate' : futures['fundingRate'],
            }

    return result
# Определяем асинхронную функцию для выполнения всех задач параллельно
async def main():
    logger.info('Price collector запущен.')
    # Создаем экземпляр класса AppTemplate
    at = AppTemplate([('config=', "<config filename>")])
    at.get_arguments_from_env()
    at.get_arguments()
    at.load_settings_from_file(at.parameters['config'])

    # Регистрируем обработчик сигналов для плавного завершения
    signal.signal(signal.SIGINT, signal_handler)

    client = rabbitmq_producer_2.RabbitMQClient(host=at.settings["host"],
                                                exchange=at.settings["out_exchange"],
                                                user=at.settings["user"],
                                                password=at.settings["password"])
    # Используем ключевое слово global для доступа к глобальной переменной exchanges
    #global exchanges

    exchanges = at.settings["exchanges"]

    while True:
        # Создаем список задач
        tasks = []
        # Перебираем каждую биржу и создаем задачу
        for exchange_id in exchanges:
            task = asyncio.create_task(fetch_tickers(exchange_id))
            tasks.append(task)
        # Ожидаем завершения всех задач и получаем объекты бирж
        exchanges_results = await asyncio.gather(*tasks)

        for exchange_id in exchanges:
            task = asyncio.create_task(fetch_funding_rates(exchange_id))
            tasks.append(task)
        # Ожидаем завершения всех задач и получаем объекты бирж
        exchanges_results = await asyncio.gather(*tasks)
        # Обрабатываем полученные данные

        futures = reformat_exchanges_data_futures(exchanges_data_futures)

        max_funding_rate_range = 0
        max_funding_rate_exchange = ''
        min_funding_rate_exchange = ''
        max_funding_rate_range_symbol =''

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
            if abs(max_funding_rate-min_funding_rate) > max_funding_rate_range:
                max_funding_rate_range = abs(max_funding_rate-min_funding_rate)
                max_funding_rate_range_symbol = future_id
                max_funding_rate_exchange = current_max_funding_rate_exchange
                min_funding_rate_exchange = current_min_funding_rate_exchange

        print(f"Max funding rate is {max_funding_rate_range}, symbol {max_funding_rate_range_symbol}, "
              f"min  {min_funding_rate_exchange} "
              f"max  {max_funding_rate_exchange} ")

        prices['__timestamp'] = int(datetime.now(timezone.utc).timestamp())
        # Отправляем данные в RabbitMQ
        try:
            client.send_to_rabbitmq(prices, fanout=True)
        except Exception as e:
            logger.error(f'Ошибка при отправке данных в RabbitMQ: {e}')

        # Закрываем все объекты бирж из списка exchanges
        for exchange in exchanges_results:
            # Используем точечную нотацию для доступа к атрибуту close объекта биржи
            await exchange.close()

        logger.info(f'Спим {at.settings["polls_delay"]} секунд.')
        time.sleep(at.settings["polls_delay"])


# Обрабатываем плавное завершение при прерывании пользователем выполнения скрипта


if __name__ == '__main__':
    asyncio.run(main())
