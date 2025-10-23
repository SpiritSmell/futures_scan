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
#exchanges = ['binance', 'bitget', 'bybit', 'gateio', 'htx', 'kucoin', 'kraken', 'poloniex']

# Создаем пустой словарь для хранения цен
prices = {"data": {}}
exchanges_data = {}
exchanges_old_data = {}
lock = asyncio.Lock()  # Создаем экземпляр блокировки


# Определяем асинхронную функцию для получения данных о тикерах с биржи
async def fetch_tickers(exchange_id):
    # Создаем объект биржи по ее идентификатору
    exchange = getattr(ccxt, exchange_id)({'enableRateLimit': True})
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


def signal_handler(signum, frame):
    logger.info('Получен сигнал прерывания. Грациозно завершаем скрипт.')
    loop = asyncio.get_event_loop()
    loop.stop()


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
        # Обрабатываем полученные данные
        for exchange_id in exchanges_data:
            tickers = exchanges_data[exchange_id]
            for symbol, ticker in tickers.items():
                bid_price = ticker['bid']
                ask_price = ticker['ask']
                # Проверяем, есть ли символ уже в словаре цен
                if symbol not in prices["data"]:
                    # Если нет, создаем новый вложенный словарь для символа
                    prices["data"][symbol] = {}
                # Сохраняем цены во вложенном словаре по идентификатору биржи
                prices["data"][symbol][exchange_id] = {'bid': bid_price, 'ask': ask_price}

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
