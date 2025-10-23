import asyncio
import datetime
from packages import processor_template
from packages.app_template import AppTemplate, logger
import json
import json
import datetime
import os

MAX_CONCURRENT_TASKS = 500

MAIN_LOOP_DELAY_TIME = 5

# количество секунд с момента получения последнего входящего сообщения
# до момента когда исходящие сообщения перестанут отправляться.
MAX_INCOMING_QUEUE_DELAY = 30


def save_error_data(currency, networks):
    # Создаем уникальное имя файла на основе временной метки
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"inactive_filter_{timestamp}.json"
    filepath = os.path.join("error_logs", filename)

    # Убедимся, что папка для логов существует
    os.makedirs("./data/error_logs", exist_ok=True)

    # Сохраняем данные в JSON формате
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump({"currency": currency, "networks": networks}, f, ensure_ascii=False, indent=4)
    logger.info(f"Данные с ошибкой сохранены в файл: {filepath}")


def process_data(data, filters=None):
    results = []
    try:
        for currency in data:
            try:
                # Получаем символ и разбиваем его по слэшу, чтобы взять только первую часть
                symbol = currency["analythics"]["symbol"].split("/")[0]

                # Проверяем, что символ есть в фильтрах
                if symbol not in filters:
                    logger.warning(f"Символ {symbol} не найден в фильтрах.")
                    continue

                networks = filters[symbol]

                # Проверяем, что ключи 'source' и 'destination' существуют
                source = currency["analythics"]["source"]
                destination = currency["analythics"]["destination"]

                if source not in networks or destination not in networks:
                    logger.warning(
                        f"Источник {source} или пункт назначения {destination} не найдены в сетях для символа {symbol}.")
                    continue

                source_exchange = networks[source]
                destination_exchange = networks[destination]

                # Проверяем статус и возможности вывода и депозита
                if source_exchange.get("active") and source_exchange.get("withdraw"):
                    if destination_exchange.get("active") and destination_exchange.get("deposit"):
                        results.append(currency)

            except KeyError as e:
                logger.error(f"Ошибка доступа к ключу: {e}. Пропускаем элемент {currency}")
                save_error_data(currency, networks)  # Сохраняем ошибочные данные
            except AttributeError as e:
                logger.error(f"Ошибка доступа к атрибуту: {e}. Пропускаем элемент {currency}")
                save_error_data(currency, networks)  # Сохраняем ошибочные данные

    except Exception as e:
        logger.critical(f"Критическая ошибка в процессе обработки данных: {e}")

    logger.debug("Исключаем заблокированные сети/валюты")
    return results


async def main():
    # Создаем экземпляр класса AppTemplate
    at = AppTemplate([('config=', "<config filename>")])
    at.get_arguments_from_env()
    at.get_arguments()
    at.load_settings_from_file(at.parameters['config'])

    # инициализируем и запускаем цикл получения данных
    dr = processor_template.DataReceiver(at.settings["user"], at.settings["password"], at.settings["host"],
                                         at.settings["in_exchange"])
    dr_currencies_networks = processor_template.DataReceiver(at.settings["user"], at.settings["password"], at.settings["host"],
                                         at.settings["in_currencies_networks"])

    dd = processor_template.DataDispatcher(at.settings["user"], at.settings["password"], at.settings["host"],
                                           at.settings["out_exchange"])

    received_data = None
    received_currencies_networks = None

    while True:
        updated = False
        # читаем входящие данные
        received_data_buffer = await dr.get_data()
        if not (received_data_buffer is None):
            if len(received_data_buffer) > 0:
                updated = True
                received_data = received_data_buffer

        # читаем входящие данные
        received_currencies_networks_buffer = await dr_currencies_networks.get_data()
        if not (received_currencies_networks_buffer is None):
            if len(received_currencies_networks_buffer) > 0:
                updated = True
                received_currencies_networks = received_currencies_networks_buffer

        # преобразуем данные
        enriched_data = None
        if updated:
            if (not(received_currencies_networks is None)) and (not (received_data is None)):
                enriched_data = process_data(received_data,received_currencies_networks)

        # устанавливаем данные на отправку
        if enriched_data:
            await dd.set_data(enriched_data)

        # Ждем некоторое время перед следующей итерацией
        await asyncio.sleep(MAIN_LOOP_DELAY_TIME)  # Можете установить другое значение времени ожидания


if __name__ == '__main__':
    asyncio.run(main())
