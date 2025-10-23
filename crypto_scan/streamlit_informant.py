import asyncio
import datetime
from packages import processor_template
from packages.app_template import AppTemplate, logger
from packages.telegram_dispatcher import TelegramDataDispatcher
import json

MAX_CONCURRENT_TASKS = 500

MAIN_LOOP_DELAY_TIME = 5

# количество секунд с момента получения последнего входящего сообщения
# до момента когда исходящие сообщения перестанут отправляться.
MAX_INCOMING_QUEUE_DELAY = 120


def load_data_from_json(filename):
    with open(filename, 'r') as f:
        prices = json.load(f)
    return prices


def save_data_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f)





def process_data(data):
    result = []
    for item in data:
        print(item)
        analythics_data = item["analythics"]
        result.append(analythics_data)
    return json.dumps(result, indent=4)


async def main():
    # Создаем экземпляр класса AppTemplate
    at = AppTemplate([('config=', "<config filename>")])
    at.get_arguments_from_env()
    at.get_arguments()
    at.load_settings_from_file(at.parameters['config'])

    # инициализируем и запускаем цикл получения данных
    dr = processor_template.DataReceiver(at.settings["user"], at.settings["password"], at.settings["host"],
                                         at.settings["in_exchange"])
    tdd = TelegramDataDispatcher("7339234236:AAHpa0d9AREeYrwabYIlJ2yOCn-JDXmAbzQ", 399189323)

    while True:
        # читаем входящие данные
        received_data = await dr.get_data()
        # преобразуем данные
        enriched_data = []
        if received_data:
            #enriched_data = process_data(received_data, at.settings["filters"])
            enriched_data = process_data(received_data)
            # save_data_to_json(enreached_data, "../data/telegram_informant.json")

        # устанавливаем данные на отправку
        if enriched_data:
            await tdd.set_data(enriched_data)
            save_data_to_json(received_data, "./data/received_data.json")
        # Ждем некоторое время перед следующей итерацией
        await asyncio.sleep(MAIN_LOOP_DELAY_TIME)  # Можете установить другое значение времени ожидания


if __name__ == '__main__':
    asyncio.run(main())
