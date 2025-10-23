import asyncio
import datetime
from packages import processor_template
from packages.app_template import AppTemplate, logger
from packages.clickhouse_dispatcher import ClickhouseDispatcher
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



def apply_filter(data, filter):
    result = True
    filter_type = filter["type"]
    filter_value = 0.0
    data_value = 0.0
    if "minimal_profit" == filter_type:
        filter_value = float(filter["value"])
        data_value = float(data["equilibrium_profit"])
        result = (data_value >= filter_value)

    if "minimal_percent" == filter_type:
        filter_value = float(filter["value"])
        data_value = float(data["equilibrium_profit_rate"]) * 100
        result = (data_value >= filter_value)

    if "maximal_percent" == filter_type:
        filter_value = float(filter["value"])
        data_value = float(data["equilibrium_profit_rate"]) * 100
        result = (data_value < filter_value)

    if result:
        logger.debug(f"Filter {filter_type} requirements {data_value} >= {filter_value} are satisfied ")
    else:
        logger.debug(f"Filter {filter_type} requirements {data_value} >= {filter_value} are NOT satisfied ")

    return result


def process_data(data, filters):
    result = []
    if not filters:
        return data
    for item in data:
        profitability_data = item["profitability"]
        comply = True
        for filter in filters:
            comply = comply and apply_filter(profitability_data, filter)
        # add only records that comply with filters requirements
        if comply:
            result.append(item)

    return result


async def main():
    # Создаем экземпляр класса AppTemplate
    at = AppTemplate([('config=', "<config filename>")])
    at.get_arguments_from_env()
    at.get_arguments()
    at.load_settings_from_file(at.parameters['config'])

    # инициализируем и запускаем цикл получения данных
    dr = processor_template.DataReceiver(at.settings["user"], at.settings["password"], at.settings["host"],
                                         at.settings["in_exchange"])
    CONNECTION_STRING = (f'http://{at.settings["clickhouse_user"]}:{at.settings["clickhouse_password"]}@'
                         f'{at.settings["clickhouse_host"]}:{at.settings["clickhouse_port"]}/')
    dd = ClickhouseDispatcher(connection_string=CONNECTION_STRING)

    while True:

        # читаем входящие данные
        received_data = await dr.get_data()

        # преобразуем данные

        enriched_data = []
        if received_data:
            enriched_data = process_data(received_data, at.settings["filters"])
            # save_data_to_json(enreached_data, "../data/telegram_informant.json")

        # устанавливаем данные на отправку
        if enriched_data:
            await dd.set_data(enriched_data)

        # если долго не было входящих данных, перестаем отправлять выходные данные
        last_update_time = await dr.get_last_update_time()
        delta_time = datetime.datetime.now() - last_update_time
        logger.debug(f"Delta time {delta_time}")

        if delta_time > datetime.timedelta(seconds=MAX_INCOMING_QUEUE_DELAY):
            logger.info(f"Delta time is too big, disable sending data to output exchange")
            await dd.set_data(None)

        # Ждем некоторое время перед следующей итерацией
        await asyncio.sleep(MAIN_LOOP_DELAY_TIME)  # Можете установить другое значение времени ожидания


if __name__ == '__main__':
    asyncio.run(main())
