import asyncio
import datetime
from packages import processor_template
from packages.app_template import AppTemplate, logger
import json
from packages.json_utils import save_data_to_json, load_data_from_json

MAX_CONCURRENT_TASKS = 500

MAIN_LOOP_DELAY_TIME = 5

# количество секунд с момента получения последнего входящего сообщения
# до момента когда исходящие сообщения перестанут отправляться.
MAX_INCOMING_QUEUE_DELAY = 30






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
    dd = processor_template.DataDispatcher(at.settings["user"], at.settings["password"], at.settings["host"],
                                           at.settings["out_exchange"])

    # data_processor = app_template.DataProcessor()
    while True:

        # читаем входящие данные
        received_data = await dr.get_data()

        # преобразуем данные

        enriched_data = []
        if dr.updated:
            enriched_data = process_data(received_data, at.settings["filters"])
            dr.updated = False
            save_data_to_json(enriched_data, "./data/apply_profitability_filters.json")

        # устанавливаем данные на отправку
        if enriched_data:
            await dd.set_data(enriched_data)

        # если долго не было входящих данных, перестаем отправлять выходные данные
        last_update_time = await dr.get_last_update_time()
        delta_time = datetime.datetime.now() - last_update_time
        logger.debug(f"Delta time {delta_time}")
        #if delta_time > datetime.timedelta(seconds=at.settings["max_incoming_queue_delay"]):
        #    logger.info(f"Delta time is too big, disable sending data to output exchange")
        #    await dd.set_data(None)

        # Ждем некоторое время перед следующей итерацией
        await asyncio.sleep(MAIN_LOOP_DELAY_TIME)  # Можете установить другое значение времени ожидания


if __name__ == '__main__':
    asyncio.run(main())
