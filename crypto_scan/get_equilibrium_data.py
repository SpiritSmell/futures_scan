import asyncio
import datetime
import sys

from packages import processor_template
from packages.app_template import AppTemplate, logger
import json
from packages.get_equilibrium_data_utils import find_equilibrium

MAX_CONCURRENT_TASKS = 500

MAIN_LOOP_DELAY_TIME = 5

# количество секунд с момента получения последнего входящего сообщения
# до момента когда исходящие сообщения перестанут отправляться.
#MAX_INCOMING_QUIEUE_DELAY = 60


def load_data_from_json(filename):
    with open(filename, 'r') as f:
        prices = json.load(f)
    return prices


def save_data_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f)


def save_list_to_csv(data, csv_filename):
    import csv

    with open(csv_filename, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Column Name"])  # Запись заголовка, если нужно
        for item in data:
            csv_writer.writerow([item])



def process_data(data):
    for item in data:
        order_books = item["order_books"]

        if "destination_orderbook" in order_books and "source_orderbook" in order_books:
            pairs_to_process = {"bids": order_books["destination_orderbook"]["bids"],
                                "asks": order_books["source_orderbook"]["asks"]}

            equilibrium = find_equilibrium(pairs_to_process)
        else:
            # нет либо source_orderbook либо destinaiton_orderbook
            equilibrium = [0, 0, 0, 0, 0, 0]

        logger.debug(f"Equilibrium price = {equilibrium}")
        item["profitability"] = {}
        item["profitability"]["equilibrium_quantity"] = equilibrium[0]
        item["profitability"]["equilibrium_profit"] = equilibrium[1]
        item["profitability"]["equilibrium_profit_rate"] = equilibrium[2]
        item["profitability"]["equilibrium_ask_cost"] = equilibrium[3]
        item["profitability"]["ask_cost_price"] = equilibrium[4]
        item["profitability"]["middle_price"] = equilibrium[5]
        item["profitability"]['__timestamp'] = int(datetime.datetime.now(datetime.timezone.utc).timestamp())

    return data


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

        enreached_data = []
        if dr.updated:
            enreached_data = process_data(received_data)
            dr.updated = False
            #save_data_to_json(enreached_data, "../data/get_equilibrium_data_out.json")

        # устанавливаем данные на отправку
        if (enreached_data):
            await dd.set_data(enreached_data)

        # если долго не было входящих данных, перестаем отправлять выходные данные
        last_update_time = await dr.get_last_update_time()
        delta_time = datetime.datetime.now() - last_update_time
        logger.debug(f"Delta time {delta_time}")
        """
        if delta_time > datetime.timedelta(seconds=at.settings["max_incoming_quieue_delay"]):
            logger.info(f"Delta time is too big, disable sending data to output exchange")
            await dd.set_data(None)
        """
        # Ждем некоторое время перед следующей итерацией
        await asyncio.sleep(MAIN_LOOP_DELAY_TIME)  # Можете установить другое значение времени ожидания


if __name__ == '__main__':

    asyncio.run(main())
