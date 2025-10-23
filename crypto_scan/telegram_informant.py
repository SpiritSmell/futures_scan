import asyncio
from packages import processor_template
from packages.app_template import AppTemplate, logger
from packages.telegram_dispatcher import TelegramDataDispatcher, dict_to_str
import json
import datetime
from packages.crypto_charts_data_processing import (
    build_last_bid_ask,
    get_destination_exchanges,
    get_element_by_symbol,
    calculate_profitability
)


MAX_CONCURRENT_TASKS = 500

MAIN_LOOP_DELAY_TIME = 5

NL = "\n"

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





def process_data(data,minimal_profit = 0, minimal_percent = 0, maximal_percent = 100):
    results = {}
    for item in data:
        logger.debug(item)
        if item.get('symbol', None) is None:
            continue
        #readable_time = datetime.datetime.fromtimestamp(item['__timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        summary_df = calculate_profitability(item)
        logger.info(f"Profitability of {item.get('symbol',None)} {summary_df}")
        if len(summary_df)==0:
            continue
        best_route = summary_df.iloc[0]

        # фильтруем
        if best_route['profit_USDT'] < minimal_profit:
            logger.info(f"Too small profit: {best_route['profit_USDT']}")
            continue
        if best_route['profit_percentage'] < minimal_percent:
            logger.info(f"Too small profit percentage: {best_route['profit_percentage']}")
            continue
        if best_route['profit_percentage'] > maximal_percent:
            logger.info(f"Too big profit percentage: {best_route['profit_percentage']}")
            continue

        if best_route['fee'] is None or best_route['fee'] == -1:
            fee = None
        else:
            fee = best_route['fee']

        result = (f"{item['symbol']} {NL}"
                  f"{best_route['source']} -> {best_route['destination']} {NL}"
                  f"${best_route['profit_USDT']:.4g} ({best_route['profit_percentage']:.2f}%) from ${best_route['equilibrium_cost']:.2f} {NL}"
                  f"Buy {best_route['equilibrium_qty']:.2g}. Network {best_route['network']}, fee {fee}{NL}"
                  f"Equilibrium price {best_route['equilibrium_price']:.4g}. Ask price {best_route['ask_price']:.4g}{NL}"

                  )
        results[item['symbol']] = result
    #return json.dumps(result, indent=4)

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
    tdd = TelegramDataDispatcher("7339234236:AAHpa0d9AREeYrwabYIlJ2yOCn-JDXmAbzQ", 399189323)

    while True:
        # читаем входящие данные
        received_data = await dr.get_data()
        # преобразуем данные
        enriched_data = []
        if received_data:
            save_data_to_json(received_data, "data/telegram_informant.json")
            enriched_data = process_data(received_data, at.settings["minimal_profit"],
                                         at.settings["minimal_percent"],
                                         at.settings["maximal_percent"])
            logger.info(f"Обработанные данные {enriched_data}")
        # устанавливаем данные на отправку
        if enriched_data:
            await tdd.set_data(enriched_data)
            #save_data_to_json(received_data, "./data/received_data.json")
        # Ждем некоторое время перед следующей итерацией
        await asyncio.sleep(MAIN_LOOP_DELAY_TIME)  # Можете установить другое значение времени ожидания

def main1():
    received_data = load_data_from_json("./data/telegram_informant.json")
    tdd = TelegramDataDispatcher("7339234236:AAHpa0d9AREeYrwabYIlJ2yOCn-JDXmAbzQ", 399189323)
    # Создаем экземпляр класса AppTemplate
    at = AppTemplate([('config=', "<config filename>")])
    at.get_arguments_from_env()
    at.get_arguments()
    at.load_settings_from_file(at.parameters['config'])

    enriched_data = process_data(received_data)

    if enriched_data:
        tdd.set_data(enriched_data)

    # преобразуем данные
    enriched_data = []
    if received_data:
        enriched_data = process_data(received_data, at.settings["minimal_profit"],
                                     at.settings["minimal_percent"],
                                     at.settings["maximal_percent"])
        logger.info(f"Обработанные данные {enriched_data}")
    # устанавливаем данные на отправку
    if enriched_data:
        tdd.set_data(enriched_data)



if __name__ == '__main__':
    #asyncio.run(main())
    main1()
