import pandas as pd
from pandas.core.interchange.dataframe_protocol import DataFrame

from packages.json_utils import load_data_from_json, save_data_to_json
from packages.get_equilibrium_data_utils import find_equilibrium


def build_last_bid_ask(tickers):
    data = {}
    for exchange in tickers:
        item = tickers[exchange]
        if not (item["ask"] and item["bid"]):
            continue
        data[exchange] = {}
        data[exchange]["ask"] = tickers[exchange]["ask"]
        data[exchange]["bid"] = tickers[exchange]["bid"]

    df = pd.DataFrame.from_dict(data, orient='index')
    return df


def get_element_by_symbol(items, symbol):

    if not items:
        return None
    if len(items) == 0:
        return None
    if not symbol:
        return items[1]
    for item in items:
        current_symbol = item.get("symbol", None)
        if current_symbol == symbol:
            return item
    return items[1]


def get_destination_exchanges(tickers):
    """
    Обрабатывает данные о котировках и возвращает два DataFrame:
    - df_destination_exchanges: данные с бирж, где bid выше минимального ask.
    - df_source_exchanges: данные с бирж, где ask ниже хотя бы одного bid из df_destination_exchanges.

    :param tickers: словарь с данными о котировках (ключи — биржи, значения — словари с полями "ask" и "bid").
    :return: tuple из двух DataFrame.
    """
    try:
        # Проверка на пустой словарь
        if not tickers:
            print("Пустой словарь tickers.")
        data = {}
        for exchange in tickers:
            try:
                item = tickers[exchange]
                # Проверка наличия ключей "ask" и "bid" и их значений
                if not (item.get("ask") and item.get("bid")):
                    continue
                data[exchange] = {
                    "ask": item["ask"],
                    "bid": item["bid"]
                }
            except KeyError as e:
                raise KeyError(f"Отсутствует ожидаемый ключ в данных для биржи {exchange}: {e}")
            except Exception as e:
                raise ValueError(f"Ошибка обработки данных для биржи {exchange}: {e}")

        # Создание DataFrame из словаря
        df = pd.DataFrame.from_dict(data, orient='index')

        # Проверка, что DataFrame не пустой
        if df.empty:
            print("DataFrame, созданный из данных, оказался пустым.")
            return pd.DataFrame(None), pd.DataFrame(None)

        # Вычисление минимального значения "ask"
        min_ask = df["ask"].min()
        # Выбор бирж, где "bid" больше минимального "ask"
        df_destination_exchanges = df[df["bid"] > min_ask]

        # Получение всех "bid" из df_destination_exchanges
        bids_from_destinations = df_destination_exchanges["bid"].values

        # Фильтрация строк, где "ask" меньше хотя бы одного "bid"
        df_source_exchanges = df[df["ask"].apply(lambda x: any(x < bid for bid in bids_from_destinations))]

        # Удаление ненужных колонок
        df_source_exchanges = df_source_exchanges.drop("bid", axis=1, errors='ignore')
        df_destination_exchanges = df_destination_exchanges.drop("ask", axis=1, errors='ignore')

        return df_destination_exchanges, df_source_exchanges

    except ValueError as ve:
        print(f"Ошибка значения: {ve}")
    except KeyError as ke:
        print(f"Ошибка ключа: {ke}")
    except Exception as e:
        print(f"Общая ошибка: {e}")
        raise

def calculate_profitability(item):
    destinations_exchanges_df, source_exchanges_df = get_destination_exchanges(item.get("tickers",{}))

    # равновесное количество, профит в USDT, профит в процентах, стоимость закупа в USDT, цена закупа,
    # равновесная цена
    result = pd.DataFrame()

    for source_index, source_row in source_exchanges_df.iterrows():
        for destination_index, destination_row in destinations_exchanges_df.iterrows():

            source_asks = item.get("order_books",{}).get(source_index,{}).get("asks",[])
            destination_bids = item.get("order_books",{}).get(destination_index,{}).get("bids",[])
            orders = {
                        "asks":source_asks,
                        "bids":destination_bids
                      }
            equilibrium = find_equilibrium(orders)
            # если нет прибыли в этом варианте
            if equilibrium[5] == -1:
                continue

            feasible_networks = get_networks_info_for_exchanges(item, source_index, destination_index)

            for feasible_network in feasible_networks:
                if feasible_network['fee'] is None or feasible_network['fee'] == -1:
                    fee = None
                else:
                    fee = feasible_network['fee'] * equilibrium[5]

                new_row = pd.DataFrame([
                    {
                        'source' : source_index,
                        'destination': destination_index,
                        'profit_USDT': equilibrium[1],
                        'profit_percentage': equilibrium[2]*100,
                        'equilibrium_cost': equilibrium[3],
                        'equilibrium_qty': equilibrium[0],
                        'ask_price' : equilibrium[4],
                        'equilibrium_price' : equilibrium[5],
                        'network' : feasible_network['network'],
                        'fee': fee
                    }
                ])
                print(new_row)
                result = pd.concat([result, new_row])
            #print(f"{source_index}, {destination_index} {equilibrium}")
    if len(result) >0 :
        result = result.sort_values(by=["profit_USDT", "profit_percentage"], ascending=[False, False])

    return result

def get_networks_info_for_exchanges(item,source_index,destination_index):
    source_networks = item.get("networks", {}).get(source_index, {}).get("networks")
    if source_networks is None or len(source_networks) == 0:
        return []
    destination_networks = item.get("networks", {}).get(destination_index, {}).get("networks")
    if destination_networks is None or len(destination_networks) == 0:
        return []
    #print(source_networks, destination_networks)
    common_networks = list(source_networks.keys() & destination_networks.keys())
    #print(common_networks)

    result = []

    for common_network in common_networks:
        if not source_networks[common_network]['withdraw']:
            continue
        if not destination_networks[common_network]['deposit']:
            continue

        new_row = {
            'source': source_index,
            'destination': destination_index,
            'network': common_network,
            'withdraw': source_networks[common_network]['withdraw'],
            'deposit': destination_networks[common_network]['deposit'],
            'fee': source_networks[common_network]['fee'],
        }
        result.append(new_row)
    return result

def get_networks_info(item):
    destinations_exchanges_df, source_exchanges_df = get_destination_exchanges(item["tickers"])

    # равновесное количество, профит в USDT, профит в процентах, стоимость закупа в USDT, цена закупа,
    # равновесная цена
    result = []

    for source_index, source_row in source_exchanges_df.iterrows():
        for destination_index, destination_row in destinations_exchanges_df.iterrows():

            result = get_networks_info_for_exchanges(item,source_index, destination_index)

    print(result)
    return result


def main():
    data = load_data_from_json("./data/chart_helper.json")
    # Создание интерфейса Panel
    # берем первый элемент
    item = data[0]

    symbols = []
    for record in data:
        symbols.append(record["symbol"])

    last_prices_df = build_last_bid_ask(item["tickers"])
    # рассчитываем биржи источники и биржи - получатели
    destinations_exchanges_df, source_exchanges_df = get_destination_exchanges(item["tickers"])

    symbol_data = get_element_by_symbol(data,'ET/USDT')

    equilibrium = find_equilibrium(item["order_books"]["binance"])

    print(calculate_profitability(item))

    get_networks_info(item)

    pass

if __name__.startswith("__main__"):
    main()
