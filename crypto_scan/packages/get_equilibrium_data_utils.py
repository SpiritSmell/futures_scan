import sys

import json


def find_equilibrium(orders: dict) -> tuple:
    try:
        # Извлекаем заявки на покупку (bids) и продажу (asks) из orders
        bids = orders['bids']
        asks = orders['asks']

        # Инициализируем переменные
        ask_i = 0  # Индекс текущей заявки на продажу
        bid_i = 0  # Индекс текущей заявки на покупку
        middle_quantity = 0  # Общее количество на средней цене
        middle_price = -1  # Средняя цена
        ask_cost = 0  # Общая стоимость заявок на продажу
        bid_cost = 0  # Общая стоимость заявок на покупку
        ask_cut = 0  # Остаток от текущей заявки на продажу
        bid_cut = 0  # Остаток от текущей заявки на покупку
        if len(asks) == 0:
            return 0, 0, 0, 0, 0, 0
        if len(bids) == 0:
            return 0, 0, 0, 0, 0, 0

        # Перебираем элементы пока цена в asks меньше цены в bids
        while float(asks[ask_i][0]) < float(bids[bid_i][0]):
            # Вычисляем минимальное количество для покупки/продажи
            ask_value = float(asks[ask_i][1]) - ask_cut
            bid_value = float(bids[bid_i][1]) - bid_cut
            cut = min(ask_value, bid_value)

            # Накопление стоимости заявок
            ask_cost += float(asks[ask_i][0]) * cut
            bid_cost += float(bids[bid_i][0]) * cut
            middle_quantity += cut

            # Обновляем среднюю цену
            middle_price = float(asks[ask_i][0])

            # Корректируем индексы и остатки в зависимости от оставшегося количества
            if ask_value > bid_value:
                bid_i += 1
                bid_cut = 0
                ask_cut += cut
            else:
                ask_i += 1
                ask_cut = 0
                bid_cut += cut

            # Прерываем цикл, если индексы выходят за пределы списка
            if ask_i >= len(asks) or bid_i >= len(bids):
                break

        # Вычисляем прибыль и процент прибыли
        profit = bid_cost - ask_cost
        profit_percentage = ((bid_cost / ask_cost) - 1) if ask_cost != 0 else 0

        # считаем средний курс покупки
        ask_cost_price = 0
        if middle_quantity > 0:
            ask_cost_price = ask_cost / middle_quantity
        # равновесное количество, профит в USDT, профит в процентах, стоимость закупа в USDT, цена закупа,
        # равновесная цена
        return middle_quantity, profit, profit_percentage, ask_cost, ask_cost_price, middle_price

    except KeyError as e:
        # Обработка ошибки, если отсутствует ключ в orders
        print(f"KeyError: Отсутствует ключ в orders - {e}")
    except IndexError as e:
        # Обработка ошибки, если индекс выходит за пределы списка
        print(f"IndexError: Индекс выходит за пределы списка - {e}")
        # Сохранение данных в JSON-файл
        error_data = {
            "asks": asks,
            "bids": bids,
            "ask_i": ask_i,
            "bid_i": bid_i
        }
        with open('./data/error_data.json', 'w') as f:
            json.dump(error_data, f, indent=4)
        print("Сохранены данные об ошибке в error_data.json")
        sys.exit(1)  # Выход из программы с кодом ошибки
    except ValueError as e:
        # Обработка ошибки при недопустимом значении
        print(f"ValueError: Недопустимое значение - {e}")
    except TypeError as e:
        # Обработка ошибки при неправильном типе данных
        print(f"TypeError: Неправильный тип данных - {e}")
    except Exception as e:
        # Обработка любой другой непредвиденной ошибки
        print(f"Unexpected error: {e}")

    # Возвращаем значения по умолчанию в случае ошибки
    return 0, 0, 0, 0, 0, 0


if __name__ == '__main__':
    orders = {}
    find_equilibrium(orders)
