import json
import tkinter as tk
from tkinter import ttk
import packages.rabbitmq_consumer as rabbitmq_consumer

HOST ='192.168.56.107'
USER = 'rmuser'
PASSWORD = 'rmpassword'


IN_FILE = 'prices_price_filter_out.json'
OUT_QUEUE = "price_filter_view"
IN_QUEUE = 'price_filter_out'

IN_EXCHANGE = "price_filter_out"

UPDATE_TIME = 1000

sort_column = 'spread'
sort_order = True
data = []



# Определение функции для обновления таблицы
def update_table():
    global data
    global rabbitmq_client
    # Открытие файла и загрузка данных из него
    #with open(IN_FILE, "r") as f:
    #    new_data = json.load(f)

    # new_data = rabbitmq_consumer.read_latest_from_queue(IN_QUEUE,host=HOST)
    new_data = rabbitmq_client.read_latest_from_exchange()

    if new_data is None:
        new_data = {}

    # Очистка таблицы
    table.delete(*table.get_children())

    # Добавление новых данных в таблицу
    for item in new_data:
        symbol = item["analytics"]["symbol"]
        source = item["analytics"]["source"]
        max_bid = item["analytics"]["max_bid"]
        destination = item["analytics"]["destination"]
        min_ask = item["analytics"]["min_ask"]
        spread = item["analytics"]["spread"]
        # look for this record in  old data
        time = 0
        for old_item in data:
            # if such element was in previous iterations,
            if symbol == old_item["analytics"]["symbol"]:
                # if 'time' key was in previous iterations
                if 'time' in old_item["analytics"]:
                    time = old_item["analytics"]["time"]
                    time+=1
                break

        item["analytics"]["time"] = time

        table.insert("", "end", values=(symbol, source, max_bid, destination, min_ask, spread, time))
        treeview_sort_column(table, sort_column, sort_order)

    data = new_data

    # table.bind("<Button-1>", on_table_row_click)
    # Перезагрузка данных через 1 секунду
    root.after(UPDATE_TIME, update_table)

# Функции для сортировки столбцов
def sort_symbol():
    data = json.load(open(IN_FILE, "r"))
    data.sort(key=lambda x: x["analytics"]["symbol"])
    with open(IN_FILE, "w") as f:
        json.dump(data, f)
    update_table()

def sort_source():
    data = json.load(open(IN_FILE, "r"))
    data.sort(key=lambda x: x["analytics"]["source"])
    with open(IN_FILE, "w") as f:
        json.dump(data, f)
    update_table()

def sort_max_bid():
    data = json.load(open(IN_FILE, "r"))
    data.sort(key=lambda x: x["analytics"]["max_bid"])
    with open(IN_FILE, "w") as f:
        json.dump(data, f)
    update_table()

def sort_destination():
    data = json.load(open(IN_FILE, "r"))
    data.sort(key=lambda x: x["analytics"]["destination"])
    with open(IN_FILE, "w") as f:
        json.dump(data, f)
    update_table()

def sort_min_ask():
    data = json.load(open(IN_FILE, "r"))
    data.sort(key=lambda x: x["analytics"]["min_ask"])
    with open(IN_FILE, "w") as f:
        json.dump(data, f)
    update_table()

def sort_spread():
    data = json.load(open(IN_FILE, "r"))
    data.sort(key=lambda x: x["analytics"]["spread"])
    with open(IN_FILE, "w") as f:
        json.dump(data, f)
    update_table()

# Функция-обработчик нажатия на строку таблицы
def on_table_row_click(event):
    table = event.widget
    selected_items = table.selection()
    if selected_items:  # Проверка, что выбрана хотя бы одна строка
        selected_item = selected_items[0]  # Получение первого элемента из выбранных строк
        # Получение данных из строки
        values = table.item(selected_item)['values']
        # Вывод данных на экран
        print("Selected Row Data:", values)


# Функции сортировки
def treeview_sort_column(tv, col, reverse):
    global sort_column
    sort_column = col
    global sort_order
    sort_order= reverse
    l = [(tv.set(k, col), k) for k in tv.get_children('')]
    l.sort(reverse=reverse)

    for index, (val, k) in enumerate(l):
        tv.move(k, '', index)

    tv.heading(col, command=lambda: treeview_sort_column(tv, col, not reverse))

if __name__ == '__main__':

    # init RabbitMQ consumer
    rabbitmq_client = rabbitmq_consumer.RabbitMQClient(USER, PASSWORD, HOST, IN_EXCHANGE)
    rabbitmq_client.connect()

    # Создание окна
    root = tk.Tk()
    root.title("Таблица котировок")

    # Создание фрейма, в котором будет расположена таблица
    frame = tk.Frame(root)
    frame.pack(fill=tk.BOTH, expand=True)

    # Создание таблицы
    table = ttk.Treeview(frame, columns=("symbol", "source", "max_bid", "destination", "min_ask", "spread", "time"), show="headings")

    # Задание названий колонок
    table.heading("symbol", text="Symbol")
    table.heading("source", text="Source")
    table.heading("max_bid", text="Max Bid")
    table.heading("destination", text="Destination")
    table.heading("min_ask", text="Min Ask")
    table.heading("spread", text="Spread")
    table.heading("time", text="Time")

    # Задание ширины колонок
    table.column("symbol", width=100)
    table.column("source", width=100)
    table.column("max_bid", width=100)
    table.column("destination", width=100)
    table.column("min_ask", width=100)
    table.column("spread", width=100)
    table.column("time", width=100)

    # Добавление данных в таблицу
    with open(IN_FILE, "r") as f:
        data = json.load(f)
        for item in data:
            symbol = item["analytics"]["symbol"]
            source = item["analytics"]["source"]
            max_bid = item["analytics"]["max_bid"]
            destination = item["analytics"]["destination"]
            min_ask = item["analytics"]["min_ask"]
            spread = item["analytics"]["spread"]
            time = "time"
            table.insert("", "end", values=(symbol, source, max_bid, destination, min_ask, spread))

    # Размещение таблицы внутри фрейма
    table.pack(fill=tk.BOTH, expand=True)

    # Привязка обработчика на событие нажатия на строку таблицы
    # table.bind("<Button-1>", on_table_row_click)
    table.bind("<<TreeviewSelect>>", on_table_row_click)

    # Задание команд для сортировки по столбцам
    table.heading("symbol", text="Symbol", command=lambda: treeview_sort_column(table, "symbol", False))
    table.heading("source", text="Source", command=lambda: treeview_sort_column(table, "source", False))
    table.heading("max_bid", text="Max Bid", command=lambda: treeview_sort_column(table, "max_bid", False))
    table.heading("destination", text="Destination", command=lambda: treeview_sort_column(table, "destination", False))
    table.heading("min_ask", text="Min Ask", command=lambda: treeview_sort_column(table, "min_ask", False))
    table.heading("spread", text="Spread", command=lambda: treeview_sort_column(table, "spread", False))

    # Запуск цикла обновления таблицы
    root.after(1000, update_table)

    # Запуск главного цикла
    root.mainloop()


