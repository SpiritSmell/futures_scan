import asyncio
import logging
import copy
from dotenv import load_dotenv, find_dotenv, dotenv_values
import requests
from datetime import datetime, timezone

MAX_CONCURRENT_TASKS = 500

MAIN_LOOP_DELAY_TIME = 5

TOKEN = ""
CHAT_ID = 0

# Загрузка конфигурации из .env файла
config = dotenv_values(find_dotenv())
load_dotenv(find_dotenv())

# Динамическая загрузка переменных из конфигурационного файла
for item in config:
    exec(f"{item} = '{config[item]}'")


# Функция для настройки логгера
def setup_logger(logger_name, level=logging.INFO):
    """
    Настраивает логгер с указанным именем и уровнем логирования.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    handler = logging.StreamHandler()
    handler.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger

def find_changed_keys(dict1, dict2):
    """
    Функция находит общие ключи между двумя словарями.

    :param dict1: Первый словарь
    :param dict2: Второй словарь
    :return: Список общих ключей
    """
    # Находим разницу множеств ключей обоих словарей
    new_keys = set(dict1.keys()) - set(dict2.keys())
    # Преобразуем множество в список для удобства
    new_keys_list = list(new_keys)
    if new_keys_list is not None:
        new_keys_list.sort()

    # Находим пересечение множеств ключей обоих словарей
    lost_keys = set(dict2.keys()) - set(dict1.keys())
    # Преобразуем множество в список для удобства
    lost_keys_list = list(lost_keys)
    if lost_keys_list is not None:
        lost_keys_list.sort()

    return new_keys_list, lost_keys_list

def filter_dict_by_keys( data: dict, allowed_keys: list) -> dict:
    """
    Фильтрует словарь, оставляя только те элементы,
    чьи ключи входят в список допустимых ключей.

    :param data: Исходный словарь.
    :param allowed_keys: Список ключей, которые нужно оставить.
    :return: Отфильтрованный словарь.
    """
    return {k: v for k, v in data.items() if k in allowed_keys}


def dict_to_str(dictionary):
    string = ""
    for item in dictionary:
        string += f"{dictionary[item]}\n"
    return string


class TelegramDataDispatcher:
    def __init__(self, token, chat_id, delay=5):
        self.data_lock = asyncio.Lock()
        self.old_data_lock = asyncio.Lock()
        self.old_data = {}
        self.data = {}
        self.delay = delay
        self.token = token
        self.chat_id = chat_id
        self.updated = False
        self.time_tracker = {}
        # Запускаем get_data асинхронно
        loop = asyncio.get_event_loop()
        task = loop.create_task(
            self.dispatch_data())

    def send_message(self,  text) -> None:

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        params = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "html"
        }
        response = requests.get(url, params=params)
        logger.debug(response.text)

    async def get_data(self):
        async with self.data_lock:
            return copy.deepcopy(self.data)

    async def get_old_data(self):
        async with self.old_data_lock:
            return copy.deepcopy(self.old_data)

    async def set_data(self, value):
        async with self.data_lock:
            self.updated = not(self.data == value)
            if self.updated:
                self.old_data = copy.deepcopy(self.data)
                self.data = copy.deepcopy(value)



    async def dispatch_data(self):
        while True:
            # в цикле отправляем данные каждые self.delay секунд
            # можно добавить чтоб данные отправлялись чаще, например, по факту обновления

            await self.send_data()
            # await self.set_data(self.extract_data(new_prices))

            await asyncio.sleep(self.delay)

    async def send_data(self):
        # проверка, что данные поменялись
        async with self.data_lock:
            if not self.updated:
                return
            self.updated = False
        data = await self.get_data()
        old_data = await self.get_old_data()
        new_keys_list,lost_keys_list = find_changed_keys(data,old_data)
        new_entries = filter_dict_by_keys(data,new_keys_list)
        lost_entries = filter_dict_by_keys(old_data, lost_keys_list)

        for new_entry in new_entries:
            self.time_tracker[new_entry] = datetime.now(timezone.utc).timestamp()

        message = ""
        if len(new_entries)>0:
            message += f"New entries:\n{dict_to_str(new_entries)}\n"
        if len(lost_entries)>0:
            message += f"Lost entries:\n{dict_to_str(lost_entries)}\n"

        for lost_entry in lost_entries:
            delta = datetime.now(timezone.utc).timestamp() - self.time_tracker.get(lost_entry)
            message += f"{lost_entry} lasted for {delta/60.0:.3g} minutes\n\n"
            self.time_tracker.pop(lost_entry)

        if (len(new_entries)>0) or (len(lost_entries)>0):
            self.send_message(message)
            logger.info(f'Sent to chat {self.chat_id}, {len(new_entries) + len(lost_entries)} records')
        else:
            logger.info(f'No data to send ')


async def main():
    # Создаем экземпляр класса AppTemplate

    logger.info(f"{TOKEN=}, {CHAT_ID=}")
    dd = TelegramDataDispatcher(token=TOKEN,chat_id=CHAT_ID)

    while True:

        enreached_data = None

        # устанавливаем данные на отправку
        if enreached_data:
            await dd.set_data(enreached_data)

        # Ждем некоторое время перед следующей итерацией
        await asyncio.sleep(MAIN_LOOP_DELAY_TIME)  # Можете установить другое значение времени ожидания


logger = setup_logger(__name__)

if __name__ == '__main__':
    asyncio.run(main())
