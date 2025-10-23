import ccxt
import json
import logging
from typing import Dict, Any

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Список бирж для обработки
EXCHANGES = [
    'binance', 'bitget', 'bybit', 'gateio','mexc',
    'htx', 'kucoin', 'kraken', 'poloniex','bitget'
]

# Ваши API-ключи и секреты для каждой биржи (при необходимости)
API_KEYS = {
    'binance': {
        'apiKey': 'p6QjdoLPjWGTmSTQekaaUMocFLIPKiNIidkEeh7YMslolON0199d37n8JlqNJyeW',
        'secret': 'xuvszRFMJEaCDoE5DWRXV8FbXnLE3uvKI5L4CJXkzcyuSbiTQwjATXKancyvI5K5'
    },
    'bybit': {
        'apiKey': 'mr4L4DA3iNaoc6R7dv',
        'secret': 'YjFGTumcETsRVlPNzuQk5SZCzqESsz7nxiIm'
    },
    'mexc': {
        'apiKey': 'mx0vglRHG3gvae6Ffj',
        'secret': 'ad5f97b69af04b05afb38bf15db9b1ce'
    },
    'okx': {
        'apiKey': 'be043c33-10f2-41b4-924a-efc09dba2e8e',
        'secret': '3EBDD396CF740FB723A41F9D035EAA47'
    }

}

# Словарь для хранения данных о валютах
exchange_data: Dict[str, Any] = {}


def fetch_currency_info(exchange_name: str) -> None:
    """
    Получает информацию о валютах с указанной биржи и сохраняет в словарь.

    :param exchange_name: Название биржи из списка ccxt.
    """
    try:
        logging.info(f"Инициализация {exchange_name}")
        exchange = getattr(ccxt, exchange_name)(API_KEYS.get(exchange_name, {'enableRateLimit': True}))

        # Загрузка информации о валютах
        currencies = exchange.fetch_currencies()
        exchange_data[exchange_name] = currencies
        logging.info(f"Успешно получены данные с {exchange_name}: {len(currencies)} записей")

    except Exception as e:
        logging.error(f"Ошибка при обработке {exchange_name}: {e}")
        exchange_data[exchange_name] = {'error': str(e)}


def save_to_json(data: Dict[str, Any], filename: str) -> None:
    """
    Сохраняет данные в JSON-файл.

    :param data: Данные для сохранения.
    :param filename: Имя выходного файла.
    """
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"Данные успешно сохранены в {filename}")
    except Exception as e:
        logging.error(f"Ошибка при сохранении данных: {e}")


def main() -> None:
    """
    Основная функция для запуска сбора данных с бирж.
    """
    for exchange_name in EXCHANGES:
        fetch_currency_info(exchange_name)

    # Сохранение собранных данных в JSON
    output_file = "./data/exchange_usdt_data.json"
    save_to_json(exchange_data, output_file)


if __name__ == "__main__":
    main()
