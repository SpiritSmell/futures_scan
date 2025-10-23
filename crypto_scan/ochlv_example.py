import asyncio
import ccxt.pro as ccxt

# Список бирж и символов для отслеживания
exchanges_to_check = [
    {'exchange': 'binance', 'symbols': ['BTC/USDT', 'ETH/USDT']},
    {'exchange': 'bitfinex', 'symbols': ['BTC/USD', 'ETH/USD']},
    {'exchange': 'kraken', 'symbols': ['BTC/USD', 'ETH/USD']}
]


# Функция для отслеживания order book
async def watch_order_book(exchange_id, symbols):
    try:
        exchange_class = getattr(ccxt, exchange_id)
        exchange_instance = exchange_class()

        # Проверка доступности метода watchOrderBookForSymbols
        if not hasattr(exchange_instance, 'watchOrderBookForSymbols'):
            print(f"Метод watchOrderBookForSymbols недоступен для биржи {exchange_id}")
            return

        # Используем метод watchOrderBookForSymbols для отслеживания
        print(f"Начинаем отслеживание order book для биржи {exchange_id}...")
        order_book_data = await exchange_instance.watchOrderBookForSymbols(symbols)

        # Печать полученных данных
        print(f"Данные order book для {exchange_id}:")
        for symbol, data in order_book_data.items():
            print(f"Символ: {symbol} -> Данные: {data}")
    except Exception as e:
        print(f"Ошибка при отслеживании order book для {exchange_id}: {e}")


# Основная асинхронная функция для запуска всех отслеживаний
async def main():
    tasks = []
    for exchange_info in exchanges_to_check:
        exchange_id = exchange_info['exchange']
        symbols = exchange_info['symbols']
        tasks.append(watch_order_book(exchange_id, symbols))

    await asyncio.gather(*tasks)


# Запуск асинхронного приложения
asyncio.run(main())
