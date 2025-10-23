import ccxt.pro as ccxt
import asyncio

async def watch_all_tickers(exchange_id):
    exchange = getattr(ccxt, exchange_id)({'enableRateLimit': True})
    await exchange.load_markets()

    symbols = list(exchange.symbols)[:10]  # Получаем все доступные торговые пары на бирже
    print(len(symbols))
    results = {}
    async def watch_symbol(symbol):
        while True:
            try:
                ticker = await exchange.watch_ticker(symbol)
                results[symbol] = ticker
                print(len(results))
                print(f"{symbol}: {ticker}")
            except Exception as e:
                print(f"Error watching {symbol}: {str(e)}")
                break

    await asyncio.gather(*(watch_symbol(symbol) for symbol in symbols))

async def main():
    await watch_all_tickers('binance')

asyncio.run(main())

