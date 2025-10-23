import ccxt as ccxt_sync
import ssl
print(ssl.get_default_verify_paths())

exchange_id = 'htx'
market_type = 'swap'

exchange_sync = getattr(ccxt_sync, exchange_id)()



all_markets = exchange_sync.fetch_markets()
exchange_sync.fetch_markets()