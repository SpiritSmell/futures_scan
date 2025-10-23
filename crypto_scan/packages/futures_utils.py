from packages.json_utils import load_data_from_json
import pandas as pd


def calculate_funding_spread(data):
    results = []
    for future_name, future_data in data.items():
        min_funding_rate = 1000000.0
        max_funding_rate = -1000000.0
        min_funding_rate_exchange = None
        max_funding_rate_exchange = None
        for exchange_name, exchange_data in future_data.items():
            funding_rate = exchange_data.get("fundingRate", min_funding_rate)
            if funding_rate<min_funding_rate:
                min_funding_rate = funding_rate
                min_funding_rate_exchange = exchange_name
            if funding_rate>max_funding_rate:
                max_funding_rate = funding_rate
                max_funding_rate_exchange = exchange_name
        funding_rate_spread = max_funding_rate - min_funding_rate

        new_row = {
                    "symbol": future_name,
                    "funding_rate_spread": funding_rate_spread,
                    "min_funding_rate_exchange": min_funding_rate_exchange,
                    "max_funding_rate_exchange": max_funding_rate_exchange
            }
        results.append(new_row)
    df = pd.DataFrame(results)


    return df


def main():
    data = load_data_from_json("./data/futures_price_collector.json")

    spreads = calculate_funding_spread(data.get("futures",{}))

    spreads_sorted = spreads.sort_values(by='funding_rate_spread')

    print(spreads_sorted)

if __name__ == "__main__":
    main()
