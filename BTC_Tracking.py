import requests
import pandas as pd
import time


class BinanceLargeTradesAnalyzer:
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3"
        self.headers = {
            "X-MBX-TIME-UNIT": "microsecond"  # Ensure microsecond precision
        }

    def get_trades(self, symbol: str, start_time: int, end_time: int) -> list:
        """
        Fetch all trades from Binance API within the specified time range.
        Uses trade IDs for pagination and ensures microsecond precision.
        """
        endpoint = f"{self.base_url}/aggTrades"
        trades = []

        # Initial request with startTime
        params = {
            "symbol": symbol,
            "startTime": start_time,
            "limit": 1000
        }

        try:
            request_count = 0
            while True:
                response = requests.get(endpoint, params=params, headers=self.headers)
                response.raise_for_status()
                data = response.json()

                if not data:
                    break

                trades.extend(data)
                request_count += 1

                # Print progress with microsecond timestamp
                print(f"Request {request_count}: Fetched {len(data)} trades. Last timestamp (μs): {data[-1]['T']}")

                # Stop if we've reached the end time
                if data[-1]["T"] >= end_time:
                    break

                # Use fromId parameter for the next request
                params = {
                    "symbol": symbol,
                    "fromId": data[-1]["a"] + 1,
                    "limit": 1000
                }

                # Respect rate limits
                time.sleep(0.15)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching trades: {e}")
            return []

        # Filter trades within our time range (using microsecond timestamps)
        trades = [trade for trade in trades if start_time <= trade["T"] <= end_time]
        print(f"\nTotal trades fetched and filtered: {len(trades)}")
        return trades

    def process_trades(self, trades: list) -> pd.DataFrame:
        """
        Process raw trades data into required format.
        Timestamps are in microseconds.
        """
        processed_trades = []

        for trade in trades:
            side = "SELL" if trade["m"] else "BUY"

            processed_trades.append({
                "T": trade["T"],  # Timestamp in microseconds
                "amount_btc": float(trade["q"]),
                "amount_usdt": float(trade["q"]) * float(trade["p"]),
                "side": side
            })

        df = pd.DataFrame(processed_trades)
        print("\nSample of processed trades with microsecond timestamps:")
        print(df.head())
        return df

    def find_largest_trades(self, df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
        """
        Find n largest trades by USDT amount.
        Groups trades by microsecond timestamp and side.
        """
        # Group trades that occur in the same microsecond and side
        grouped = df.groupby(['T', 'side']).agg({
            'amount_btc': 'sum',
            'amount_usdt': 'sum'
        }).reset_index()

        # Print grouping info
        print("\nTrade grouping summary:")
        print(f"Original trades: {len(df)}")
        print(f"After grouping by microsecond and side: {len(grouped)}")

        # Sort by USDT amount and get top n
        return grouped.nlargest(n, 'amount_usdt')


def main():
    # Initialize analyzer
    analyzer = BinanceLargeTradesAnalyzer()

    # Define time range (January 20, 2025, 12:00:00 to January 21, 2025, 12:00:00 US time)
    start_time = int(1737392400000 * 1000)  # Convert milliseconds to microseconds
    end_time = int(1737478800000 * 1000)  # Convert milliseconds to microseconds

    print(f"Fetching trades from {start_time} to {end_time} (microseconds)")
    trades = analyzer.get_trades("BTCUSDT", start_time, end_time)

    print("\nProcessing trades...")
    df = analyzer.process_trades(trades)

    print("\nFinding top 5 trades...")
    largest_trades = analyzer.find_largest_trades(df)

    # Format output
    formatted_trades = largest_trades.apply(
        lambda x: {
            'timestamp_us': int(x['T']),
            'amount_btc': round(x['amount_btc'], 3),
            'amount_usdt': round(x['amount_usdt'], 3),
            'side': x['side']
        },
        axis=1
    ).tolist()

    # Print results in required format
    print("\nTop 5 BTC/USDT Taker Trades:")
    print("Timestamp (μs)    Amount (BTC)    Amount (USDT)    Side")
    print("-" * 60)
    for trade in formatted_trades:
        print(f"{trade['timestamp_us']:15d} {trade['amount_btc']:13.3f} "
              f"{trade['amount_usdt']:15.3f} {trade['side']:4}")


if __name__ == "__main__":
    main()