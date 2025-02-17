import requests
import pandas as pd
import time

class BinanceLargeTradesAnalyzer:
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3"

    def get_trades(self, symbol: str, start_time: int, end_time: int) -> list:
        """
        Fetch all trades from Binance API within the specified time range.
        Handles pagination to retrieve more than 1000 trades.
        """
        endpoint = f"{self.base_url}/aggTrades"
        trades = []

        params = {
            "symbol": symbol,
            "startTime": start_time,
            "endTime": end_time,
            "limit": 1000  # Maximum allowed per request
        }

        headers = {
            "X-MBX-TIME-UNIT": "microsecond"
        }


        try:
            request_count = 0
            while True:
                response = requests.get(endpoint, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()

                if not data:
                    break  # No more data available

                trades.extend(data)
                request_count += 1

                # Print progress
                print(f"Request {request_count}: Fetched {len(data)} trades. Last timestamp: {data[-1]['T']}")

                # Update startTime for the next request
                params["startTime"] = data[-1]["T"] + 1

                # Stop if we've reached the end time
                if params["startTime"] >= end_time:
                    break

                # Respect rate limits
                time.sleep(0.15)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching trades: {e}")
            return []

        print(f"\nTotal trades fetched: {len(trades)}")
        return trades

    def process_trades(self, trades: list) -> pd.DataFrame:
        """
        Process raw trades data into required format.
        """
        processed_trades = []

        for trade in trades:
            # Only consider taker trades (isBuyerMaker = False for BUY, True for SELL)
            if not trade["m"]:  # Taker BUY
                side = "BUY"
            else:  # Taker SELL
                side = "SELL"

            processed_trades.append({
                "timestamp_us": trade["T"],  # Already in microseconds
                "amount_btc": float(trade["q"]),
                "amount_usdt": float(trade["q"]) * float(trade["p"]),
                "side": side
            })

        # Print a sample of processed trades
        print("\nSample of processed trades:")
        for trade in processed_trades[:5]:
            print(trade)

        return pd.DataFrame(processed_trades)

    def find_largest_trades(self, df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
        """
        Find n largest trades by USDT amount.
        """

        duplicate_check = df[df.duplicated(subset=["timestamp_us", "side"], keep=False)]
        print("\nTrades with duplicate timestamps before grouping:")
        print(duplicate_check)


        # Group trades that occur in the same microsecond and side
        grouped = df.groupby(['timestamp_us', 'side']).agg({
            'amount_btc': 'sum',
            'amount_usdt': 'sum'
        }).reset_index()

        # Print grouped data
        print("\nGrouped trades:")
        print(grouped.head(10))

        # Sort by USDT amount and get top n
        return grouped.nlargest(n, 'amount_usdt')


def main():
    # Initialize analyzer
    analyzer = BinanceLargeTradesAnalyzer()

    # Define time range (January 20, 2025, 12:00:00 to January 21, 2025, 12:00:00 US time)
    start_time = int(1737392400000 * 1000)  # Start time in microseconds
    end_time = int(1737478800000 * 1000) # End time in microseconds

    # Fetch and process trades
    print("Fetching trades...")
    trades = analyzer.get_trades("BTCUSDT", start_time, end_time)

    print("\nProcessing trades...")
    df = analyzer.process_trades(trades)

    # Find largest trades
    print("\nFinding top 5 trades...")
    largest_trades = analyzer.find_largest_trades(df)

    # Format output
    formatted_trades = largest_trades.apply(
        lambda x: {
            'timestamp_us': int(x['timestamp_us']),
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