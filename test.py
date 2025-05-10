import pandas as pd
from openalgo import api
import os

OPEN_ALGO_API_KEY = os.getenv('OPEN_ALGO_API_KEY')

# Config
API_KEY = OPEN_ALGO_API_KEY
HOST = 'http://127.0.0.1:5000'
SYMBOL = 'RELIANCE'
EXCHANGE = 'NSE'
START_DATE = '2024-01-01'
END_DATE = '2025-04-30'
INTERVALS = ['1m', '5m', '10m', '15m']
OUTPUT_FILE = 'volume_changes.xlsx'
PERCENTAGE_THRESHOLD = 0.03

# Initialize client
client = api(api_key=API_KEY, host=HOST)

def fetch_data(symbol, exchange, interval, start_date, end_date):
    return client.history(
        symbol=symbol,
        exchange=exchange,
        interval=interval,
        start_date=start_date,
        end_date=end_date
    )

def get_relative_volume_changes(data):
    if isinstance(data, dict) and "candles" in data:
        candles = data["candles"]
    elif isinstance(data, pd.DataFrame):
        dates = data.index.tolist()
        volumes = data["volume"].tolist()
        closes = df["close"].tolist()
        opens = df["open"].tolist()
    elif isinstance(data, list):
        candles = data
    else:
        raise ValueError("Unsupported format for data")

    def sign(x):
        return (x > 0) - (x < 0)
    
    result = {}

    if 'candles' in locals():
        for i in range(1, len(candles)):
            try:
                date1 = str(candles[i - 1][0])[:19]
                date2 = str(candles[i][0])[:19]
                vol1 = candles[i - 1][5]
                vol2 = candles[i][5]
                diff = vol2 - vol1
                pct = round(diff / vol1, 4) if vol1 else 0
                result[f"{date2} - {date1}"] = {
                    'vol1': vol1,
                    'vol2': vol2,
                    'diff': diff,
                    'percentage': pct
                }
            except Exception:
                continue
    else:
        for i in range(1, len(volumes)):
            try:
                date1 = str(dates[i - 1])[:19]
                date2 = str(dates[i])[:19]
                vol1 = volumes[i - 1]
                vol2 = volumes[i]
                diff = vol2 - vol1
                pct = round(diff / vol1, 4) if vol1 else 0
                d1= closes[i-1]-opens[i - 1]
                d2= closes[i]-opens[i]
                rg=sign(d1)*sign(d2)
                result[f"{date2} - {date1}"] = {
                    'vol1': vol1,
                    'vol2': vol2,
                    'diff': diff,
                    'percentage': pct
                }
            except Exception:
                continue

    return result

def filter_changes(changes, threshold=0.03):
    return {k: v for k, v in changes.items() if abs(v['percentage']) <= threshold and  v for k, v in changes.items() if v['rg'] > 0}

# Write to Excel with multiple sheets
with pd.ExcelWriter(OUTPUT_FILE) as writer:
    for interval in INTERVALS:
        raw_data = fetch_data(SYMBOL, EXCHANGE, interval, START_DATE, END_DATE)
        changes = get_relative_volume_changes(raw_data)
        filtered = filter_changes(changes, PERCENTAGE_THRESHOLD)

        df = pd.DataFrame.from_dict(filtered, orient='index').reset_index()
        df.rename(columns={'index': 'date_range'}, inplace=True)
        df.to_excel(writer, sheet_name=interval, index=False)
