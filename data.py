import requests
import pandas as pd

def get_stock_data(stock_id):

    url = "https://api.finmindtrade.com/api/v4/data"

    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": str(stock_id),
        "start_date": "2023-01-01",
        "token": API_TOKEN
    }

    res = requests.get(url, params=params)
    data = res.json()

    if "data" not in data or len(data["data"]) == 0:
        return pd.DataFrame()

    df = pd.DataFrame(data["data"])

    df = df.rename(columns={
        "close": "close",
        "max": "high",
        "min": "low"
    })

    return df[["close", "high", "low"]].dropna()
