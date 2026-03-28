import requests
def get_stock_data(stock_id):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}.TW"

    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers)

    data = res.json()

    result = data.get('chart', {}).get('result')

    if not result:
        return pd.DataFrame()   # ⭐ 回傳空，避免爆炸

    quote = result[0]['indicators']['quote'][0]

    df = pd.DataFrame({
        "close": quote.get('close'),
        "high": quote.get('high'),
        "low": quote.get('low')
    })

    return df.dropna()
