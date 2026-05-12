import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from FinMind.data import DataLoader
from loguru import logger

API_TOKEN = os.getenv('FINMIND_TOKEN')
API_URL = 'https://api.finmindtrade.com/api/v4/data'
api = DataLoader()

_INITIAL_QUOTA_PRINTED = False

# 停用所有來自 FinMind 的 Log 訊息
logger.remove()
logging.getLogger('FinMind').setLevel(logging.WARNING)



def _safe_response_json(res):
    """避免 API 回傳非 JSON 時，印錯誤又讓程式中斷。"""
    try:
        return res.json()
    except Exception:
        return {}


def _extract_remaining_quota(data, res=None):
    """從 FinMind 回傳 body/header 中盡量抓出剩餘次數/額度資訊。"""
    if res is not None:
        for key in [
            "X-RateLimit-Remaining", "X-Rate-Limit-Remaining",
            "RateLimit-Remaining", "x-ratelimit-remaining"
        ]:
            value = res.headers.get(key)
            if value not in [None, ""]:
                return f"header {key}={value}"

    for key in [
        "api_usage", "api_remaining", "remaining", "remaining_count",
        "quota", "limit", "msg", "message", "status"
    ]:
        value = data.get(key) if isinstance(data, dict) else None
        if value not in [None, ""]:
            text = str(value)
            if any(word in text.lower() for word in [
                "remaining", "quota", "limit", "api", "剩餘", "次數", "額度"
            ]):
                return text
    return None


def _print_initial_quota_once(data, res=None):
    """第一次收到 API 回應時，印出起始剩餘次數。"""
    global _INITIAL_QUOTA_PRINTED
    if _INITIAL_QUOTA_PRINTED:
        return

    quota_msg = _extract_remaining_quota(data, res)
    if quota_msg is None:
        quota_msg = "API 回應未提供剩餘次數欄位"

    print(f"🔢 FinMind API 起始剩餘次數: {quota_msg}")
    _INITIAL_QUOTA_PRINTED = True


def _print_api_status_error(source, stock_id, res, data=None):
    """非 200/異常 API 狀態時，統一印出 status code 與訊息。"""
    if data is None:
        data = _safe_response_json(res)

    msg = data.get("msg") or data.get("message") or data.get("status") or res.text[:200]
    print(
        f"❌ {source} API error {stock_id}: "
        f"status_code={res.status_code}, msg={msg}"
    )


def get_stock_data(stock_id):
    try:
        params = {
            'dataset': 'TaiwanStockPrice',
            'data_id': str(stock_id),
            'start_date': '2023-01-01',
            'token': API_TOKEN,
        }
        res = requests.get(API_URL, params=params, timeout=180)
        data = _safe_response_json(res)
        _print_initial_quota_once(data, res)

        if res.status_code == 402:
            _print_api_status_error('get_stock_data', stock_id, res, data)
            raise RuntimeError(
                f"FinMind quota exceeded for {stock_id}: {data.get('msg')}")

        if res.status_code != 200:
            _print_api_status_error('get_stock_data', stock_id, res, data)
            return pd.DataFrame()

        if 'data' not in data or len(data['data']) == 0:
            print(
                f"⚠️ get_stock_data empty {stock_id}: status={res.status_code}, msg={data.get('msg')}")
            return pd.DataFrame()

        df = pd.DataFrame(data['data'])

        volume_col = None
        for c in ['Trading_Volume', 'trading_volume', 'Trading_Volume_1000']:
            if c in df.columns:
                volume_col = c
                break

        required_cols = ['date', 'open', 'close', 'max', 'min']
        if volume_col:
            required_cols.append(volume_col)

        df = df[required_cols].copy()
        df['date'] = pd.to_datetime(df['date'])

        if volume_col:
            df['volume'] = pd.to_numeric(df[volume_col], errors='coerce')
            if df['volume'].max() > 100000:
                df['volume'] = df['volume'] / 1000
        else:
            df['volume'] = None

        df = df.dropna(subset=['open', 'close', 'max',
                       'min']).sort_values('date')

        return df
    except RuntimeError:
        raise
    except Exception as e:
        print(f'❌ get_stock_data error {stock_id}: {e}')
        return pd.DataFrame()


def get_revenue_raw(stock_id):
    try:
        params = {
            'dataset': 'TaiwanStockMonthRevenue',  # 🔥 月營收
            'data_id': stock_id,
            'start_date': '2022-01-01',
            'token': API_TOKEN,
        }

        res = requests.get(API_URL, params=params, timeout=10)
        res_data = _safe_response_json(res)
        _print_initial_quota_once(res_data, res)

        if res.status_code != 200:
            _print_api_status_error('revenue source', stock_id, res, res_data)
            return []

        data = res_data.get('data', [])
        return data

    except Exception as e:
        print(f'❌ revenue source error {stock_id}: {e}')
        return []


def get_profit_ratio(stock_id):
    try:
        df = api.taiwan_stock_financial_statement(
            stock_id=stock_id,
            start_date='2022-01-01',
        )
        return df
    except Exception as e:
        print(f'❌ profit source error {stock_id}: {e}')
        return pd.DataFrame()


def get_eps_raw(stock_id):
    try:
        params = {
            'dataset': 'TaiwanStockFinancialStatements',
            'data_id': stock_id,
            'start_date': '2020-01-01',
            'token': API_TOKEN,
        }
        res = requests.get(API_URL, params=params, timeout=10)
        data = _safe_response_json(res)
        _print_initial_quota_once(data, res)

        if res.status_code != 200:
            _print_api_status_error('EPS source', stock_id, res, data)
            return []

        return data.get('data', [])
    except Exception as e:
        print(f'❌ EPS source error {stock_id}: {e}')
        return []


def get_dividend_raw(stock_id):
    try:
        params = {
            'dataset': 'TaiwanStockDividend',
            'data_id': stock_id,
            'start_date': '2020-01-01',
            'token': API_TOKEN,
        }
        res = requests.get(API_URL, params=params, timeout=10)
        data = _safe_response_json(res)
        _print_initial_quota_once(data, res)

        if res.status_code != 200:
            _print_api_status_error('dividend source', stock_id, res, data)
            return []
        return data.get('data', [])
    except Exception as e:
        print(f'❌ dividend source error {stock_id}: {e}')
        return []


def get_per_raw(stock_id):
    try:
        params = {
            'dataset': 'TaiwanStockPER',
            'data_id': stock_id,
            'start_date': '2023-01-01',
            'token': API_TOKEN,
        }
        res = requests.get(API_URL, params=params, timeout=10)
        data = _safe_response_json(res)
        _print_initial_quota_once(data, res)

        if res.status_code != 200:
            _print_api_status_error('PER source', stock_id, res, data)
            return []

        return data.get('data', [])
    except Exception as e:
        print(f'❌ PER source error {stock_id}: {e}')
        return []


def get_per_pbr_90d_stats(stock_id, days=90):
    """
    回傳：
    {
        "per": 最新PER,
        "per_90d_high": 90天PER最高,
        "per_90d_low": 90天PER最低,
        "pbr": 最新PBR,
        "pbr_90d_high": 90天PBR最高,
        "pbr_90d_low": 90天PBR最低,
    }
    """
    try:
        start_date = (datetime.now() - timedelta(days=days * 2)
                      ).strftime("%Y-%m-%d")
        # 抓寬一點，避免遇到非交易日不夠 90 筆

        params = {
            "dataset": "TaiwanStockPER",
            "data_id": str(stock_id),
            "start_date": start_date,
            "token": API_TOKEN,
        }

        res = requests.get(API_URL, params=params, timeout=10)
        res_data = _safe_response_json(res)
        _print_initial_quota_once(res_data, res)

        if res.status_code != 200:
            _print_api_status_error('PER/PBR 90D', stock_id, res, res_data)
            return {
                "per": None,
                "per_90d_high": None,
                "per_90d_low": None,
                "pbr": None,
                "pbr_90d_high": None,
                "pbr_90d_low": None,
            }

        data = res_data.get("data", [])
        if not data:
            return {
                "per": None,
                "per_90d_high": None,
                "per_90d_low": None,
                "pbr": None,
                "pbr_90d_high": None,
                "pbr_90d_low": None,
            }

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").tail(days).copy()

        # 用候選欄位名提高相容性
        per_col = next(
            (c for c in ["price_to_earning_ratio",
             "PER", "per"] if c in df.columns),
            None
        )
        pbr_col = next(
            (c for c in ["price_book_ratio", "PBR", "pbr"] if c in df.columns),
            None
        )

        if per_col:
            df[per_col] = pd.to_numeric(df[per_col], errors="coerce")
            latest_per = df[per_col].iloc[-1] if not df.empty else None
            per_high = df[per_col].max()
            per_low = df[per_col].min()
        else:
            latest_per = per_high = per_low = None

        if pbr_col:
            df[pbr_col] = pd.to_numeric(df[pbr_col], errors="coerce")
            latest_pbr = df[pbr_col].iloc[-1] if not df.empty else None
            pbr_high = df[pbr_col].max()
            pbr_low = df[pbr_col].min()
        else:
            latest_pbr = pbr_high = pbr_low = None

        def safe_round(v):
            return round(float(v), 2) if pd.notna(v) else None

        return {
            "per": safe_round(latest_per),
            "per_90d_high": safe_round(per_high),
            "per_90d_low": safe_round(per_low),
            "pbr": safe_round(latest_pbr),
            "pbr_90d_high": safe_round(pbr_high),
            "pbr_90d_low": safe_round(pbr_low),
        }

    except Exception as e:
        print(f"❌ PER/PBR 90D error {stock_id}: {e}")
        return {
            "per": None,
            "per_90d_high": None,
            "per_90d_low": None,
            "pbr": None,
            "pbr_90d_high": None,
            "pbr_90d_low": None,
        }
