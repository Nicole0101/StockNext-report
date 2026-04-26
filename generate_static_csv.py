from datetime import datetime
import argparse
import os
import time

import pandas as pd
import requests
import config

from data_sources import get_revenue_raw, get_per_pbr_90d_stats
from financial_analysis import (
    get_eps_analysis,
    get_profit_ratio,
    extract_metric,
)

DATA_COLS = [
    "eps_Y", "eps_ttm", "per_Y", "per_ttm",
    "rev", "rev_mom", "rev_qoq", "rev_yoy",
    "gross_margin", "gross_margin_qoq", "gross_margin_yoy_diff",
    "operating_margin", "operating_margin_qoq", "operating_margin_yoy_diff",
    "net_margin", "net_margin_qoq", "net_margin_yoy_diff",
    "per_latest", "per_90d_high", "per_90d_low",
    "pbr_latest", "pbr_90d_high", "pbr_90d_low",
]

GROUPS = {
    "eps": ["eps_Y", "eps_ttm", "per_Y", "per_ttm"],
    "revenue": ["rev", "rev_mom", "rev_qoq", "rev_yoy"],
    "profit": [
        "gross_margin", "gross_margin_qoq", "gross_margin_yoy_diff",
        "operating_margin", "operating_margin_qoq", "operating_margin_yoy_diff",
        "net_margin", "net_margin_qoq", "net_margin_yoy_diff",
    ],
    "valuation": [
        "per_latest", "per_90d_high", "per_90d_low",
        "pbr_latest", "pbr_90d_high", "pbr_90d_low",
    ],
}

BASE_COLS = ["stock_id", "name"] + DATA_COLS + [
    "static_updated_at", "static_status", "static_reason",
]

SOURCE_META_COLS = []
for g in GROUPS:
    SOURCE_META_COLS += [f"{g}_status", f"{g}_reason"]

ORDERED_COLS = BASE_COLS + SOURCE_META_COLS

TERMINAL_STATUSES = {"ok", "partial_ok"}
SOURCE_TERMINAL_STATUSES = {"ok", "no_data"}


def now_utc_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def is_blank_value(value) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "none", "null"}


def is_finmind_limit_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return (
        "upper limit" in msg
        or "reach the upper limit" in msg
        or "requests reach" in msg
        or "api_request_limit" in msg
        or "429" in msg
    )


def all_blank(row: dict, cols: list[str]) -> bool:
    return all(is_blank_value(row.get(c)) for c in cols)


def any_blank(row: dict, cols: list[str]) -> bool:
    return any(is_blank_value(row.get(c)) for c in cols)


def get_finmind_usage():
    token = os.getenv("FINMIND_TOKEN")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    url = "https://api.web.finmindtrade.com/v2/user_info"
    resp = requests.get(url, headers=headers, timeout=30)
    data = resp.json()
    used = int(data.get("user_count", 0) or 0)
    limit = int(data.get("api_request_limit", 0) or 0)
    remain = max(limit - used, 0)
    print(f"FinMind usage: {used}/{limit}, remain={remain}", flush=True)
    return used, limit, remain


def set_group_status(row: dict, group: str, status: str, reason: str = ""):
    row[f"{group}_status"] = status
    row[f"{group}_reason"] = reason or ""


def empty_static_row(s: dict) -> dict:
    row = {c: None for c in ORDERED_COLS}
    row["stock_id"] = str(s["stock_id"]).strip()
    row["name"] = s.get("name")
    row["static_updated_at"] = now_utc_str()
    row["static_status"] = "incomplete"
    row["static_reason"] = "not processed yet"
    for g in GROUPS:
        set_group_status(row, g, "pending", "not processed yet")
    return row


def get_revenue_trend(stock_id):
    data = get_revenue_raw(stock_id)
    if not data:
        return None, "no revenue raw data"

    df = pd.DataFrame(data)
    if "revenue" not in df.columns:
        if "value" in df.columns:
            df["revenue"] = df["value"]
        else:
            return None, "revenue/value column not found"

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df = df.sort_values("date").dropna(subset=["date", "revenue"])

    if len(df) < 13:
        return None, f"only {len(df)} revenue months; need at least 13"

    curr = df.iloc[-1]["revenue"]
    prev_m = df.iloc[-2]["revenue"]
    prev_q = df.iloc[-4]["revenue"]
    prev_y = df.iloc[-13]["revenue"]

    def pct(a, b):
        return (a - b) / b * 100 if b else None

    return {
        "rev": round(curr / 1e8, 2),
        "mom": round(pct(curr, prev_m), 2),
        "qoq": round(pct(curr, prev_q), 2),
        "yoy": round(pct(curr, prev_y), 2),
    }, ""


def finalize_static_status(row: dict) -> dict:
    problems = []
    no_data_groups = []

    for g, cols in GROUPS.items():
        g_status = str(row.get(f"{g}_status", "")).strip().lower()
        g_reason = str(row.get(f"{g}_reason", "")).strip()

        if g_status == "ok":
            missing = [c for c in cols if is_blank_value(row.get(c))]
            if missing:
                set_group_status(
                    row, g, "incomplete", "ok status but missing: " + ",".join(missing[:8]))
                problems.append(f"{g}: missing fields")
        elif g_status == "no_data":
            no_data_groups.append(f"{g}({g_reason})" if g_reason else g)
        elif g_status in {"api_limited", "limited"}:
            problems.append(f"{g}: api_limited")
        elif g_status == "error":
            problems.append(
                f"{g}: error" + (f" - {g_reason}" if g_reason else ""))
        else:
            problems.append(f"{g}: {g_status or 'pending'}")

    if problems:
        if any("api_limited" in p for p in problems):
            row["static_status"] = "api_limited"
        elif any("error" in p for p in problems):
            row["static_status"] = "error"
        else:
            row["static_status"] = "incomplete"
        row["static_reason"] = "; ".join(problems[:6])
    elif no_data_groups:
        row["static_status"] = "partial_ok"
        row["static_reason"] = "source no data: " + \
            "; ".join(no_data_groups[:6])
    else:
        row["static_status"] = "ok"
        row["static_reason"] = ""

    return row


def build_static_row(s: dict) -> dict:
    stock_id = str(s["stock_id"]).strip()
    name = s.get("name")
    row = empty_static_row(s)
    row["static_updated_at"] = now_utc_str()

    # EPS and annual/TTM PER.
    try:
        eps_res = get_eps_analysis(stock_id, 1)
        print("EPS =", stock_id, eps_res, flush=True)
        eps_res = tuple(eps_res) if isinstance(eps_res, tuple) else (None,) * 4
        eps_res = eps_res + (None,) * (4 - len(eps_res))
        eps_last, eps_ttm, per_last, per_ttm = eps_res[:4]
        row["eps_Y"] = eps_last
        row["eps_ttm"] = eps_ttm
        row["per_Y"] = per_last
        row["per_ttm"] = per_ttm
        if all_blank(row, GROUPS["eps"]):
            set_group_status(row, "eps", "no_data",
                             "EPS/PER source returned empty")
        elif any_blank(row, GROUPS["eps"]):
            set_group_status(row, "eps", "incomplete",
                             "EPS/PER source returned partial data")
        else:
            set_group_status(row, "eps", "ok", "")
    except Exception as e:
        if is_finmind_limit_error(e):
            set_group_status(row, "eps", "api_limited", str(e))
            return finalize_static_status(row)
        set_group_status(row, "eps", "error", str(e))

    # Monthly revenue trend.
    try:
        rev, reason = get_revenue_trend(stock_id)
        rev = rev or {}
        row["rev"] = rev.get("rev")
        row["rev_mom"] = rev.get("mom")
        row["rev_qoq"] = rev.get("qoq")
        row["rev_yoy"] = rev.get("yoy")
        if rev:
            if any_blank(row, GROUPS["revenue"]):
                set_group_status(row, "revenue", "incomplete",
                                 "revenue source returned partial data")
            else:
                set_group_status(row, "revenue", "ok", "")
        else:
            set_group_status(row, "revenue", "no_data",
                             reason or "revenue source returned empty")
    except Exception as e:
        if is_finmind_limit_error(e):
            set_group_status(row, "revenue", "api_limited", str(e))
            return finalize_static_status(row)
        set_group_status(row, "revenue", "error", str(e))

    # Profit ratios.
    try:
        profit_res = get_profit_ratio(stock_id)
        cur_g, qoq_g, yoy_g = extract_metric(profit_res, "gross")
        cur_o, qoq_o, yoy_o = extract_metric(profit_res, "op")
        cur_n, qoq_n, yoy_n = extract_metric(profit_res, "net")
        row["gross_margin"] = cur_g
        row["gross_margin_qoq"] = qoq_g
        row["gross_margin_yoy_diff"] = yoy_g
        row["operating_margin"] = cur_o
        row["operating_margin_qoq"] = qoq_o
        row["operating_margin_yoy_diff"] = yoy_o
        row["net_margin"] = cur_n
        row["net_margin_qoq"] = qoq_n
        row["net_margin_yoy_diff"] = yoy_n
        if all_blank(row, GROUPS["profit"]):
            set_group_status(row, "profit", "no_data",
                             "profit ratio source returned empty")
        elif any_blank(row, GROUPS["profit"]):
            set_group_status(row, "profit", "incomplete",
                             "profit ratio source returned partial data")
        else:
            set_group_status(row, "profit", "ok", "")
    except Exception as e:
        if is_finmind_limit_error(e):
            set_group_status(row, "profit", "api_limited", str(e))
            return finalize_static_status(row)
        set_group_status(row, "profit", "error", str(e))

    # 90-day PER/PBR.
    try:
        per_pbr = get_per_pbr_90d_stats(stock_id) or {}
        row["per_latest"] = per_pbr.get("per")
        row["per_90d_high"] = per_pbr.get("per_90d_high")
        row["per_90d_low"] = per_pbr.get("per_90d_low")
        row["pbr_latest"] = per_pbr.get("pbr")
        row["pbr_90d_high"] = per_pbr.get("pbr_90d_high")
        row["pbr_90d_low"] = per_pbr.get("pbr_90d_low")
        if all_blank(row, GROUPS["valuation"]):
            set_group_status(row, "valuation", "no_data",
                             "PER/PBR source returned empty")
        elif any_blank(row, GROUPS["valuation"]):
            set_group_status(row, "valuation", "incomplete",
                             "PER/PBR source returned partial data")
        else:
            set_group_status(row, "valuation", "ok", "")
    except Exception as e:
        if is_finmind_limit_error(e):
            set_group_status(row, "valuation", "api_limited", str(e))
            return finalize_static_status(row)
        set_group_status(row, "valuation", "error", str(e))

    return finalize_static_status(row)


def normalize_static_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=ORDERED_COLS)
    df = df.copy()
    df.columns = df.columns.str.strip()
    for c in ORDERED_COLS:
        if c not in df.columns:
            df[c] = None
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    return df[ORDERED_COLS]


def read_existing_static(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=ORDERED_COLS)
    return normalize_static_df(pd.read_csv(path, encoding="utf-8-sig", dtype=str))


def atomic_write_csv(df: pd.DataFrame, path: str):
    tmp_path = path + ".tmp"
    df = normalize_static_df(df)
    df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    os.replace(tmp_path, path)


def legacy_missing_data_cols(row: dict) -> list[str]:
    return [c for c in DATA_COLS if is_blank_value(row.get(c))]


def should_update(row, retry_errors: bool, retry_no_data: bool, force: bool) -> bool:
    if force or row is None:
        return True
    if isinstance(row, pd.Series):
        row = row.to_dict()

    static_status = str(row.get("static_status", "")).strip().lower()

    # Rows created by v3 have source statuses. Trust them more than field blankness.
    has_source_meta = any(not is_blank_value(
        row.get(f"{g}_status")) for g in GROUPS)
    if has_source_meta:
        source_statuses = [
            str(row.get(f"{g}_status", "")).strip().lower() for g in GROUPS]
        if all(s in SOURCE_TERMINAL_STATUSES for s in source_statuses):
            # ok: all data present. partial_ok: some source confirmed no_data. Both are terminal by default.
            if static_status == "partial_ok" and retry_no_data:
                return True
            return False
        if any(s == "no_data" for s in source_statuses) and retry_no_data:
            return True
        if static_status == "error" and not retry_errors:
            return False
        return True

    # Legacy rows do not know whether blanks are true no_data, so blanks must be rechecked once.
    if static_status == "error" and not retry_errors:
        return False
    if static_status in TERMINAL_STATUSES and not legacy_missing_data_cols(row):
        return False
    return True


def repair_legacy_status_only(df: pd.DataFrame) -> pd.DataFrame:
    repaired = []
    for _, r in df.iterrows():
        row = r.to_dict()
        has_source_meta = any(not is_blank_value(
            row.get(f"{g}_status")) for g in GROUPS)
        if has_source_meta:
            row = finalize_static_status(row)
        else:
            missing = legacy_missing_data_cols(row)
            if missing:
                row["static_status"] = "incomplete"
                row["static_reason"] = "legacy row missing fields; run API check to distinguish no_data: " + \
                    ",".join(missing[:8])
            else:
                row["static_status"] = "ok"
                row["static_reason"] = ""
        repaired.append(row)
    return normalize_static_df(pd.DataFrame(repaired))


def build_incremental(stock_list, output_file, max_rows, min_remain, retry_errors, retry_no_data, force, sleep_sec, repair_only, check_every):
    existing = read_existing_static(output_file)
    existing = repair_legacy_status_only(existing)
    existing_by_id = {
        str(r["stock_id"]): r.to_dict()
        for _, r in existing.iterrows()
        if str(r.get("stock_id", "")).strip()
    }

    src_ids = [str(s["stock_id"]).strip() for s in stock_list]
    rows_by_id = {sid: existing_by_id.get(
        sid, empty_static_row(s)) for sid, s in zip(src_ids, stock_list)}

    ordered_rows = [rows_by_id[str(s["stock_id"]).strip()] for s in stock_list]
    atomic_write_csv(pd.DataFrame(ordered_rows), output_file)

    if repair_only:
        print(f"Repaired statuses only -> {output_file}", flush=True)
        return

    candidates = []
    for s in stock_list:
        sid = str(s["stock_id"]).strip()
        current = rows_by_id.get(sid)
        if should_update(current, retry_errors=retry_errors, retry_no_data=retry_no_data, force=force):
            candidates.append(s)

    print(f"Existing rows: {len(existing_by_id)}", flush=True)
    print(f"Total source stocks: {len(stock_list)}", flush=True)
    print(f"Need update this run: {len(candidates)}", flush=True)

    processed = 0
    stop_reason = "completed"

    for i, s in enumerate(candidates, 1):
        if max_rows is not None and processed >= max_rows:
            stop_reason = f"max_rows reached: {max_rows}"
            break

        # Checking user_info too often can itself consume quota on some plans.
        # Check before the first stock and then every N processed stocks.
        should_check_usage = processed == 0 or (
            check_every and processed % check_every == 0)
        if should_check_usage:
            try:
                _, _, remain = get_finmind_usage()
                if remain <= min_remain:
                    stop_reason = f"FinMind remain <= min_remain: {remain} <= {min_remain}"
                    break
            except Exception as e:
                print(
                    f"Cannot check FinMind usage, continue cautiously: {e}", flush=True)

        sid = str(s["stock_id"]).strip()
        print(
            f"Processing {i}/{len(candidates)}: {sid} {s.get('name')}", flush=True)

        row = build_static_row(s)
        rows_by_id[sid] = row
        processed += 1

        ordered_rows = [rows_by_id[str(x["stock_id"]).strip()]
                        for x in stock_list]
        atomic_write_csv(pd.DataFrame(ordered_rows), output_file)
        print(
            f"Saved progress: {processed} updated in this run -> {output_file}", flush=True)

        if str(row.get("static_status", "")).lower() == "api_limited":
            stop_reason = f"FinMind API upper limit reached at {sid} {s.get('name')}: {row.get('static_reason')}"
            break

        if sleep_sec > 0:
            time.sleep(sleep_sec)

    final_df = read_existing_static(output_file)
    status_counts = final_df["static_status"].astype(
        str).str.lower().value_counts().to_dict() if not final_df.empty else {}
    print(f"Run stopped: {stop_reason}", flush=True)
    print(f"Updated this run: {processed}", flush=True)
    print(
        f"AllStatic progress: {status_counts}, total={len(final_df)}", flush=True)


def load_stock_list():
    csv_file = config.CSV_FILE
    src_df = pd.read_csv(csv_file, sep="\t", encoding="utf-8-sig", dtype=str)
    src_df.columns = src_df.columns.str.strip()
    src_df = src_df.rename(columns={"Ticker": "stock_id", "Name": "name"})
    src_df["stock_id"] = src_df["stock_id"].astype(str).str.strip()
    return src_df.to_dict(orient="records")


def main():
    parser = argparse.ArgumentParser(
        description="Incrementally build AllStatic.csv and distinguish no_data from incomplete/API limit.")
    parser.add_argument("--output", default=getattr(config,
                        "STATIC_OUTPUT_FILE", "AllStatic.csv"))
    parser.add_argument("--max-rows", type=int, default=None,
                        help="Max stocks to update in this run.")
    parser.add_argument("--min-remain", type=int, default=20,
                        help="Stop before FinMind remain drops to this number.")
    parser.add_argument("--retry-errors", action="store_true",
                        help="Retry rows whose static_status is error.")
    parser.add_argument("--retry-no-data", action="store_true",
                        help="Retry rows marked partial_ok/no_data.")
    parser.add_argument("--force", action="store_true",
                        help="Refresh every stock even if existing row is terminal.")
    parser.add_argument("--repair-only", action="store_true",
                        help="Only repair status columns; do not call APIs.")
    parser.add_argument("--sleep-sec", type=float,
                        default=0.2, help="Sleep between stocks.")
    parser.add_argument("--check-every", type=int, default=10,
                        help="Check FinMind usage before first stock and every N processed stocks. Use 1 for every stock.")
    args = parser.parse_args()

    try:
        stock_list = load_stock_list()
    except Exception as e:
        print(f"Failed to read source CSV/config: {e}", flush=True)
        return

    build_incremental(
        stock_list=stock_list,
        output_file=args.output,
        max_rows=args.max_rows,
        min_remain=args.min_remain,
        retry_errors=args.retry_errors,
        retry_no_data=args.retry_no_data,
        force=args.force,
        sleep_sec=args.sleep_sec,
        repair_only=args.repair_only,
        check_every=max(args.check_every, 1),
    )


if __name__ == "__main__":
    main()
