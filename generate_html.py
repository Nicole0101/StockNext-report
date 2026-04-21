import pandas as pd
import config
from datetime import datetime, timedelta
from jinja2 import Template
import os
from main import get_full_stock_analysis  # 確保 data.py 已準備好

# ========================
# 1️⃣ 工具函數：資料結構化整理
# ========================


def format_output(results):
    results = [r for r in results if r]

    valid_results = [r for r in results if isinstance(r.get("chgPct"), (int, float))]
    invalid_results = [r for r in results if not isinstance(r.get("chgPct"), (int, float))]

    sorted_by_score = sorted(
        valid_results,
        key=lambda x: x.get("score", -999999),
        reverse=True
    )

    sorted_by_chg = sorted(
        valid_results,
        key=lambda x: x.get("chgPct", -999999),
        reverse=True
    )

    # 把無資料股接在最後
    stocks = sorted_by_chg + invalid_results

    return {
        "stocks": stocks,
        "top_stocks": sorted_by_score[:5],
        "hot_stocks": sorted_by_chg[:5],
        "weak_stocks": sorted_by_chg[-5:] if sorted_by_chg else [],
        "rebound_list": [s for s in valid_results if "反彈" in s.get("strategy", "")],
        "selloff_list": [s for s in valid_results if "出貨" in s.get("strategy", "")],
        "buy_signal_list": [s for s in valid_results if s.get("sig") == 1],
        "volume_up_list": [s for s in valid_results if s.get("volume_ok")],
        "bottom_pick_list": [s for s in valid_results if s.get("entry_note") == "抄底"]
    }

def build_strings(data):
    def safe_join(lst):
        return ", ".join([s["name"] for s in lst if s])

    return {
        "top_str": safe_join(data.get("top_stocks", [])),
        "weak_str": safe_join(data.get("weak_stocks", [])),
        "rebound_str": safe_join(data.get("rebound_list", [])[:5]),
        "selloff_str": safe_join(data.get("selloff_list", [])[:5]),
    }


# ========================
# 2️⃣ 主流程
# ========================
def main():
    try:
        report_type = config.REPORT_TYPE
        csv_file = config.CSV_FILE
        report_title = config.REPORT_TITLE
        output_file = config.OUTPUT_FILE
        df = pd.read_csv(csv_file, sep="\t", encoding="utf-8-sig", dtype=str)
        df.columns = df.columns.str.strip()
        stock_list = df.rename(columns={"Ticker": "stock_id", "Name": "name"}).to_dict(orient="records")      
        print(df.shape)
        print(df.columns.tolist())
        print(df.head())
        print(df.tail())

        stock_list = df.rename(
            columns={"Ticker": "stock_id", "Name": "name"}
        ).to_dict(orient="records")

    except Exception as e:
        print(f"❌ 讀取 config.yml 或 CSV 失敗: {e}")
        return

    print(f"🚀 開始分析股票... [{report_type}]")
    results = get_full_stock_analysis(stock_list)

    if not results:
        print("⚠️ 無分析結果")
        return

    data = format_output(results)
    text_data = build_strings(data)
    print("stock keys:", data["stocks"][0].keys() if data["stocks"] else [])

    now_dt = datetime.utcnow() + timedelta(hours=8)
    now_str = now_dt.strftime("%m%d%H%M")
    filename = f"{output_file}_{now_str}.html"

    # GitHub Pages 連結
    repo_full = os.getenv("GITHUB_REPOSITORY", "nicole0101/Holding")
    branch = os.getenv("GITHUB_REF_NAME", "main")
    user, repo = repo_full.split("/")

    if branch == "main":
        file_url = f"https://{user}.github.io/{repo}/{filename}"
    else:
        file_url = f"https://github.com/{user}/{repo}/blob/{branch}/{filename}"

    # 副標題
    if report_type == "Holding":
        report_subtitle = "持股追蹤與風險檢視"
    elif report_type == "Gold":
        report_subtitle = "潛力黃金股觀察名單"
    else:
        report_subtitle = "台股技術分析"

    try:
        with open("template.html", "r", encoding="utf-8") as f:
            template = Template(f.read())

        html_content = template.render(
            stocks=data["stocks"],
            top_stocks=text_data["top_str"],
            weak_stocks=text_data["weak_str"],
            rebound_list=text_data["rebound_str"],
            selloff_list=text_data["selloff_str"],
            report_title=report_title,
            report_subtitle=report_subtitle,
            report_type=report_type,
            generated_time=now_dt.strftime("%Y-%m-%d %H:%M")
        )

        for f_name in [filename, "index.html"]:
            with open(f_name, "w", encoding="utf-8") as f:
                f.write(html_content)

        print(f"✅ HTML 已生成：{filename}")

    except Exception as e:
        print(f"❌ HTML 生成失敗: {e}")
        return

    send_line_notify(data, file_url, report_title, report_type)


# ========================
# 3️⃣ LINE 通知模組
# ========================
def send_line_notify(data, file_url, report_title, report_type):
    """獨立發送 LINE 訊息"""
    try:
        from line_push import send_line

        stocks = data.get("stocks", [])
        top5 = [f"{s['name']}({s['chgPct']}%)" for s in stocks[:5]]
        weak5 = [f"{s['name']}({s['chgPct']}%)" for s in stocks[-5:]]

        if report_type == "Holding":
            report_header = "📊 持股追蹤分析報告"
            strong_label = "🔥 強勢持股"
            weak_label = "⚠ 弱勢持股"
        elif report_type == "Gold":
            report_header = "📊 黃金股觀察報告"
            strong_label = "🔥 強勢黃金股"
            weak_label = "⚠ 弱勢黃金股"
        else:
            report_header = f"📊 {report_title}"
            strong_label = "🔥 強勢股"
            weak_label = "⚠ 弱勢股"

        msg = f"""
{report_header}

{strong_label}
{chr(10).join(top5)}

{weak_label}
{chr(10).join(weak5)}

📎 完整詳細報表：
{file_url}
        """
        send_line(msg.strip())
        print("✅ LINE 通知已發送")

    except Exception as e:
        print(f"⚠️ LINE 通知發送失敗: {e}")


if __name__ == "__main__":
    main()
