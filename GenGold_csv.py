import os
import shutil
import pandas as pd

goldcsv_dir = "Git/Gold/Goldcsv"
gold_dir = "Git/Gold"

output_in_goldcsv = os.path.join(goldcsv_dir, "Gold.csv")
output_in_gold = os.path.join(gold_dir, "Gold.csv")

all_rows = []

for filename in os.listdir(goldcsv_dir):
    file_path = os.path.join(goldcsv_dir, filename)

    if not os.path.isfile(file_path):
        continue

    if filename.lower() == "gold.csv":
        continue

    if not filename.lower().endswith((".csv", ".txt")):
        continue

    try:
        df = pd.read_csv(file_path, sep="\t", encoding="utf-8-sig")

        if {"Ticker", "Name"}.issubset(df.columns):
            temp = df[["Ticker", "Name"]].copy()
            temp.columns = ["Ticker", "Name"]
        elif {"代碼", "名稱"}.issubset(df.columns):
            temp = df[["代碼", "名稱"]].copy()
            temp.columns = ["Ticker", "Name"]
        else:
            print(f"skip: {filename}, columns={df.columns.tolist()}")
            continue

        temp["Ticker"] = temp["Ticker"].astype(str).str.strip()
        temp["Name"] = temp["Name"].astype(str).str.strip()

        temp = temp[
            (temp["Ticker"] != "") &
            (temp["Name"] != "") &
            (temp["Ticker"].str.lower() != "nan") &
            (temp["Name"].str.lower() != "nan")
        ]

        all_rows.append(temp)
        print(f"loaded: {filename}, rows={len(temp)}")

    except Exception as e:
        print(f"failed: {filename}, error={e}")

import os
import shutil
import pandas as pd

base_dir = os.path.dirname(os.path.abspath(__file__))
goldcsv_dir = os.path.join(base_dir, "Goldcsv")
gold_dir = base_dir

output_in_goldcsv = os.path.join(goldcsv_dir, "Gold.csv")
output_in_gold = os.path.join(gold_dir, "Gold.csv")

all_rows = []

print("cwd =", os.getcwd())
print("script dir =", base_dir)
print("goldcsv_dir =", goldcsv_dir)

if not os.path.isdir(goldcsv_dir):
    raise FileNotFoundError(f"Goldcsv directory not found: {goldcsv_dir}")

for filename in os.listdir(goldcsv_dir):
    file_path = os.path.join(goldcsv_dir, filename)

    if not os.path.isfile(file_path):
        continue

    if filename.lower() == "gold.csv":
        continue

    if not filename.lower().endswith((".csv", ".txt")):
        continue

    try:
        df = pd.read_csv(file_path, sep="\t", encoding="utf-8-sig")

        if {"Ticker", "Name"}.issubset(df.columns):
            temp = df[["Ticker", "Name"]].copy()
            temp.columns = ["Ticker", "Name"]
        elif {"代碼", "名稱"}.issubset(df.columns):
            temp = df[["代碼", "名稱"]].copy()
            temp.columns = ["Ticker", "Name"]
        else:
            print(f"skip: {filename}, columns={df.columns.tolist()}")
            continue

        temp["Ticker"] = temp["Ticker"].astype(str).str.strip()
        temp["Name"] = temp["Name"].astype(str).str.strip()

        temp = temp[
            (temp["Ticker"] != "") &
            (temp["Name"] != "") &
            (temp["Ticker"].str.lower() != "nan") &
            (temp["Name"].str.lower() != "nan")
        ]

        all_rows.append(temp)
        print(f"loaded: {filename}, rows={len(temp)}")

    except Exception as e:
        print(f"failed: {filename}, error={e}")

if all_rows:
    result = pd.concat(all_rows, ignore_index=True)
    result["Ticker_num"] = pd.to_numeric(result["Ticker"], errors="coerce")

    result = (
        result
        .drop_duplicates(subset=["Ticker"], keep="first")
        .sort_values(by=["Ticker_num", "Ticker"], ascending=[True, True])
        .drop(columns=["Ticker_num"])
        .reset_index(drop=True)
    )

    result.to_csv(output_in_goldcsv, sep="\t", index=False, encoding="utf-8-sig")
    shutil.copy2(output_in_goldcsv, output_in_gold)

    print(f"written: {output_in_goldcsv}")
    print(f"copied: {output_in_gold}")
    print(f"rows: {len(result)}")
else:
    print("no data")
else:
    print("no data")
