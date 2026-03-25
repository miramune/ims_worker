import json
import pandas as pd
import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from db import init_db, get_conn

# ====== 設定 ======
SPREADSHEET_ID = "1aoR16J8yGx1wkgK0DYHd51UcarYv8yOXmtFuThvdnu4"
SHEET_NAME = "scoring_sheet"  
SERVICE_ACCOUNT_FILE = "service_account.json"

# ====== 設定ファイル ======
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

FACTORS = config["factors"]


# ====== Google Sheets 読み込み ======
def load_sheet():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )

    service = build("sheets", "v4", credentials=credentials)
    sheet = service.spreadsheets()

    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A:AH"
    ).execute()

    values = result.get("values", [])

    if not values:
        print("❌ シートにデータがありません")
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]

    df = pd.DataFrame(rows, columns=header)

    # 列名整形
    df.columns = [str(c).strip() for c in df.columns]

    print("DEBUG columns:", df.columns)

    # ===== 列名自動変換 =====
    rename_map = {}

    for col in df.columns:
        if "メール" in col:
            rename_map[col] = "email"
        elif "名前" in col:
            rename_map[col] = "name"
        elif "タイムスタンプ" in col:
            rename_map[col] = "response_id"

    df.rename(columns=rename_map, inplace=True)

    print("DEBUG renamed columns:", df.columns)
    print("DEBUG sample:")
    print(df.head())

    return df


# ====== スコア計算 ======
def compute_scores(row):
    factor_scores = {}

    for fac in FACTORS:
        fid = fac["id"]
        items = fac["items"]

        vals = []
        for item in items:
            v = str(row.get(item, "")).strip()
            if v != "":
                vals.append(float(v))

        factor_scores[fid] = sum(vals)/len(vals) if vals else None

    valid = [v for v in factor_scores.values() if v is not None]
    overall = sum(valid)/len(valid) if valid else None

    return factor_scores, overall


# ====== フィードバック生成 ======
def generate_feedback(row):
    fb = pd.read_csv("feedback_master.csv", sep=None, engine="python")
    fb.columns = [str(c).strip().lstrip("\ufeff") for c in fb.columns]

    fb["serial"] = pd.to_numeric(fb["serial"], errors="coerce")

    improve, strength = [], []

    for i in range(1, 31):
        col = f"IM{str(i).zfill(2)}"
        v = str(row.get(col, "")).strip()

        if v == "":
            continue

        val = float(v)
        fb_row = fb[fb["serial"] == i]

        if fb_row.empty:
            continue

        if val <= 1:
            txt = str(fb_row.iloc[0]["feedback_improve"])
            if txt and txt.lower() != "nan":
                improve.append("・" + txt)

        elif val >= 3:
            txt = str(fb_row.iloc[0]["feedback_strength"])
            if txt and txt.lower() != "nan":
                strength.append("・" + txt)

    return "\n".join(improve), "\n".join(strength)


# ====== メイン処理 ======
def run():
    init_db()
    df = load_sheet()

    if df.empty:
        return

    conn = get_conn()
    cur = conn.cursor()

    for _, row in df.iterrows():
        email = str(row.get("email", "")).strip()

        if not email:
            continue

        response_id = str(row.get("response_id", "")).strip()
        name = str(row.get("name", "")).strip()

        # 重複チェック
        cur.execute(
            "SELECT id FROM respondents WHERE response_id=?",
            (response_id,)
        )
        if cur.fetchone():
            continue

        scores, overall = compute_scores(row)
        improve, strength = generate_feedback(row)

        # respondents
        cur.execute("""
        INSERT INTO respondents (response_id, email, name, processed_at, status)
        VALUES (?, ?, ?, ?, ?)
        """, (
            response_id,
            email,
            name,
            datetime.datetime.now().isoformat(),
            "done"
        ))

        rid = cur.lastrowid

        # scores
        cur.execute("""
        INSERT INTO scores (respondent_id, overall, f1, f2, f3, f4, f5)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            rid,
            overall,
            scores["F1"], scores["F2"],
            scores["F3"], scores["F4"], scores["F5"]
        ))

        # feedback
        cur.execute("""
        INSERT INTO feedback (respondent_id, improve_text, strength_text)
        VALUES (?, ?, ?)
        """, (rid, improve, strength))

    conn.commit()
    conn.close()

    print("✅ Batch completed successfully")


if __name__ == "__main__":
    run()
