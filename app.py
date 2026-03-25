from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import sqlite3
import pandas as pd

app = FastAPI()
templates = Jinja2Templates(directory="templates")

DB_PATH = "ims.sqlite3"


# ===== DB取得 =====
def get_result_by_email(email: str):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT
          r.email, r.name,
          s.f1, s.f2, s.f3, s.f4, s.f5,
          f.improve_text, f.strength_text
        FROM respondents r
        LEFT JOIN scores s ON s.respondent_id = r.id
        LEFT JOIN feedback f ON f.respondent_id = r.id
        WHERE r.email = ?
        ORDER BY r.id DESC
        LIMIT 1
    """, (email,))

    row = cur.fetchone()
    conn.close()

    # 🔥 ここが最重要
    if row:
        return dict(row)

    return None


# ===== 因子別フィードバック =====
def generate_factor_feedback_from_csv(data):
    fb = pd.read_csv("feedback_master.csv", sep=None, engine="python")
    fb.columns = [str(c).strip().lstrip("\ufeff") for c in fb.columns]
    fb["factor"] = pd.to_numeric(fb["factor"], errors="coerce")

    result = {
        "F1": {"strengths": [], "improves": []},
        "F2": {"strengths": [], "improves": []},
        "F3": {"strengths": [], "improves": []},
        "F4": {"strengths": [], "improves": []},
        "F5": {"strengths": [], "improves": []},
    }

    score_map = {
        1: float(data["f1"]) if data["f1"] is not None else None,
        2: float(data["f2"]) if data["f2"] is not None else None,
        3: float(data["f3"]) if data["f3"] is not None else None,
        4: float(data["f4"]) if data["f4"] is not None else None,
        5: float(data["f5"]) if data["f5"] is not None else None,
    }

    for _, row in fb.iterrows():
        f = row["factor"]

        if pd.isna(f):
            continue

        f = int(f)
        fid = f"F{f}"
        score = score_map.get(f)

        if score is None:
            continue

        if score >= 3 and len(result[fid]["strengths"]) < 2:
            txt = str(row["feedback_strength"]).strip()
            if txt and txt.lower() != "nan":
                result[fid]["strengths"].append("・" + txt)

        elif score < 3 and len(result[fid]["improves"]) < 2:
            txt = str(row["feedback_improve"]).strip()
            if txt and txt.lower() != "nan":
                result[fid]["improves"].append("・" + txt)

    return result


# ===== API =====
@app.get("/result", response_class=HTMLResponse)
def result(request: Request, email: str):
    data = get_result_by_email(email)

    if not data:
        return HTMLResponse("該当データが見つかりません")

    factor_items = generate_factor_feedback_from_csv(data)

    return templates.TemplateResponse(
        "result.html",
        {
            "request": request,
            "data": data,
            "factor_items": factor_items
        }
    )


# ===== 起動時バッチ =====
try:
    from batch import run
    run()
    print("✅ Batch executed on startup")
except Exception as e:
    print("⚠️ Batch failed:", e)
