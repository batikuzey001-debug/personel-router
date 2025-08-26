import os, json, time, random
from typing import List, Dict, Tuple
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# ------------ Retry yardımcıları (429 için bekle-tekrar dene) ------------
def _retry(fn, *args, **kwargs):
    max_try = 5
    for i in range(max_try):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            msg = str(e)
            if "Quota exceeded" in msg or "429" in msg:
                wait = (2 ** i) + random.uniform(0, 0.5)
                print(f"[Retry] 429/Quota, deneme {i+1}/{max_try}, bekle={wait:.2f}s")
                time.sleep(wait)
                continue
            raise

def safe_get_values(ws: gspread.Worksheet, rng: str):
    return _retry(ws.get_values, rng)

def safe_update(ws: gspread.Worksheet, rng: str, values):
    return _retry(ws.update, rng, values)

def safe_update_cell(ws: gspread.Worksheet, row: int, col: int, value):
    return _retry(ws.update_cell, row, col, value)

def safe_append_row(ws: gspread.Worksheet, row_vals):
    return _retry(ws.append_row, row_vals, value_input_option="USER_ENTERED")
# -------------------------------------------------------------------------

def get_gspread_client() -> gspread.Client:
    svc_json = os.environ.get("SVC_JSON")
    if not svc_json:
        raise RuntimeError("SVC_JSON env değişkeni yok.")
    info = json.loads(svc_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)

def open_sheet_by_id(client: gspread.Client, sheet_id: str) -> gspread.Spreadsheet:
    if not sheet_id:
        raise RuntimeError("Sheet ID boş.")
    return client.open_by_key(sheet_id)

def get_or_create_worksheet(ss: gspread.Spreadsheet, title: str, rows: int = 200, cols: int = 26) -> gspread.Worksheet:
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=str(rows), cols=str(cols))

def read_headers(ws: gspread.Worksheet) -> List[str]:
    vals = safe_get_values(ws, "1:1")
    return [h.strip() for h in vals[0]] if vals else []

def ensure_header(ws: gspread.Worksheet, headers: List[str]) -> List[str]:
    cur = read_headers(ws)
    if not cur:
        safe_update(ws, "1:1", [headers])
        return headers
    merged = cur[:]
    for h in headers:
        if h not in merged:
            merged.append(h)
    if merged != cur:
        safe_update(ws, "1:1", [merged])
    return merged

def ensure_column(ws: gspread.Worksheet, col_name: str) -> int:
    hdr = read_headers(ws)
    if col_name in hdr:
        return hdr.index(col_name) + 1
    hdr.append(col_name)
    safe_update(ws, "1:1", [hdr])
    return len(hdr)

def get_all_records_with_row(ws: gspread.Worksheet) -> Tuple[List[Dict], List[str]]:
    """PersonelListesi için kullanıyoruz (küçük tablo)."""
    hdr = read_headers(ws)
    if not hdr:
        return [], []
    all_vals = safe_get_values(ws, "2:10000")
    out: List[Dict] = []
    for i, row in enumerate(all_vals, start=2):
        if not any(row):
            continue
        rec = {hdr[j]: (row[j] if j < len(hdr) and j < len(row) else "") for j in range(len(hdr))}
        rec["_row"] = i
        out.append(rec)
    return out, hdr
