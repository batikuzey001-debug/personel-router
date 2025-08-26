import os, json
from typing import List, Dict, Tuple
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def get_gspread_client() -> gspread.Client:
    """Railway env'deki SVC_JSON ile gspread istemcisi döner."""
    svc_json = os.environ.get("SVC_JSON")
    if not svc_json:
        raise RuntimeError("SVC_JSON env değişkeni yok.")
    try:
        info = json.loads(svc_json)
    except json.JSONDecodeError as e:
        raise RuntimeError("SVC_JSON geçerli bir JSON değil.") from e
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
    vals = ws.get_values("1:1")
    return [h.strip() for h in vals[0]] if vals else []

def ensure_header(ws: gspread.Worksheet, headers: List[str]) -> List[str]:
    """Eksik başlıkları ekler, birleşik listeyi döner."""
    cur = read_headers(ws)
    if not cur:
        ws.update("1:1", [headers])
        return headers
    merged = cur[:]
    for h in headers:
        if h not in merged:
            merged.append(h)
    if merged != cur:
        ws.update("1:1", [merged])
    return merged

def ensure_column(ws: gspread.Worksheet, col_name: str) -> int:
    """Kolon yoksa ekler, 1-bazlı kolon indexi döner."""
    hdr = read_headers(ws)
    if col_name in hdr:
        return hdr.index(col_name) + 1
    hdr.append(col_name)
    ws.update("1:1", [hdr])
    return len(hdr)

def get_all_records_with_row(ws: gspread.Worksheet) -> Tuple[List[Dict], List[str]]:
    """Header'a göre satırları dict listesi olarak döner, her dict'e `_row` ekler."""
    hdr = read_headers(ws)
    if not hdr:
        return [], []
    all_vals = ws.get_all_values()
    out: List[Dict] = []
    for i, row in enumerate(all_vals[1:], start=2):
        rec = {hdr[j]: (row[j] if j < len(row) else "") for j in range(len(hdr))}
        rec["_row"] = i
        out.append(rec)
    return out, hdr
