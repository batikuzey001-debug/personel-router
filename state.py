# state.py
import gspread
from sheets import get_or_create_worksheet, ensure_header

STATE_SHEET = "_State"
STATE_HEADERS = ["LogName", "LastRow"]

def ensure_state(ss: gspread.Spreadsheet):
    ws = get_or_create_worksheet(ss, STATE_SHEET, rows=50, cols=5)
    ensure_header(ws, STATE_HEADERS)
    return ws

def get_last_row(ss: gspread.Spreadsheet, log_name: str) -> int:
    ws = ensure_state(ss)
    vals = ws.get_all_values()
    for i, row in enumerate(vals[1:], start=2):
        if row and len(row) > 0 and row[0] == log_name:
            try:
                return int(row[1])
            except:
                return 1
    return 1

def set_last_row(ss: gspread.Spreadsheet, log_name: str, last_row: int):
    ws = ensure_state(ss)
    vals = ws.get_all_values()
    # ara / varsa güncelle
    for i, row in enumerate(vals[1:], start=2):
        if row and len(row) > 0 and row[0] == log_name:
            ws.update(f"B{i}", [[str(last_row)]])
            return
    # yoksa ekle
    ws.append_row([log_name, str(last_row)], value_input_option="USER_ENTERED")
