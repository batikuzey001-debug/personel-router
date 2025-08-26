import os, json
from typing import Dict
import gspread

from sheets import (get_gspread_client, open_sheet_by_id,
                    ensure_column, read_headers)
from persons import ensure_person_list, find_or_create_person, ensure_person_page
from utils import now_str
from state import get_last_row, set_last_row

LOG_SHEETS = ["MesaiLog","BonusLog","FinansLog"]
WINDOW = 200  # sadece son 200 satırı oku

def pick_time(rec: Dict) -> str:
    for k in ["LogTs","TalepTs","CloseTs","BildirimTarih","FirstTs","Ts","Zaman"]:
        if rec.get(k): return rec[k]
    return ""

def extract_name(sheet_name: str, rec: Dict) -> str:
    if sheet_name == "MesaiLog" and rec.get("Kisi"):
        return str(rec["Kisi"]).strip()
    for k in ["ClosedByFull","FirstByFull"]:
        if rec.get(k):
            val = str(rec[k])
            return val.split("|")[0].split("/")[0].strip()
    for k in ["AdSoyad","Ad","UserName"]:
        if rec.get(k): return str(rec[k]).strip()
    return ""

def make_source_key(sheet_name: str, rec: Dict) -> str:
    for k in ["MsgID","OrigMsgID","CloseMsgID","FirstMsgID","ID"]:
        if rec.get(k): return f"{sheet_name}:{rec[k]}"
    return f"{sheet_name}:row{rec.get('_row','?')}"

def ensure_processed_cols(ws: gspread.Worksheet):
    pcol = ensure_column(ws, "ProcessedAt")
    rcol = ensure_column(ws, "RoutedTo")
    return pcol, rcol

def iter_rows(ws: gspread.Worksheet, start_row: int):
    """start_row'dan itibaren satırları (headere göre) yield eder.
       Sadece son WINDOW satırı okunur."""
    header_vals = ws.get_values("1:1")
    header = [h.strip() for h in header_vals[0]] if header_vals else []
    if not header:
        return
    last = ws.row_count  # tahsisli satır sayısı (çoğu zaman yeter)
    start = max(2, min(start_row, last))  # en az satır-2 (data başlangıcı)
    # pencere uygula:
    start = max(2, max(start, last - WINDOW + 1))
    rng = f"{start}:{last}"
    rows = ws.get_values(rng)  # sadece bu aralığı çek
    for i, row in enumerate(rows, start=start):
        rec = {header[j]: (row[j] if j < len(row) else "") for j in range(len(header))}
        rec["_row"] = i
        yield rec, header

def route_once() -> Dict[str, int]:
    client = get_gspread_client()
    src_id = os.environ["SOURCE_SHEET_ID"]
    dst_id = os.environ["TARGET_SHEET_ID"]

    src = open_sheet_by_id(client, src_id)
    dst = open_sheet_by_id(client, dst_id)
    ensure_person_list(dst)

    results: Dict[str,int] = {}

    for name in LOG_SHEETS:
        try:
            ws = src.worksheet(name)
        except gspread.WorksheetNotFound:
            results[name] = 0
            continue

        pcol, rcol = ensure_processed_cols(ws)
        last_done = get_last_row(dst, name)

        processed = 0
        max_row_seen = last_done

        for rec, header in iter_rows(ws, last_done + 1):
            max_row_seen = max(max_row_seen, rec["_row"])

            # Zaten işlenmişse atla
            if str(rec.get("ProcessedAt","")).strip():
                continue

            kisi = extract_name(name, rec)
            if not kisi:
                continue

            pid, durum = find_or_create_person(dst, kisi)
            if durum.lower().startswith("pasif"):
                ws.update_cell(rec["_row"], pcol, now_str())
                ws.update_cell(rec["_row"], rcol, "PASIF")
                processed += 1
                continue

            pws = ensure_person_page(dst, pid)
            source_key = make_source_key(name, rec)
            zaman = pick_time(rec)
            raw = {k:v for k,v in rec.items() if k!="_row"}

            # Kişi sayfasına ekle
            hdr = read_headers(pws)
            row = [None]*len(hdr)
            def setv(col, val):
                if col in hdr: row[hdr.index(col)] = val
            setv("Kaynak", name.replace("Log",""))
            setv("SourceKey", source_key)
            setv("Zaman", zaman)
            setv("AlanlarJSON", json.dumps(raw, ensure_ascii=False))
            setv("ProcessedAt", now_str())
            pws.append_row(row, value_input_option="USER_ENTERED")

            # Kaynak satırı işaretle
            ws.update_cell(rec["_row"], pcol, now_str())
            ws.update_cell(rec["_row"], rcol, pid)
            processed += 1

        # State'i güncelle
        set_last_row(dst, name, max_row_seen)
        results[name] = processed

    return results
