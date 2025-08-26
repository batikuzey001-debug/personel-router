import os, json
from typing import Dict
import gspread

from sheets import (get_gspread_client, open_sheet_by_id,
                    ensure_column, get_all_records_with_row, read_headers)
from persons import ensure_person_list, find_or_create_person, ensure_person_page
from utils import now_str, normalize_name

# İşlenecek sayfa isimleri
LOG_SHEETS = ["MesaiLog","BonusLog","FinansLog"]

def pick_time(rec: Dict) -> str:
    for k in ["LogTs","TalepTs","CloseTs","BildirimTarih","FirstTs","Ts","Zaman"]:
        if rec.get(k): return rec[k]
    return ""

def extract_name(sheet_name: str, rec: Dict) -> str:
    if sheet_name == "MesaiLog":
        if rec.get("Kisi"): return str(rec["Kisi"]).strip()
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

def append_to_person(pws: gspread.Worksheet, source_key: str, zaman: str, kaynak: str, raw: Dict):
    hdr = read_headers(pws)
    row = [None]*len(hdr)

    def setv(name, val):
        if name in hdr:
            row[hdr.index(name)] = val

    setv("Kaynak", kaynak)
    setv("SourceKey", source_key)
    setv("Zaman", zaman)
    setv("AlanlarJSON", json.dumps(raw, ensure_ascii=False))
    setv("ProcessedAt", now_str())

    pws.append_row(row, value_input_option="USER_ENTERED")

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
        records, hdr = get_all_records_with_row(ws)
        done = 0

        for rec in records:
            if str(rec.get("ProcessedAt","")).strip():
                continue

            kisi = extract_name(name, rec)
            if not kisi:
                continue

            pid, durum = find_or_create_person(dst, kisi)

            if durum.lower().startswith("pasif"):
                ws.update_cell(rec["_row"], pcol, now_str())
                ws.update_cell(rec["_row"], rcol, "PASIF")
                done += 1
                continue

            pws = ensure_person_page(dst, pid)
            source_key = make_source_key(name, rec)
            zaman = pick_time(rec)
            raw = {k:v for k,v in rec.items() if k!="_row"}
            append_to_person(pws, source_key, zaman, name.replace("Log",""), raw)

            ws.update_cell(rec["_row"], pcol, now_str())
            ws.update_cell(rec["_row"], rcol, pid)
            done += 1

        results[name] = done
    return results
