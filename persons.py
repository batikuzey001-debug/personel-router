from typing import Dict, Tuple
import gspread
from sheets import get_or_create_worksheet, ensure_header, get_all_records_with_row
from utils import normalize_name, now_str, next_person_id

# Personel listesi sayfası
PERSON_LIST_SHEET = "PersonelListesi"
PERSON_LIST_HEADERS = [
    "PersonelID","AdSoyad","NormName","Durum","BaslamaTarihi",
    "CikisTarihi","Olusturuldu","Guncellendi"
]

# Kişisel sayfa başlıkları
PERSON_PAGE_HEADERS = ["Kaynak","SourceKey","Zaman","AlanlarJSON","ProcessedAt"]

def ensure_person_list(ss: gspread.Spreadsheet) -> gspread.Worksheet:
    ws = get_or_create_worksheet(ss, PERSON_LIST_SHEET, rows=1000, cols=12)
    ensure_header(ws, PERSON_LIST_HEADERS)
    return ws

def load_person_index(ws: gspread.Worksheet) -> Tuple[Dict[str, Dict], Dict[str, Dict], list]:
    rows, hdr = get_all_records_with_row(ws)
    by_norm = {}
    by_id = {}
    ids = []
    for r in rows:
        pid = r.get("PersonelID","").strip()
        name = r.get("AdSoyad","").strip()
        norm = r.get("NormName","").strip() or normalize_name(name)
        status = (r.get("Durum","Aktif") or "Aktif").strip()
        by_norm[norm] = {"id":pid, "row":r["_row"], "durum":status, "name":name}
        if pid:
            by_id[pid] = {"row":r["_row"], "durum":status, "name":name, "norm":norm}
            ids.append(pid)
    return by_norm, by_id, ids

def find_or_create_person(ss: gspread.Spreadsheet, name: str) -> Tuple[str, str]:
    """İsimden kişiyi bulur ya da oluşturur. Dönen: (PersonelID, Durum)."""
    ws = ensure_person_list(ss)
    by_norm, by_id, ids = load_person_index(ws)
    norm = normalize_name(name)

    if norm in by_norm and by_norm[norm].get("id"):
        info = by_norm[norm]
        row = info["row"]
        # Ad güncelle
        ws.update(f"B{row}:D{row}", [[name, norm, info["durum"]]])
        ws.update_cell(row, PERSON_LIST_HEADERS.index("Guncellendi")+1, now_str())
        return info["id"], info["durum"]

    # Yeni kişi
    new_id = next_person_id(ids)
    ws.append_row([
        new_id, name, norm, "Aktif", "", "", now_str(), now_str()
    ], value_input_option="USER_ENTERED")

    ensure_person_page(ss, new_id)  # kişisel sayfa
    return new_id, "Aktif"

def ensure_person_page(ss: gspread.Spreadsheet, person_id: str) -> gspread.Worksheet:
    pws = get_or_create_worksheet(ss, person_id, rows=1000, cols=10)
    ensure_header(pws, PERSON_PAGE_HEADERS)
    return pws
