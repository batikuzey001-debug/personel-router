import os, re
from datetime import datetime
import pytz

# Türkçe karakterleri sadeleştir
TR_MAP = str.maketrans({
    "ç":"c","ğ":"g","ı":"i","ö":"o","ş":"s","ü":"u",
    "Ç":"c","Ğ":"g","İ":"i","Ö":"o","Ş":"s","Ü":"u"
})

def normalize_name(name: str) -> str:
    """İsimleri karşılaştırmak için normalize et (küçük, TR sade, boşluksuz)."""
    if not name: return ""
    s = name.translate(TR_MAP).lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

def now_str() -> str:
    """Saat dilimine göre zaman metni."""
    tz = pytz.timezone(os.getenv("TIMEZONE", "Europe/Istanbul"))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def next_person_id(existing_ids) -> str:
    """
    RD-001, RD-002 ... devamı.
    existing_ids: mevcut PersonelID listesi
    """
    max_n = 0
    for pid in existing_ids:
        m = re.match(r"^RD-(\d{3})$", str(pid).strip())
        if m:
            n = int(m.group(1))
            if n > max_n: max_n = n
    return f"RD-{max_n+1:03d}"
