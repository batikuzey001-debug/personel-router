import os, time, traceback
from router import route_once

def main():
    print("ARES PHYTON • Personel Router çalışıyor 🚀")
    interval = int(os.getenv("ROUTER_INTERVAL_SECONDS", "15"))
    while True:
        try:
            res = route_once()
            print("Özet:", ", ".join(f"{k}:{v}" for k,v in res.items()))
        except Exception as e:
            print("HATA:", e)
            traceback.print_exc()   # hata detayını göster
        time.sleep(interval)

if __name__ == "__main__":
    main()
