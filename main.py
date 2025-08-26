import os, time, traceback, sys, random
from router import route_once

def main():
    print("ARES PHYTON • Personel Router çalışıyor 🚀")
    interval = int(os.getenv("ROUTER_INTERVAL_SECONDS", "30"))
    start_offset = int(os.getenv("START_OFFSET_SECONDS", "0"))
    jitter = int(os.getenv("JITTER_SECONDS", "0"))

    # Başlangıç kaydırma: diğer scriptlerle çakışmayı azalt
    if start_offset > 0:
        print(f"Başlangıç bekleme: {start_offset}s")
        time.sleep(start_offset)

    while True:
        try:
            res = route_once()
            print("Özet:", ", ".join(f"{k}:{v}" for k,v in res.items()))
        except Exception as e:
            print("HATA:", e)
            traceback.print_exc(file=sys.stdout)

        # Her turda küçük rastgele bekleme (jitter)
        extra = random.uniform(0, jitter) if jitter > 0 else 0
        time.sleep(interval + extra)

if __name__ == "__main__":
    main()
