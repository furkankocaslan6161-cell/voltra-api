"""
Voltra ETL — tek seferlik çalıştırma girişi.

Kullanım:
    python -m etl.run_etl

Cron ile saatlik çalıştırma örneği (crontab -e):
    5 * * * * cd /path/to/voltra-etl && /path/to/venv/bin/python -m etl.run_etl >> etl.log 2>&1

Her çalıştırmada bugünü ve ETL_LOOKBACK_DAYS kadar geriye giden günleri yeniden
çeker — EPİAŞ verileri zaman zaman revize edildiği için sadece "yeni" veriyi
değil, yakın geçmişi de tazelemek güvenlidir. upsert kullanıldığından
tekrar çekilen günler veritabanında çoğalmaz, sadece güncellenir.
"""

from __future__ import annotations
import sys
import logging
from datetime import date, timedelta

sys.path.append(".")  # etl/ altından da çalıştırılabilsin diye

from config import DATA_SOURCES, TRACKED_PLANTS, ETL_LOOKBACK_DAYS
from db import init_db, upsert_hourly, upsert_production
from etl.fetch import get_client, fetch_series, fetch_production_plan

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("voltra-etl")


def run():
    init_db()
    eptr = get_client()

    end = date.today()
    start = end - timedelta(days=ETL_LOOKBACK_DAYS)
    start_s, end_s = start.isoformat(), end.isoformat()
    log.info("ETL çalışıyor: %s -> %s", start_s, end_s)

    for name, cfg in DATA_SOURCES.items():
        try:
            rows = fetch_series(eptr, cfg["call"], start_s, end_s)
            n = upsert_hourly(cfg["table"], rows)
            log.info("  %-12s -> %-20s : %d satır", name, cfg["table"], n)
        except Exception as e:
            log.error("  %-12s BAŞARISIZ: %s", name, e)

    for plant_name, uevcb_id in TRACKED_PLANTS.items():
        try:
            rows = fetch_production_plan(eptr, uevcb_id, start_s, end_s)
            n = upsert_production(rows)
            log.info("  üretim planı %-20s -> %d satır", plant_name, n)
        except Exception as e:
            log.error("  üretim planı %s BAŞARISIZ: %s", plant_name, e)

    log.info("ETL tamamlandı.")


if __name__ == "__main__":
    run()
