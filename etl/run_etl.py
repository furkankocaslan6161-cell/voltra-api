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

from config import (
    DATA_SOURCES,
    TRACKED_PLANTS,
    ETL_LOOKBACK_DAYS,
    ANNUAL_BACKFILL_DAYS,
    FAST_REFRESH_SOURCES,
    GENERATION_CALL,
    GENERATION_TABLE,
    CAPACITY_CALL,
    CAPACITY_TABLE,
)
from db import init_db, upsert_hourly, upsert_production, upsert_generation, upsert_capacity, get_state, set_state
from etl.fetch import get_client, fetch_series, fetch_production_plan, fetch_generation_mix

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("voltra-etl")


def run():
    """Tam ETL: PTF/SMF/tüketim/yük-tahmini + üretim karışımı (son ETL_LOOKBACK_DAYS gün)
    + santral üretim planı + (ilk çalıştırmada) yıllık PTF/SMF geçmişi."""
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

    try:
        rows = fetch_generation_mix(eptr, GENERATION_CALL, start_s, end_s)
        n = upsert_generation(rows)
        log.info("  %-12s -> %-20s : %d satır", "generation", GENERATION_TABLE, n)
    except Exception as e:
        log.error("  generation BAŞARISIZ: %s", e)

    try:
        rows = fetch_generation_mix(eptr, CAPACITY_CALL, start_s, end_s)
        n = upsert_capacity(rows)
        log.info("  %-12s -> %-20s : %d satır", "capacity", CAPACITY_TABLE, n)
    except Exception as e:
        log.error("  capacity BAŞARISIZ: %s", e)

    for plant_name, uevcb_id in TRACKED_PLANTS.items():
        try:
            rows = fetch_production_plan(eptr, uevcb_id, start_s, end_s)
            n = upsert_production(rows)
            log.info("  üretim planı %-20s -> %d satır", plant_name, n)
        except Exception as e:
            log.error("  üretim planı %s BAŞARISIZ: %s", plant_name, e)

    _backfill_annual_once(eptr)

    log.info("ETL tamamlandı.")


def _backfill_annual_once(eptr):
    """
    "Yıllık PTF/SMF" grafiği için son ANNUAL_BACKFILL_DAYS günü BİR KEZ doldurur.
    etl_state tablosundaki bayrağa bakar, ikinci kez tekrar çekmez (EPİAŞ'a
    365 günlük sorguyu her saat tekrarlamamak için).
    """
    if get_state("annual_backfill_done") == "1":
        return
    log.info("Yıllık PTF/SMF geçmişi ilk kez dolduruluyor (%d gün) — bu birkaç dakika sürebilir...", ANNUAL_BACKFILL_DAYS)
    end = date.today()
    start = end - timedelta(days=ANNUAL_BACKFILL_DAYS)
    chunk_days = 60
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=chunk_days), end)
        for name, table in (("ptf", "ptf_hourly"), ("smf", "smf_hourly")):
            try:
                call_name = DATA_SOURCES[name]["call"]
                rows = fetch_series(eptr, call_name, cursor.isoformat(), chunk_end.isoformat())
                n = upsert_hourly(table, rows)
                log.info("  yıllık doldurma %-6s %s -> %s : %d satır", name, cursor, chunk_end, n)
            except Exception as e:
                log.error("  yıllık doldurma %s (%s -> %s) BAŞARISIZ: %s", name, cursor, chunk_end, e)
        cursor = chunk_end
    set_state("annual_backfill_done", "1")
    log.info("Yıllık PTF/SMF geçmişi dolduruldu.")


def run_fast_refresh():
    """
    15 dakikada bir çalışan HAFİF yenileme: sadece bugünü, sadece PTF/SMF ve
    üretim karışımını çeker — tam ETL'in aksine geçmiş günlere/santral
    planına dokunmaz. Amaç: fiyat ve üretim grafiklerinin gün içinde sık
    güncellenmesi.
    """
    init_db()
    eptr = get_client()
    today_s = date.today().isoformat()

    for name in FAST_REFRESH_SOURCES:
        cfg = DATA_SOURCES[name]
        try:
            rows = fetch_series(eptr, cfg["call"], today_s, today_s)
            n = upsert_hourly(cfg["table"], rows)
            log.info("  [hızlı] %-12s -> %-20s : %d satır", name, cfg["table"], n)
        except Exception as e:
            log.error("  [hızlı] %-12s BAŞARISIZ: %s", name, e)

    try:
        rows = fetch_generation_mix(eptr, GENERATION_CALL, today_s, today_s)
        n = upsert_generation(rows)
        log.info("  [hızlı] %-12s -> %-20s : %d satır", "generation", GENERATION_TABLE, n)
    except Exception as e:
        log.error("  [hızlı] generation BAŞARISIZ: %s", e)


if __name__ == "__main__":
    run()
