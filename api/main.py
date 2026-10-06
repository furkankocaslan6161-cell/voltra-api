"""
Voltra API — ETL'in veritabanına yazdığı veriyi frontend'e sunar.

Çalıştırma:
    uvicorn api.main:app --reload --port 8000

voltra-prototip.html içindeki Chart.js grafikleri şu an sabit (sentetik)
diziler kullanıyor. Gerçek entegrasyonda, o script bloklarındaki `actual`,
`forecast`, `historyData` dizilerini burada tanımlı endpoint'lerden
fetch() ile doldurmanız yeterli. Örnek:

    const res = await fetch('https://api.sizin-domaininiz.com/ptf/today');
    const { hours, values } = await res.json();
"""

from __future__ import annotations
import logging
import threading
from collections import defaultdict
from datetime import date, datetime, timedelta
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, func
from apscheduler.schedulers.background import BackgroundScheduler

from db import SessionLocal, TABLE_MODELS, GenerationHourly, CapacityHourly, PtfHourly, SmfHourly, init_db

log = logging.getLogger("voltra-api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = FastAPI(title="Voltra API")

# Prod'da allow_origins'i kendi domaininizle sınırlayın.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

scheduler = BackgroundScheduler()


def _run_etl_safely():
    """ETL'i çalıştırır; hata olursa API'yi düşürmeden sadece loglar."""
    try:
        from etl.run_etl import run as run_etl
        run_etl()
    except Exception:
        log.exception("Zamanlanmış ETL çalıştırması başarısız oldu")


def _run_fast_refresh_safely():
    """15 dakikalık hafif yenilemeyi çalıştırır; hata olursa sadece loglar."""
    try:
        from etl.run_etl import run_fast_refresh
        run_fast_refresh()
    except Exception:
        log.exception("Hızlı yenileme başarısız oldu")


@app.on_event("startup")
def on_startup():
    init_db()
    # İlk veriyi hemen çek (sunucunun açılışını bloklamasın diye arka planda).
    threading.Thread(target=_run_etl_safely, daemon=True).start()
    # Tam ETL (üretim planı + geçmiş günler) saatte bir.
    scheduler.add_job(_run_etl_safely, "interval", hours=1, id="voltra-etl-hourly")
    # Sadece bugünün PTF/SMF/üretim karışımı 15 dakikada bir.
    scheduler.add_job(_run_fast_refresh_safely, "interval", minutes=15, id="voltra-etl-fast")
    scheduler.start()


@app.on_event("shutdown")
def on_shutdown():
    scheduler.shutdown(wait=False)


def _series(table_name: str, start: date, end: date):
    model = TABLE_MODELS[table_name]
    with SessionLocal() as session:
        rows = session.execute(
            select(model.dt, model.value)
            .where(model.dt >= start, model.dt < end + timedelta(days=1))
            .order_by(model.dt)
        ).all()
    return {
        "hours": [r.dt.strftime("%Y-%m-%d %H:%M") for r in rows],
        "values": [float(r.value) for r in rows],
    }


def _series_range(table_name: str, start_dt: datetime, end_dt: datetime):
    model = TABLE_MODELS[table_name]
    with SessionLocal() as session:
        rows = session.execute(
            select(model.dt, model.value)
            .where(model.dt >= start_dt, model.dt < end_dt)
            .order_by(model.dt)
        ).all()
    return {
        "hours": [r.dt.strftime("%Y-%m-%d %H:%M") for r in rows],
        "values": [float(r.value) for r in rows],
    }


@app.get("/ptf/last24h")
def ptf_last24h():
    """Son 24 saatin PTF'si (takvim gününe değil, şu ana göre kayan pencere)."""
    end = datetime.utcnow()
    start = end - timedelta(hours=24)
    return _series_range("ptf_hourly", start, end)


@app.get("/smf/last24h")
def smf_last24h():
    """Son 24 saatin SMF'si (takvim gününe değil, şu ana göre kayan pencere)."""
    end = datetime.utcnow()
    start = end - timedelta(hours=24)
    return _series_range("smf_hourly", start, end)


@app.get("/generation/last24h")
def generation_last24h():
    """Son 24 saatin kaynak bazında üretimi: {hours, series: {kaynak_adi: [değerler]}}."""
    end = datetime.utcnow()
    start = end - timedelta(hours=24)
    with SessionLocal() as session:
        rows = session.execute(
            select(GenerationHourly.dt, GenerationHourly.source, GenerationHourly.value_mw)
            .where(GenerationHourly.dt >= start, GenerationHourly.dt < end)
            .order_by(GenerationHourly.dt)
        ).all()
    hours_set = sorted({r.dt.strftime("%Y-%m-%d %H:%M") for r in rows})
    hour_index = {h: i for i, h in enumerate(hours_set)}
    series: dict[str, list] = defaultdict(lambda: [None] * len(hours_set))
    for r in rows:
        h = r.dt.strftime("%Y-%m-%d %H:%M")
        series[r.source][hour_index[h]] = float(r.value_mw)
    return {"hours": hours_set, "series": series}


@app.get("/generation/last24h-total")
def generation_last24h_total():
    """Son 24 saatte tüm kaynakların toplam üretimi (yaklaşık MWh)."""
    end = datetime.utcnow()
    start = end - timedelta(hours=24)
    with SessionLocal() as session:
        total = session.execute(
            select(func.sum(GenerationHourly.value_mw))
            .where(GenerationHourly.dt >= start, GenerationHourly.dt < end)
        ).scalar()
    return {
        "from": start.strftime("%Y-%m-%d %H:%M"),
        "to": end.strftime("%Y-%m-%d %H:%M"),
        "total_mwh_approx": float(total) if total is not None else None,
    }


@app.get("/generation/today")
def generation_today():
    """Bugünün saatlik üretim karışımı: {hours, series: {kaynak_adi: [değerler]}}."""
    d = date.today()
    with SessionLocal() as session:
        rows = session.execute(
            select(GenerationHourly.dt, GenerationHourly.source, GenerationHourly.value_mw)
            .where(GenerationHourly.dt >= d, GenerationHourly.dt < d + timedelta(days=1))
            .order_by(GenerationHourly.dt)
        ).all()
    hours_set = sorted({r.dt.strftime("%Y-%m-%d %H:%M") for r in rows})
    hour_index = {h: i for i, h in enumerate(hours_set)}
    series: dict[str, list] = defaultdict(lambda: [None] * len(hours_set))
    for r in rows:
        h = r.dt.strftime("%Y-%m-%d %H:%M")
        series[r.source][hour_index[h]] = float(r.value_mw)
    return {"hours": hours_set, "series": series}


@app.get("/generation/latest")
def generation_latest():
    """Her kaynak için en son saate ait tek değer (iç kullanım / ileride lazım olursa)."""
    with SessionLocal() as session:
        latest_dt = session.execute(select(func.max(GenerationHourly.dt))).scalar()
        if latest_dt is None:
            return {"dt": None, "mix": {}}
        rows = session.execute(
            select(GenerationHourly.source, GenerationHourly.value_mw).where(GenerationHourly.dt == latest_dt)
        ).all()
    return {"dt": latest_dt.strftime("%Y-%m-%d %H:%M"), "mix": {r.source: float(r.value_mw) for r in rows}}


@app.get("/capacity/latest")
def capacity_latest():
    """
    Kaynak bazında en güncel Emre Amade Kapasite (EAK) — 'Kurulu Güç' kartı için.
    EPİAŞ'ın gerçek zamanlı API'sinde kaynak bazında statik/lisanslı kurulu güç
    raporu bulunmuyor; EAK (sisteme o an sağlanabilecek aktif kapasite) en
    yakın ve kaynak kırılımı olan veridir.
    """
    with SessionLocal() as session:
        latest_dt = session.execute(select(func.max(CapacityHourly.dt))).scalar()
        if latest_dt is None:
            return {"dt": None, "mix": {}}
        rows = session.execute(
            select(CapacityHourly.source, CapacityHourly.value_mw).where(CapacityHourly.dt == latest_dt)
        ).all()
    return {"dt": latest_dt.strftime("%Y-%m-%d %H:%M"), "mix": {r.source: float(r.value_mw) for r in rows}}


@app.get("/ptf/hourly-profile")
def ptf_hourly_profile(days: int = 30):
    """
    Son `days` günün PTF verisini saat-of-day'e göre gruplar: her saat dilimi
    (00, 01, ..., 23) için ortalama, en düşük ve en yüksek fiyat. 'Tipik
    günlük fiyat eğrisi' — batarya arbitrajı/yatırım kararları için faydalı.
    """
    end = datetime.utcnow()
    start = end - timedelta(days=days)
    with SessionLocal() as session:
        rows = session.execute(
            select(PtfHourly.dt, PtfHourly.value)
            .where(PtfHourly.dt >= start, PtfHourly.dt < end)
        ).all()
    buckets: dict[int, list] = defaultdict(list)
    for r in rows:
        buckets[r.dt.hour].append(float(r.value))
    hours = list(range(24))
    avg = [sum(buckets[h]) / len(buckets[h]) if buckets[h] else None for h in hours]
    lo = [min(buckets[h]) if buckets[h] else None for h in hours]
    hi = [max(buckets[h]) if buckets[h] else None for h in hours]
    return {"hours": hours, "avg": avg, "min": lo, "max": hi, "days": days}


@app.get("/generation/month")
def generation_month():
    """
    İçinde bulunulan ayın başından bugüne, kaynak bazında TOPLAM üretim
    (saatlik MW değerlerinin toplamı — yaklaşık MWh). 'Mevcut ayın üretimleri
    kaynaklar bazında' pasta/donut grafiği için.
    """
    today = date.today()
    month_start = today.replace(day=1)
    with SessionLocal() as session:
        rows = session.execute(
            select(GenerationHourly.source, func.sum(GenerationHourly.value_mw))
            .where(GenerationHourly.dt >= month_start, GenerationHourly.dt < today + timedelta(days=1))
            .group_by(GenerationHourly.source)
        ).all()
    mix = {source: float(total) for source, total in rows}
    return {"month": month_start.strftime("%Y-%m"), "mix": mix, "unit": "MWh (yaklaşık, saatlik MW toplamı)"}


@app.get("/generation/today-total")
def generation_today_total():
    """Bugün (gece yarısından şu ana kadar) tüm kaynakların toplam üretimi (yaklaşık MWh)."""
    d = date.today()
    with SessionLocal() as session:
        total = session.execute(
            select(func.sum(GenerationHourly.value_mw))
            .where(GenerationHourly.dt >= d, GenerationHourly.dt < d + timedelta(days=1))
        ).scalar()
    return {"date": d.strftime("%Y-%m-%d"), "total_mwh_approx": float(total) if total is not None else None}


@app.get("/ptf-smf/annual")
def ptf_smf_annual(months: int = 12):
    """Son `months` ay için aylık ortalama PTF ve SMF — yıllık trend grafiği için."""
    with SessionLocal() as session:
        month_col = func.date_trunc("month", PtfHourly.dt)
        ptf_rows = session.execute(
            select(month_col.label("m"), func.avg(PtfHourly.value))
            .group_by("m").order_by("m")
        ).all()
        month_col_s = func.date_trunc("month", SmfHourly.dt)
        smf_rows = session.execute(
            select(month_col_s.label("m"), func.avg(SmfHourly.value))
            .group_by("m").order_by("m")
        ).all()
    ptf_by_month = {m.strftime("%Y-%m"): float(v) for m, v in ptf_rows}
    smf_by_month = {m.strftime("%Y-%m"): float(v) for m, v in smf_rows}
    all_months = sorted(set(ptf_by_month) | set(smf_by_month))[-months:]
    return {
        "months": all_months,
        "ptf_avg": [ptf_by_month.get(m) for m in all_months],
        "smf_avg": [smf_by_month.get(m) for m in all_months],
    }


@app.get("/health")
def health():
    return {"status": "ok"}


# NOT: Bu iki rota, {series} parametresiyle HER ŞEYİ yakalayabildiği için
# dosyanın EN SONUNDA tanımlanmalı. FastAPI rotaları tanım sırasına göre
# eşleştirir; bu rotalar yukarıdaki özel (/generation/..., /ptf-smf/...,
# /health) rotalardan önce tanımlanırsa, örneğin "/generation/today"
# isteği buraya "series=generation" olarak düşer ve yanlışlıkla
# "Bilinmeyen seri" hatası döner.
@app.get("/{series}/today")
def today(series: str):
    table = {"ptf": "ptf_hourly", "smf": "smf_hourly", "consumption": "consumption_hourly", "load-plan": "load_plan_hourly"}.get(series, series)
    if table not in TABLE_MODELS:
        raise HTTPException(404, "Bilinmeyen seri")
    d = date.today()
    return _series(table, d, d)


@app.get("/{series}/history")
def history(series: str, days: int = 90):
    table = {"ptf": "ptf_hourly", "smf": "smf_hourly", "consumption": "consumption_hourly", "load-plan": "load_plan_hourly"}.get(series, series)
    if table not in TABLE_MODELS:
        raise HTTPException(404, "Bilinmeyen seri")
    end = date.today()
    start = end - timedelta(days=days)
    return _series(table, start, end)
