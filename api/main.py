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
from datetime import date, datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, func
from apscheduler.schedulers.background import BackgroundScheduler

from db import SessionLocal, TABLE_MODELS, GenerationHourly, InstalledCapacity, PtfHourly, SmfHourly, init_db

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

# Türkiye kalıcı olarak UTC+3 (yaz/kış saati yok). Veritabanı zamanları UTC'ye
# normalize edilmiş döndürdüğü için, API'nin gösterdiği tüm saatler ve "bugün"/
# "son 24 saat" pencereleri İstanbul saatine göre hesaplanır.
try:
    from zoneinfo import ZoneInfo
    TR_TZ = ZoneInfo("Europe/Istanbul")
except Exception:  # tzdata yoksa sabit UTC+3 yeterli
    TR_TZ = timezone(timedelta(hours=3))


# Net elektrik alışverişi (ithalat-ihracat) bir "üretim kaynağı" değildir ve negatif
# olabilir; kaynak bazlı üretim toplamlarına ve grafiklerine dahil edilmez.
_NON_GEN_SOURCES = ("importexport", "uluslararasi")


def _gen_only():
    return func.lower(GenerationHourly.source).notin_(_NON_GEN_SOURCES)


def _now_tr() -> datetime:
    return datetime.now(TR_TZ)


def _day_start(d: date) -> datetime:
    """İstanbul saatine göre bir günün başlangıcı (saat dilimli)."""
    return datetime(d.year, d.month, d.day, tzinfo=TR_TZ)


def _tr(dt: datetime) -> datetime:
    """Veritabanından gelen (UTC'ye normalize edilmiş) zamanı Türkiye saatine çevirir."""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(TR_TZ)


def _fmt(dt: datetime) -> str:
    return _tr(dt).strftime("%Y-%m-%d %H:%M")


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
            .where(model.dt >= _day_start(start), model.dt < _day_start(end) + timedelta(days=1))
            .order_by(model.dt)
        ).all()
    return {
        "hours": [_fmt(r.dt) for r in rows],
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
        "hours": [_fmt(r.dt) for r in rows],
        "values": [float(r.value) for r in rows],
    }


@app.get("/ptf/last24h")
def ptf_last24h():
    """Son 24 saatin PTF'si (takvim gününe değil, şu ana göre kayan pencere)."""
    end = _now_tr()
    start = end - timedelta(hours=24)
    return _series_range("ptf_hourly", start, end)


@app.get("/smf/last24h")
def smf_last24h():
    """Son 24 saatin SMF'si (takvim gününe değil, şu ana göre kayan pencere)."""
    end = _now_tr()
    start = end - timedelta(hours=24)
    return _series_range("smf_hourly", start, end)


@app.get("/generation/last24h")
def generation_last24h():
    """Son 24 saatin kaynak bazında üretimi: {hours, series: {kaynak_adi: [değerler]}}."""
    end = _now_tr()
    start = end - timedelta(hours=24)
    with SessionLocal() as session:
        rows = session.execute(
            select(GenerationHourly.dt, GenerationHourly.source, GenerationHourly.value_mw)
            .where(GenerationHourly.dt >= start, GenerationHourly.dt < end, _gen_only())
            .order_by(GenerationHourly.dt)
        ).all()
    hours_set = sorted({_fmt(r.dt) for r in rows})
    hour_index = {h: i for i, h in enumerate(hours_set)}
    series: dict[str, list] = defaultdict(lambda: [None] * len(hours_set))
    for r in rows:
        h = _fmt(r.dt)
        series[r.source][hour_index[h]] = float(r.value_mw)
    return {"hours": hours_set, "series": series}


@app.get("/generation/last24h-total")
def generation_last24h_total():
    """Son 24 saatte tüm kaynakların toplam üretimi (yaklaşık MWh)."""
    end = _now_tr()
    start = end - timedelta(hours=24)
    with SessionLocal() as session:
        total = session.execute(
            select(func.sum(GenerationHourly.value_mw))
            .where(GenerationHourly.dt >= start, GenerationHourly.dt < end, _gen_only())
        ).scalar()
    return {
        "from": _fmt(start),
        "to": _fmt(end),
        "total_mwh_approx": float(total) if total is not None else None,
    }


@app.get("/generation/today")
def generation_today():
    """Bugünün saatlik üretim karışımı: {hours, series: {kaynak_adi: [değerler]}}."""
    d = _now_tr().date()
    with SessionLocal() as session:
        rows = session.execute(
            select(GenerationHourly.dt, GenerationHourly.source, GenerationHourly.value_mw)
            .where(GenerationHourly.dt >= _day_start(d), GenerationHourly.dt < _day_start(d) + timedelta(days=1), _gen_only())
            .order_by(GenerationHourly.dt)
        ).all()
    hours_set = sorted({_fmt(r.dt) for r in rows})
    hour_index = {h: i for i, h in enumerate(hours_set)}
    series: dict[str, list] = defaultdict(lambda: [None] * len(hours_set))
    for r in rows:
        h = _fmt(r.dt)
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
    return {"dt": _fmt(latest_dt), "mix": {r.source: float(r.value_mw) for r in rows}}


@app.get("/capacity/latest")
def capacity_latest():
    """
    Kaynak bazında en güncel KURULU GÜÇ (MW) — EPİAŞ Şeffaflık Platformu "Kurulu Güç"
    raporundan. {period: "YYYY-MM", mix: {kaynak: MW}}.
    """
    with SessionLocal() as session:
        latest_dt = session.execute(select(func.max(InstalledCapacity.dt))).scalar()
        if latest_dt is None:
            return {"period": None, "mix": {}}
        rows = session.execute(
            select(InstalledCapacity.source, InstalledCapacity.value_mw).where(InstalledCapacity.dt == latest_dt)
        ).all()
    return {"period": _tr(latest_dt).strftime("%Y-%m"), "mix": {r.source: float(r.value_mw) for r in rows}}


@app.get("/ptf/hourly-profile")
def ptf_hourly_profile(days: int = 30):
    """
    Son `days` günün PTF verisini saat-of-day'e göre gruplar: her saat dilimi
    (00, 01, ..., 23) için ortalama, en düşük ve en yüksek fiyat. 'Tipik
    günlük fiyat eğrisi' — batarya arbitrajı/yatırım kararları için faydalı.
    """
    end = _now_tr()
    start = end - timedelta(days=days)
    with SessionLocal() as session:
        rows = session.execute(
            select(PtfHourly.dt, PtfHourly.value)
            .where(PtfHourly.dt >= start, PtfHourly.dt < end)
        ).all()
    buckets: dict[int, list] = defaultdict(list)
    for r in rows:
        buckets[_tr(r.dt).hour].append(float(r.value))
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
    today = _now_tr().date()
    month_start = today.replace(day=1)
    with SessionLocal() as session:
        rows = session.execute(
            select(GenerationHourly.source, func.sum(GenerationHourly.value_mw))
            .where(GenerationHourly.dt >= _day_start(month_start), GenerationHourly.dt < _day_start(today) + timedelta(days=1), _gen_only())
            .group_by(GenerationHourly.source)
        ).all()
    mix = {source: float(total) for source, total in rows}
    return {"month": month_start.strftime("%Y-%m"), "mix": mix, "unit": "MWh (yaklaşık, saatlik MW toplamı)"}


@app.get("/generation/today-total")
def generation_today_total():
    """Bugün (gece yarısından şu ana kadar) tüm kaynakların toplam üretimi (yaklaşık MWh)."""
    d = _now_tr().date()
    with SessionLocal() as session:
        total = session.execute(
            select(func.sum(GenerationHourly.value_mw))
            .where(GenerationHourly.dt >= _day_start(d), GenerationHourly.dt < _day_start(d) + timedelta(days=1), _gen_only())
        ).scalar()
    return {"date": d.strftime("%Y-%m-%d"), "total_mwh_approx": float(total) if total is not None else None}


@app.get("/ptf-smf/annual")
def ptf_smf_annual(months: int = 12):
    """Son `months` ay için aylık ortalama PTF ve SMF — yıllık trend grafiği için."""
    with SessionLocal() as session:
        month_col = func.date_trunc("month", func.timezone("Europe/Istanbul", PtfHourly.dt))
        ptf_rows = session.execute(
            select(month_col.label("m"), func.avg(PtfHourly.value))
            .group_by("m").order_by("m")
        ).all()
        month_col_s = func.date_trunc("month", func.timezone("Europe/Istanbul", SmfHourly.dt))
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


@app.get("/status")
def status():
    """Teşhis: her tablodaki en son kayıt zamanı (İstanbul saati) ve satır sayısı."""
    out = {"now": _fmt(_now_tr())}
    with SessionLocal() as session:
        for name, model in (("ptf", PtfHourly), ("smf", SmfHourly)):
            latest = session.execute(select(func.max(model.dt))).scalar()
            count = session.execute(select(func.count()).select_from(model)).scalar()
            out[name] = {"latest": _fmt(latest) if latest else None, "rows": count}
        latest = session.execute(select(func.max(GenerationHourly.dt))).scalar()
        count = session.execute(select(func.count()).select_from(GenerationHourly)).scalar()
        sources = [r[0] for r in session.execute(select(GenerationHourly.source).distinct()).all()]
        out["generation"] = {"latest": _fmt(latest) if latest else None, "rows": count, "sources": sorted(sources)}
        cap_dt = session.execute(select(func.max(InstalledCapacity.dt))).scalar()
        cap_sources = [r[0] for r in session.execute(select(InstalledCapacity.source).distinct()).all()]
        out["capacity"] = {"period": _tr(cap_dt).strftime("%Y-%m") if cap_dt else None, "sources": sorted(cap_sources)}
    return out


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
    d = _now_tr().date()
    return _series(table, d, d)


@app.get("/{series}/history")
def history(series: str, days: int = 90):
    table = {"ptf": "ptf_hourly", "smf": "smf_hourly", "consumption": "consumption_hourly", "load-plan": "load_plan_hourly"}.get(series, series)
    if table not in TABLE_MODELS:
        raise HTTPException(404, "Bilinmeyen seri")
    end = _now_tr().date()
    start = end - timedelta(days=days)
    return _series(table, start, end)
