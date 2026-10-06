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
from datetime import date, timedelta
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select
from apscheduler.schedulers.background import BackgroundScheduler

from db import SessionLocal, TABLE_MODELS, init_db

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


@app.on_event("startup")
def on_startup():
    init_db()
    # İlk veriyi hemen çek (sunucunun açılışını bloklamasın diye arka planda).
    threading.Thread(target=_run_etl_safely, daemon=True).start()
    # Sonrasında saatlik tekrarla.
    scheduler.add_job(_run_etl_safely, "interval", hours=1, id="voltra-etl-hourly")
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


@app.get("/{series}/today")
def today(series: str):
    if series not in TABLE_MODELS and series not in ("ptf", "smf", "consumption", "load-plan"):
        raise HTTPException(404, "Bilinmeyen seri")
    table = {"ptf": "ptf_hourly", "smf": "smf_hourly", "consumption": "consumption_hourly", "load-plan": "load_plan_hourly"}.get(series, series)
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


@app.get("/health")
def health():
    return {"status": "ok"}
