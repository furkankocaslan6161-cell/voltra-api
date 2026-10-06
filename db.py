"""
Voltra ETL — veritabanı katmanı.

Basit, zaman serisi odaklı bir şema kullanıyoruz: her tablo (dt, value) çifti
tutar ve dt üzerinde birincil anahtar + upsert yapılır, böylece EPİAŞ verisini
revize edilmiş haliyle yeniden çektiğinizde satırlar güncellenir, çoğalmaz.

Gerçek kullanımda TimescaleDB uzantısını (Postgres üstüne) kurup her tabloyu
bir "hypertable"a çevirmeniz, büyüyen zaman serisi verisinde sorgu
performansını ciddi şekilde artırır — kurulum notu README'de.
"""

from __future__ import annotations
from datetime import datetime
from sqlalchemy import create_engine, Column, DateTime, Numeric, String, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import DATABASE_URL


def _normalize_db_url(url: str) -> str:
    """
    Railway/Heroku gibi platformlar DATABASE_URL'i sade 'postgresql://' olarak
    verir; SQLAlchemy bu durumda varsayılan olarak psycopg2 sürücüsünü arar.
    psycopg2-binary bazı Python sürümlerinde derleme/kurulum sorunu çıkardığı
    için, saf Python ile yazılmış ve her ortamda sorunsuz kurulan pg8000
    sürücüsünü zorluyoruz.
    """
    if url.startswith("postgresql+psycopg2://"):
        return url.replace("postgresql+psycopg2://", "postgresql+pg8000://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+pg8000://", 1)
    return url


engine = create_engine(_normalize_db_url(DATABASE_URL), pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


def _hourly_table(name: str):
    """(dt, value) şemasında bir tablo sınıfı üretir."""
    attrs = {
        "__tablename__": name,
        "dt": Column(DateTime(timezone=True), primary_key=True),
        "value": Column(Numeric, nullable=False),
    }
    return type(name.title().replace("_", ""), (Base,), attrs)


PtfHourly = _hourly_table("ptf_hourly")
SmfHourly = _hourly_table("smf_hourly")
ConsumptionHourly = _hourly_table("consumption_hourly")
LoadPlanHourly = _hourly_table("load_plan_hourly")

TABLE_MODELS = {
    "ptf_hourly": PtfHourly,
    "smf_hourly": SmfHourly,
    "consumption_hourly": ConsumptionHourly,
    "load_plan_hourly": LoadPlanHourly,
}


class ProductionPlan(Base):
    """Santral bazlı üretim planı/gerçekleşen üretim (KGÜP/UEVM)."""
    __tablename__ = "production_hourly"
    dt = Column(DateTime(timezone=True), primary_key=True)
    uevcb_id = Column(String, primary_key=True)
    plan_mw = Column(Numeric)
    actual_mw = Column(Numeric)


class GenerationHourly(Base):
    """Kaynak bazında (güneş, rüzgar, doğalgaz, barajlı vb.) gerçek zamanlı üretim."""
    __tablename__ = "generation_hourly"
    dt = Column(DateTime(timezone=True), primary_key=True)
    source = Column(String, primary_key=True)
    value_mw = Column(Numeric, nullable=False)


class EtlState(Base):
    """Tek seferlik işlerin (ör. yıllık geçmiş verinin ilk doldurulması) durumunu tutar."""
    __tablename__ = "etl_state"
    key = Column(String, primary_key=True)
    value = Column(String)


def init_db():
    """Tabloları oluşturur (idempotent — zaten varsa dokunmaz)."""
    Base.metadata.create_all(engine)


def upsert_hourly(table_name: str, rows: list[dict]):
    """
    rows: [{"dt": datetime, "value": float}, ...]
    dt çakışırsa value günceller (EPİAŞ verisi revize edildiğinde önemli).
    """
    if not rows:
        return 0
    model = TABLE_MODELS[table_name]
    with SessionLocal() as session:
        stmt = pg_insert(model).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["dt"],
            set_={"value": stmt.excluded.value},
        )
        session.execute(stmt)
        session.commit()
    return len(rows)


def upsert_production(rows: list[dict]):
    """rows: [{"dt":..., "uevcb_id":..., "plan_mw":..., "actual_mw":...}, ...]"""
    if not rows:
        return 0
    with SessionLocal() as session:
        stmt = pg_insert(ProductionPlan).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["dt", "uevcb_id"],
            set_={"plan_mw": stmt.excluded.plan_mw, "actual_mw": stmt.excluded.actual_mw},
        )
        session.execute(stmt)
        session.commit()
    return len(rows)


def upsert_generation(rows: list[dict]):
    """rows: [{"dt":..., "source":..., "value_mw":...}, ...]"""
    if not rows:
        return 0
    with SessionLocal() as session:
        stmt = pg_insert(GenerationHourly).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["dt", "source"],
            set_={"value_mw": stmt.excluded.value_mw},
        )
        session.execute(stmt)
        session.commit()
    return len(rows)


def get_state(key: str) -> str | None:
    with SessionLocal() as session:
        row = session.get(EtlState, key)
        return row.value if row else None


def set_state(key: str, value: str):
    with SessionLocal() as session:
        stmt = pg_insert(EtlState).values([{"key": key, "value": value}])
        stmt = stmt.on_conflict_do_update(index_elements=["key"], set_={"value": stmt.excluded.value})
        session.execute(stmt)
        session.commit()
