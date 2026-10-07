"""
Voltra ETL — EPİAŞ'tan veri çekme katmanı.

eptr2, [allextras] ile kurulduğunda sonuçları pandas DataFrame olarak döndürür.
Dikkat: EPİAŞ'ın döndürdüğü sütun adları servise göre değişebilir (ör. "price",
"mcp", "smf" gibi). Bu dosyadaki `_first_matching_column` fonksiyonu birkaç
olası adı dener; sizin hesabınızla ilk gerçek çalıştırmada `verbose=True`
çıktısını (veya df.columns.tolist()) kontrol edip gerekirse
CANDIDATE_VALUE_COLUMNS listesine kendi sütun adınızı eklemeniz yeterli.
Bu, EPİAŞ'ın API yanıt şemasını bu ortamdan test edemediğimiz için bilinçli
bırakılmış tek "kontrol edin" noktasıdır — geri kalan her şey çalışır durumdadır.
"""

from __future__ import annotations
import logging
import os
import re
import threading
import time
from datetime import date

import pandas as pd
import requests
from eptr2 import EPTR2

log = logging.getLogger("voltra-etl")

CANDIDATE_DT_COLUMNS = ["dt", "date", "tarih", "datetime", "hour"]
CANDIDATE_VALUE_COLUMNS = ["value", "price", "mcp", "smf", "smp", "ptf", "consumption", "rt_cons", "load_plan", "lep", "systemMarginalPrice"]

# Üretim/kapasite tablolarında kaynak olarak ASLA sayılmayacak sütunlar
# (tarih/saat ve toplam sütunları).
NON_SOURCE_COLUMNS = {"dt", "date", "tarih", "datetime", "hour", "saat", "period", "capacitydate", "toplam", "total"}


def _first_matching_column(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(
        f"Beklenen sütunlardan hiçbiri bulunamadı. Mevcut sütunlar: {df.columns.tolist()}. "
        "fetch.py içindeki CANDIDATE_* listelerine doğru sütun adını ekleyin."
    )


# ---------------------------------------------------------------------------
# EPİAŞ istemcisi (giriş bileti / TGT yönetimi)
# ---------------------------------------------------------------------------
# Tek bir EPTR2 nesnesi tüm ETL çalışmalarında paylaşılır (her çalışmada yeniden
# login, EPİAŞ'ın giriş servisini reddetmesine yol açıyordu). Ancak giriş bileti
# (TGT) birkaç saat içinde geçerliliğini yitirir; bu yüzden istemci en fazla
# CLIENT_MAX_AGE_SEC kadar yaşatılır, ardından yenilenir. Kimlik doğrulama
# hatası görülürse de (en fazla 5 dakikada bir) istemci yenilenip çağrı tekrar denenir.
_client_singleton: EPTR2 | None = None
_client_created_at: float = 0.0
_last_forced_refresh: float = 0.0
_client_lock = threading.Lock()
CLIENT_MAX_AGE_SEC = 60 * 60
FORCE_REFRESH_MIN_GAP_SEC = 5 * 60
_AUTH_HINTS = ("tgt", "ticket", "authenticat", "credential", "unauthor", "401", "403", "oturum", "session")


def get_client(force_new: bool = False) -> EPTR2:
    global _client_singleton, _client_created_at, _last_forced_refresh
    with _client_lock:
        now = time.time()
        expired = _client_singleton is None or (now - _client_created_at) > CLIENT_MAX_AGE_SEC
        if force_new and _client_singleton is not None:
            if now - _last_forced_refresh < FORCE_REFRESH_MIN_GAP_SEC:
                force_new = False  # çok sık yeniden login olmayalım
            else:
                _last_forced_refresh = now
        if expired or force_new:
            # use_dotenv=True: EPTR_USERNAME / EPTR_PASSWORD ortam değişkenlerinden okunur.
            # recycle_tgt=True: giriş bileti (TGT) süresi dolana kadar yeniden kullanılır.
            _client_singleton = EPTR2(use_dotenv=True, recycle_tgt=True)
            _client_created_at = now
        return _client_singleton


def _call(eptr: EPTR2, call_name: str, **kwargs):
    """eptr.call(...) — kimlik doğrulama kaynaklı hatada istemciyi yenileyip bir kez daha dener."""
    try:
        return eptr.call(call_name, **kwargs)
    except Exception as e:
        msg = str(e).lower()
        if any(h in msg for h in _AUTH_HINTS):
            log.warning("EPİAŞ çağrısı (%s) kimlik doğrulama hatası verdi, istemci yenilenip tekrar denenecek: %s", call_name, e)
            return get_client(force_new=True).call(call_name, **kwargs)
        raise


def fetch_series(eptr: EPTR2, call_name: str, start_date: str, end_date: str) -> list[dict]:
    """
    Tek bir eptr2 servisini çağırır ve [{"dt": Timestamp, "value": float}, ...] döndürür.
    start_date / end_date: "YYYY-MM-DD" formatında.
    """
    df = _call(eptr, call_name, start_date=start_date, end_date=end_date)
    if df is None or len(df) == 0:
        return []

    dt_col = _first_matching_column(df, CANDIDATE_DT_COLUMNS)
    val_col = _first_matching_column(df, CANDIDATE_VALUE_COLUMNS)

    out = df[[dt_col, val_col]].rename(columns={dt_col: "dt", val_col: "value"})
    out["dt"] = pd.to_datetime(out["dt"], utc=False)
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["value"])
    return out.to_dict(orient="records")


def fetch_generation_mix(eptr: EPTR2, call_name: str, start_date: str, end_date: str) -> list[dict]:
    """
    Kaynak bazında gerçek zamanlı üretim (güneş, rüzgar, doğalgaz, barajlı vb.).
    EPİAŞ'ın döndürdüğü tabloda tarih/saat ve toplam dışındaki HER sütun ayrı bir
    kaynaktır; bu fonksiyon sütun adlarını bilmeden hepsini "eritip"
    [{"dt":..., "source": "wind", "value_mw": ...}, ...] satırlarına çevirir.
    """
    df = _call(eptr, call_name, start_date=start_date, end_date=end_date)
    if df is None or len(df) == 0:
        return []

    dt_col = _first_matching_column(df, CANDIDATE_DT_COLUMNS)
    value_cols = [c for c in df.columns if c != dt_col and str(c).lower() not in NON_SOURCE_COLUMNS]

    out = df[[dt_col] + value_cols].rename(columns={dt_col: "dt"})
    out["dt"] = pd.to_datetime(out["dt"], utc=False)
    melted = out.melt(id_vars=["dt"], value_vars=value_cols, var_name="source", value_name="value_mw")
    melted["value_mw"] = pd.to_numeric(melted["value_mw"], errors="coerce")
    melted = melted.dropna(subset=["value_mw"])
    return melted.to_dict(orient="records")


# ---------------------------------------------------------------------------
# Kurulu güç (EPİAŞ "Kurulu Güç" raporu)
# ---------------------------------------------------------------------------
_LONG_NAME_COLS = ("name", "type", "source", "fueltype", "resource", "kaynak", "energysource", "sourcetype", "fuel")
_LONG_VALUE_COLS = ("installedcapacity", "installedpower", "capacity", "value", "kurulugüc", "kurulugu", "power", "total")
_capacity_param_style: str | None = None  # hangi parametre biçimi çalıştıysa hatırlanır


def _capacity_rows_from_frame(df: pd.DataFrame, period: date) -> list[dict]:
    """Gelen tablo geniş (sütun = kaynak) ya da uzun (satır = kaynak) olabilir; ikisini de işler."""
    cols = list(df.columns)
    log.info("  kurulu güç tablosu: %d satır, sütunlar=%s", len(df), cols)
    low = {c: str(c).lower() for c in cols}
    dt = pd.Timestamp(period)

    # --- Uzun biçim: bir "kaynak adı" sütunu + bir sayısal sütun ---
    name_col = next((c for c in cols if low[c] in _LONG_NAME_COLS and not pd.api.types.is_numeric_dtype(df[c])), None)
    val_col = next((c for c in cols if low[c] in _LONG_VALUE_COLS and c != name_col), None)
    if name_col is not None and val_col is not None:
        tmp = df[[name_col, val_col]].copy()
        tmp[val_col] = pd.to_numeric(tmp[val_col], errors="coerce")
        tmp = tmp.dropna(subset=[val_col])
        tmp = tmp[~tmp[name_col].astype(str).str.lower().isin(NON_SOURCE_COLUMNS)]
        grouped = tmp.groupby(name_col)[val_col].sum()
        return [{"dt": dt, "source": str(k), "value_mw": float(v)} for k, v in grouped.items()]

    # --- Geniş biçim: en güncel satırı al, sütunları kaynak say ---
    date_col = next((c for c in cols if low[c] in NON_SOURCE_COLUMNS - {"toplam", "total"}), None)
    if date_col is not None:
        df = df.sort_values(date_col)
    last = df.iloc[-1]
    rows = []
    for c in cols:
        if low[c] in NON_SOURCE_COLUMNS:
            continue
        v = pd.to_numeric(last[c], errors="coerce")
        if pd.notna(v):
            rows.append({"dt": dt, "source": str(c), "value_mw": float(v)})
    return rows


# --- EPİAŞ REST (eptr2'de "installed-capacity" çağrısı tanımlı değilse yedek yol) ---
EPIAS_CAS_URL = "https://giris.epias.com.tr/cas/v1/tickets"
EPIAS_API_BASE = "https://seffaflik.epias.com.tr/electricity-service"
CAPACITY_REST_PATHS = ["/v1/generation/data/installed-capacity"]
_capacity_discovery_logged = False


def _log_capacity_discovery(eptr: EPTR2) -> bool:
    """eptr2 sürümünü ve kurulu güçle ilgili tanımlı çağrıları bir kez loglar. Çağrı varsa True döner."""
    global _capacity_discovery_logged
    try:
        names = [str(n) for n in eptr.get_available_calls()]
    except Exception as e:
        names = []
        if not _capacity_discovery_logged:
            log.info("  eptr2 çağrı listesi alınamadı: %s", e)
    if not _capacity_discovery_logged:
        _capacity_discovery_logged = True
        try:
            from importlib.metadata import version
            ver = version("eptr2")
        except Exception:
            ver = "?"
        related = [n for n in names if any(k in n.lower() for k in ("install", "capacity", "kurulu", "pp-list"))]
        log.info("  eptr2 sürümü=%s, kurulu güçle ilgili tanımlı çağrılar=%s", ver, related)
    return "installed-capacity" in names


def _get_tgt(eptr: EPTR2) -> str:
    """Mevcut istemcinin giriş biletini (TGT) bulur; yoksa CAS üzerinden bir kez giriş yapar."""
    for v in list(vars(eptr).values()):
        if isinstance(v, str) and v.startswith("TGT-"):
            return v
    user, pw = os.getenv("EPTR_USERNAME"), os.getenv("EPTR_PASSWORD")
    r = requests.post(
        EPIAS_CAS_URL, data={"username": user, "password": pw},
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "text/plain"}, timeout=30,
    )
    m = re.search(r"TGT-[\w\-\.]+", r.text or "")
    if r.status_code in (200, 201) and m:
        return m.group(0)
    raise RuntimeError(f"EPİAŞ girişi başarısız: HTTP {r.status_code} {(r.text or '')[:200]}")


def _rest_installed_capacity(eptr: EPTR2, period: date) -> list[dict]:
    tgt = _get_tgt(eptr)
    headers = {"TGT": tgt, "Content-Type": "application/json", "Accept": "application/json"}
    stamp = f"{period.isoformat()}T00:00:00+03:00"
    bodies = [{"period": stamp}, {"startDate": stamp, "endDate": stamp}]
    for path in CAPACITY_REST_PATHS:
        for body in bodies:
            r = requests.post(EPIAS_API_BASE + path, json=body, headers=headers, timeout=60)
            log.info("  kurulu güç REST %s %s -> HTTP %s %s", path, list(body), r.status_code, (r.text or "")[:300].replace("\n", " "))
            if r.status_code in (404, 405):
                raise RuntimeError(f"Kurulu güç servis adresi bulunamadı: {path} (HTTP {r.status_code})")
            if r.status_code != 200:
                continue
            data = r.json()
            items = data.get("items") if isinstance(data, dict) else data
            if not items and isinstance(data, dict):
                items = next((v for v in data.values() if isinstance(v, list) and v), None)
            if not items:
                continue
            rows = _capacity_rows_from_frame(pd.DataFrame(items), period)
            if rows:
                return rows
    return []


def fetch_installed_capacity(eptr: EPTR2, call_name: str, periods: list[date]) -> list[dict]:
    """
    EPİAŞ "Kurulu Güç" raporunu çeker. `periods` en yeniden eskiye sıralı ay başları;
    ilk veri dolu olan dönem kullanılır. eptr2'de çağrı tanımlıysa onu, değilse
    doğrudan EPİAŞ REST servisini kullanır.
    """
    global _capacity_param_style
    have_call = _log_capacity_discovery(eptr)
    styles = ["period", "range"]
    if _capacity_param_style in styles:
        styles.remove(_capacity_param_style)
        styles.insert(0, _capacity_param_style)

    last_error: Exception | None = None
    for period in periods:
        iso = period.isoformat()
        if have_call:
            for style in styles:
                kwargs = {"period": iso} if style == "period" else {"start_date": iso, "end_date": iso}
                try:
                    df = _call(eptr, call_name, **kwargs)
                except Exception as e:
                    last_error = e
                    log.info("  kurulu güç denemesi (%s, %s) başarısız: %s", style, iso, e)
                    continue
                if df is None or len(df) == 0:
                    continue
                rows = _capacity_rows_from_frame(df, period)
                if rows:
                    _capacity_param_style = style
                    return rows
        else:
            rows = _rest_installed_capacity(eptr, period)
            if rows:
                return rows
    if last_error is not None:
        raise last_error
    return []


def fetch_production_plan(eptr: EPTR2, uevcb_id: int, start_date: str, end_date: str) -> list[dict]:
    """
    KGÜP (üretim planı) — santral bazlı. TRACKED_PLANTS içindeki her UEVCB id için ayrı çağrılır.
    Gerçekleşen üretimle (UEVM) karşılaştırmak isterseniz aynı şemayla ikinci bir
    fetch_production_actual fonksiyonu ekleyip "uevm" çağrısını kullanabilirsiniz.
    """
    df = _call(eptr, "kgup", start_date=start_date, end_date=end_date, uevcb_id=uevcb_id)
    if df is None or len(df) == 0:
        return []
    dt_col = _first_matching_column(df, CANDIDATE_DT_COLUMNS)
    val_col = _first_matching_column(df, ["toplam", "plan_mw", "value", "mw"])
    out = df[[dt_col, val_col]].rename(columns={dt_col: "dt", val_col: "plan_mw"})
    out["dt"] = pd.to_datetime(out["dt"], utc=False)
    out["uevcb_id"] = str(uevcb_id)
    out["actual_mw"] = None
    return out.to_dict(orient="records")
