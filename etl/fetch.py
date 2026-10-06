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
import pandas as pd
from eptr2 import EPTR2

CANDIDATE_DT_COLUMNS = ["dt", "date", "tarih", "datetime", "hour"]
CANDIDATE_VALUE_COLUMNS = ["value", "price", "mcp", "smf", "smp", "ptf", "consumption", "rt_cons", "load_plan", "lep", "systemMarginalPrice"]


def _first_matching_column(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(
        f"Beklenen sütunlardan hiçbiri bulunamadı. Mevcut sütunlar: {df.columns.tolist()}. "
        "fetch.py içindeki CANDIDATE_* listelerine doğru sütun adını ekleyin."
    )


_client_singleton: EPTR2 | None = None


def get_client() -> EPTR2:
    """
    Tek bir EPTR2 nesnesini (ve onun giriş biletini/TGT'sini) tüm ETL
    çalışmaları boyunca paylaşır. Her çalıştırmada yeni bir EPTR2() nesnesi
    oluşturmak, her seferinde EPİAŞ'a yeniden login isteği göndermek anlamına
    gelir — saatlik ETL ile 15 dakikalık hızlı yenileme aynı anda/sık sık
    çalıştığında bu, EPİAŞ'ın giriş servisini reddetmesine (hataya) yol açar.
    """
    global _client_singleton
    if _client_singleton is None:
        # use_dotenv=True: EPTR_USERNAME / EPTR_PASSWORD ortam değişkenlerinden okunur.
        # recycle_tgt=True: giriş bileti (TGT) süresi dolana kadar yeniden kullanılır.
        _client_singleton = EPTR2(use_dotenv=True, recycle_tgt=True)
    return _client_singleton


def fetch_series(eptr: EPTR2, call_name: str, start_date: str, end_date: str) -> list[dict]:
    """
    Tek bir eptr2 servisini çağırır ve [{"dt": Timestamp, "value": float}, ...] döndürür.
    start_date / end_date: "YYYY-MM-DD" formatında.
    """
    df = eptr.call(call_name, start_date=start_date, end_date=end_date)
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
    EPİAŞ'ın döndürdüğü tabloda tarih sütunu dışındaki HER sütun ayrı bir
    kaynaktır; bu fonksiyon sütun adlarını bilmeden hepsini "eritip"
    [{"dt":..., "source": "ruzgar", "value_mw": ...}, ...] satırlarına çevirir.
    """
    df = eptr.call(call_name, start_date=start_date, end_date=end_date)
    if df is None or len(df) == 0:
        return []

    dt_col = _first_matching_column(df, CANDIDATE_DT_COLUMNS)
    value_cols = [c for c in df.columns if c != dt_col and str(c).lower() not in ("toplam", "total")]

    out = df[[dt_col] + value_cols].rename(columns={dt_col: "dt"})
    out["dt"] = pd.to_datetime(out["dt"], utc=False)
    melted = out.melt(id_vars=["dt"], value_vars=value_cols, var_name="source", value_name="value_mw")
    melted["value_mw"] = pd.to_numeric(melted["value_mw"], errors="coerce")
    melted = melted.dropna(subset=["value_mw"])
    return melted.to_dict(orient="records")


def fetch_production_plan(eptr: EPTR2, uevcb_id: int, start_date: str, end_date: str) -> list[dict]:
    """
    KGÜP (üretim planı) — santral bazlı. TRACKED_PLANTS içindeki her UEVCB id için ayrı çağrılır.
    Gerçekleşen üretimle (UEVM) karşılaştırmak isterseniz aynı şemayla ikinci bir
    fetch_production_actual fonksiyonu ekleyip "uevm" çağrısını kullanabilirsiniz.
    """
    df = eptr.call("kgup", start_date=start_date, end_date=end_date, uevcb_id=uevcb_id)
    if df is None or len(df) == 0:
        return []
    dt_col = _first_matching_column(df, CANDIDATE_DT_COLUMNS)
    val_col = _first_matching_column(df, ["toplam", "plan_mw", "value", "mw"])
    out = df[[dt_col, val_col]].rename(columns={dt_col: "dt", val_col: "plan_mw"})
    out["dt"] = pd.to_datetime(out["dt"], utc=False)
    out["uevcb_id"] = str(uevcb_id)
    out["actual_mw"] = None
    return out.to_dict(orient="records")
