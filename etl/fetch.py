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
CANDIDATE_VALUE_COLUMNS = ["value", "price", "mcp", "smf", "smp", "ptf", "consumption", "rt_cons", "load_plan", "lep"]


def _first_matching_column(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(
        f"Beklenen sütunlardan hiçbiri bulunamadı. Mevcut sütunlar: {df.columns.tolist()}. "
        "fetch.py içindeki CANDIDATE_* listelerine doğru sütun adını ekleyin."
    )


def get_client() -> EPTR2:
    # use_dotenv=True: EPTR_USERNAME / EPTR_PASSWORD .env dosyasından okunur.
    # recycle_tgt=True: kimlik doğrulama bileti (TGT) yeniden kullanılır, her çağrıda tekrar login olunmaz.
    return EPTR2(use_dotenv=True, recycle_tgt=True)


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
