"""
Voltra ETL — merkezi konfigürasyon.

DATA_SOURCES burada, çektiğimiz her seri için:
  - eptr2 çağrı adını (call name / alias)
  - bizim veritabanı tablomuzun adını
tutuyor. eptr2 paketi 200'den fazla servisi kapsıyor; burada sadece
sitenin bugünkü prototipinde kullanılan dört temel seriyi tanımlıyoruz.
İhtiyaç duydukça yeni satırlar ekleyebilirsiniz — doğru çağrı adını bulmak için:

    python -m eptr2 search ptf
    python -m eptr2 list

komutlarını (paket kurulduktan sonra) kullanabilirsiniz.
"""

import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://voltra:voltra@localhost:5432/voltra")
ETL_LOOKBACK_DAYS = int(os.getenv("ETL_LOOKBACK_DAYS", "3"))

# call: eptr2'deki servis adı (veya alias'ı)
# table: bu serinin yazılacağı Postgres tablosu
DATA_SOURCES = {
    "ptf": {"call": "ptf", "table": "ptf_hourly"},          # Piyasa Takas Fiyatı (GÖP)
    "smf": {"call": "smf", "table": "smf_hourly"},          # Sistem Marjinal Fiyatı (DGP)
    "rt_cons": {"call": "rt-cons", "table": "consumption_hourly"},  # Gerçek zamanlı tüketim
    "load_plan": {"call": "load-plan", "table": "load_plan_hourly"},  # Yük tahmini
}

# Santral üretim tahmini/gerçekleşen üretim (KGÜP/UEVM) için UEVCB kimliklerinizi
# EPİAŞ Şeffaflık Platformu'nda "Santral Listesi" servisinden bulup buraya ekleyin.
# Örnek: {"BOZCAADA RES": 3204384}
TRACKED_PLANTS: dict[str, int] = {
    # "SANTRAL_ADI": UEVCB_ID,
}
