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
ANNUAL_BACKFILL_DAYS = int(os.getenv("ANNUAL_BACKFILL_DAYS", "365"))

# call: eptr2'deki servis adı (veya alias'ı)
# table: bu serinin yazılacağı Postgres tablosu
DATA_SOURCES = {
    "ptf": {"call": "ptf", "table": "ptf_hourly"},          # Piyasa Takas Fiyatı (GÖP)
    "smf": {"call": "smf", "table": "smf_hourly"},          # Sistem Marjinal Fiyatı (DGP)
    "rt_cons": {"call": "rt-cons", "table": "consumption_hourly"},  # Gerçek zamanlı tüketim
    "load_plan": {"call": "load-plan", "table": "load_plan_hourly"},  # Yük tahmini
}

# "Türkiye Enerji Piyasası" sayfası için: her 15 dakikada bir, sadece BUGÜNÜ
# yeniden çeken hafif seri listesi (tam ETL'in aksine geçmiş günlere bakmaz).
FAST_REFRESH_SOURCES = ["ptf", "smf"]

# Kaynak bazında (güneş, rüzgar, doğalgaz, barajlı vb.) gerçek zamanlı üretim.
# DİKKAT: eptr2'nin bu servis için gerçek çağrı adı "rt-gen" olarak varsayıldı;
# ilk gerçek çalıştırmada hata verirse (ör. "bilinmeyen servis"),
#   python -m eptr2 list
# komutuyla doğru adı bulup burayı güncelleyin — fetch.py'deki
# fetch_generation_mix fonksiyonu, hangi sütun adı gelirse gelsin (tarih
# dışındaki tüm sayısal sütunları ayrı bir "kaynak" olarak) otomatik işler.
GENERATION_CALL = "rt-gen"
GENERATION_TABLE = "generation_hourly"

# "Kurulu güç" kartı için: EPİAŞ'ın gerçek zamanlı API'sinde kaynak bazında
# statik/lisanslı kurulu güç raporu yok; en yakın ve kaynak bazında kırılımı
# olan veri "Emre Amade Kapasite" (EAK) — sisteme o an sağlanabilecek aktif
# kapasite. eptr2'deki servis adı "eak". Aynı fetch_generation_mix
# fonksiyonuyla (tarih dışındaki her sütunu bir kaynak olarak) işlenir.
CAPACITY_CALL = "eak"
CAPACITY_TABLE = "capacity_hourly"

# Santral üretim tahmini/gerçekleşen üretim (KGÜP/UEVM) için UEVCB kimliklerinizi
# EPİAŞ Şeffaflık Platformu'nda "Santral Listesi" servisinden bulup buraya ekleyin.
# Örnek: {"BOZCAADA RES": 3204384}
TRACKED_PLANTS: dict[str, int] = {
    # "SANTRAL_ADI": UEVCB_ID,
}
