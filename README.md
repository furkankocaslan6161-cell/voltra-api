# Voltra ETL & API

EPİAŞ Şeffaflık Platformu'ndan PTF, SMF, tüketim ve (opsiyonel) santral üretim
planı verisini çekip bir Postgres/TimescaleDB veritabanına yazan, ardından bu
veriyi basit bir REST API ile sunan arka uç.

Bu paket, resmi olmayan ama aktif geliştirilen açık kaynak **eptr2**
(https://github.com/Tideseed/eptr2) kütüphanesini kullanır.

## Kurulum

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # sonra .env içine EPİAŞ kullanıcı adı/şifrenizi girin
docker compose up -d   # yerel Postgres/TimescaleDB
```

## İlk çalıştırma

```bash
python -m etl.run_etl
```

Bu komut: veritabanı tablolarını oluşturur, PTF/SMF/tüketim/yük-tahmini
serilerini son `ETL_LOOKBACK_DAYS` gün için çeker ve veritabanına yazar.

**Kontrol etmeniz gereken tek nokta:** `etl/fetch.py` içindeki
`CANDIDATE_VALUE_COLUMNS` listesi. EPİAŞ'ın döndürdüğü DataFrame'in sütun adı
serviste servise değişebilir; bu ortamda gerçek EPİAŞ hesabıyla test
edemediğimiz için birkaç olası adı (`price`, `mcp`, `smf`...) deneyip ilk
bulduğunu kullanan esnek bir eşleştirme yazıldı. İlk çalıştırmada bir hata
alırsanız, hata mesajı DataFrame'in gerçek sütunlarını yazdırır — o adı
listeye eklemeniz yeterlidir. Bir daha dokunmanız gerekmez.

## Otomatikleştirme

Cron ile saatlik çalıştırma (crontab -e):

```
5 * * * * cd /path/to/voltra-etl && .venv/bin/python -m etl.run_etl >> etl.log 2>&1
```

## API'yi başlatma

```bash
uvicorn api.main:app --reload --port 8000
```

Örnek uç noktalar:
- `GET /ptf/today` → bugünün saatlik PTF verisi
- `GET /ptf/history?days=90` → son 90 günün günlük/saatlik PTF verisi
- `GET /smf/today`, `/consumption/today`, `/load-plan/today`

Yanıt şekli, `voltra-prototip.html` içindeki Chart.js grafiklerinin beklediği
`{hours: [...], values: [...]}` formatına bilinçli olarak uyumlu tutuldu —
prototipteki sabit `actual`/`forecast`/`historyData` dizilerini bu
endpoint'lerden `fetch()` ile doldurmanız yeterli, grafik/tasarım kodunu
değiştirmenize gerek kalmaz.

## Santral üretim tahmini eklemek

`config.py` içindeki `TRACKED_PLANTS` sözlüğüne, EPİAŞ Şeffaflık
Platformu'ndaki "Santral Listesi" servisinden bulacağınız UEVCB kimlikleriyle
santral ekleyin:

```python
TRACKED_PLANTS = {
    "BOZCAADA RES": 3204384,
}
```

## Railway'e deploy (7/24 çalışan canlı API)

Bu proje artık ETL'i de kendi içinde (saatlik, arka planda) çalıştırıyor — Railway'de sadece **bir** web servisi + bir Postgres eklentisi yeterli, ayrı bir cron sunucusu gerekmiyor.

1. **GitHub'a yükleyin:** Bu `voltra-etl` klasörünü kendi GitHub hesabınızda yeni bir reponun (ör. `voltra-api`) içine koyun (GitHub'ın web arayüzünden "Add file → Upload files" ile sürükle-bırak da yeterli, git komutu bilmenize gerek yok).
2. **Railway'de proje açın:** railway.app → GitHub ile giriş yapın → "New Project" → "Deploy from GitHub repo" → az önce yüklediğiniz repoyu seçin.
3. **Postgres ekleyin:** Aynı proje içinde "New" → "Database" → "PostgreSQL". Railway otomatik olarak bir `DATABASE_URL` değişkeni oluşturur ve web servisinize bağlar (Variables sekmesinde "Add variable reference" ile `DATABASE_URL`'i Postgres'ten web servisine bağlamanız gerekebilir — Railway bunu genelde kendisi önerir).
4. **EPİAŞ bilgilerinizi ekleyin:** Web servisinizin "Variables" sekmesine `EPTR_USERNAME` ve `EPTR_PASSWORD` değişkenlerini (gerçek EPİAŞ hesap bilgilerinizle) girin. **Bu bilgileri asla bana veya herhangi bir sohbete yazmayın** — sadece Railway'in Variables paneline.
5. **Deploy'u bekleyin:** Railway otomatik olarak `requirements.txt`'i kurar, `Procfile`'daki komutla (`uvicorn api.main:app ...`) servisi başlatır. İlk açılışta arka planda ETL otomatik çalışır ve saatte bir kendini tazeler.
6. **Ortak URL'i alın:** Servisin "Settings → Networking → Generate Domain" ile size `https://xxxx.up.railway.app` gibi bir adres verir. `GET https://xxxx.up.railway.app/health` adresine girip `{"status":"ok"}` görüyorsanız API çalışıyor demektir.
7. **Widget'ı bağlayın:** Bu URL'i bana gönderin — `voltra-widget.html` içindeki sabit (sentetik) veri dizilerini bu API'den `fetch()` ile çeken gerçek koda çeviririm.

**Önemli:** `etl/fetch.py` içindeki sütun eşleştirmesi ilk gerçek çalıştırmada bir hata verirse, Railway'in "Logs" sekmesinde hatayı (gerçek sütun adlarını) görürsünüz — o adı `CANDIDATE_VALUE_COLUMNS` listesine ekleyip yeniden deploy etmeniz yeterli; bana da hata mesajını yapıştırırsanız ben düzeltirim.

## Sonraki adımlar (bu depoda henüz yok)

- Kullanıcı hesabı / oturum yönetimi (Faz 2 — abonelik modülü)
- Tahmin modeli servisi (SARIMA/Prophet → ML) — bu ETL'in ürettiği temiz veri
  üzerine kurulur
- Üretim tahmini için NWP (hava durumu) veri kaynağı entegrasyonu
- Gerçek EPİAŞ hesabıyla uçtan uca test ve `CANDIDATE_VALUE_COLUMNS`
  listesinin kesinleştirilmesi
