"""
Türkiye toplam kurulu güç — kaynak bazında resmi aylık veri.

EPİAŞ Şeffaflık Platformu'nun genel "Kurulu Güç" servisi (/v1/generation/data/
installed-capacity-new / -old) canlı ortamda "Method not found or disabled"
(ERR-006) döndürdüğü için, kurulu güç kartı T.C. Enerji ve Tabii Kaynaklar
Bakanlığı'nın (TEİAŞ verilerine dayalı) her ay açıkladığı resmi dağılımla
beslenir. Bu veri aylık değiştiğinden elle güncellenir: yeni ay açıklandığında
yalnızca PERIOD ve VALUES_MW değerlerini değiştirip yeniden deploy etmek yeterlidir.

Kaynak anahtarları, widget'taki Türkçe ad eşlemesiyle uyumludur:
  hydro -> Hidroelektrik (barajlı + akarsu), sun -> Güneş, naturalGas -> Doğalgaz,
  wind -> Rüzgar, domesticCoal -> Yerli Kömür, importCoal -> İthal Kömür,
  biomass -> Biyokütle, geothermal -> Jeotermal
"""

from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta

log = logging.getLogger("voltra-api")

# Ağustos 2026 sonu itibarıyla (Enerji ve Tabii Kaynaklar Bakanlığı açıklaması)
PERIOD = "2026-08"
SOURCE_NOTE = "Enerji ve Tabii Kaynaklar Bakanlığı (TEİAŞ verilerine dayalı) aylık kurulu güç dağılımı"
VALUES_MW = {
    "hydro": 32331,
    "sun": 27914,
    "naturalGas": 25012,
    "wind": 15434,
    "domesticCoal": 11565,
    "importCoal": 10466,
    "biomass": 2424,
    "geothermal": 1798,
}
TOTAL_MW = 126944  # Bakanlığın açıkladığı toplam; kaynak toplamıyla birebir aynı olmalı


def seed_installed_capacity() -> bool:
    """Anlık görüntüyü veritabanına yazar (aynı dönem zaten yazılmışsa dokunmaz). Yazdıysa True."""
    assert sum(VALUES_MW.values()) == TOTAL_MW, "Kaynak toplamı açıklanan toplamla uyuşmuyor"
    from db import get_state, set_state, upsert_capacity
    if get_state("capacity_snapshot_period") == PERIOD:
        return False
    y, m = (int(x) for x in PERIOD.split("-"))
    dt = datetime(y, m, 1, tzinfo=timezone(timedelta(hours=3)))
    rows = [{"dt": dt, "source": k, "value_mw": float(v)} for k, v in VALUES_MW.items()]
    n = upsert_capacity(rows)
    set_state("capacity_snapshot_period", PERIOD)
    set_state("capacity_note", SOURCE_NOTE)
    log.info("Kurulu güç anlık görüntüsü yazıldı: dönem %s, %d kaynak, toplam %d MW", PERIOD, n, TOTAL_MW)
    return True
