# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5db35f91-51bf-441d-8117-deeef1da6c4f",
# META       "default_lakehouse_name": "Lakehouse_NordLade",
# META       "default_lakehouse_workspace_id": "5772901f-81a2-488d-afa1-65630c3fe909",
# META       "known_lakehouses": [
# META         {
# META           "id": "5db35f91-51bf-441d-8117-deeef1da6c4f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Zelle 1: Einstellungen und Regeln laden
# 
# Lädt NB_00_Einstellungen_und_Regeln. Daraus braucht dieses Notebook die Pfade zum Lager und zur Partner-Ablage sowie die Liste der Quellen.

# CELL ********************

%run NB_00_Einstellungen_und_Regeln

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 2: Parameter – was soll geliefert werden?
#
# Vor jedem Lauf hier einstellen:
#   von_datum, bis_datum  erster und letzter Tag
#                         im Format TT.MM.JJJJ
#                         (Tag 1 = 01.07.2026,
#                          Tag 90 = 28.09.2026)
#   quelle                "alle" oder eine Quelle:
#                         "ladevorgaenge", "kunden",
#                         "stationen", "tarife"
#   version               1 = Erstlieferung
#                         2 = korrigierte Version
#   variante              True nur für Tag 33:
#                         die veränderte Datei
# =====================================================

von_datum = "31.07.2026"
bis_datum = "31.07.2026"
quelle = "alle"
version = 2
variante = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3: Dateien in die Partner-Ablage legen
#
# Was diese Zelle tut:
#   Sie spielt den Partner. Für jeden Tag im gewählten
#   Zeitraum sucht sie die passenden Dateien im Lager
#   (Files/beispieldaten) und legt eine Kopie in die
#   Partner-Ablage (Files/partner_ablage).
#   Das Lager selbst bleibt unverändert.
#
# Am Ende zeigt sie je Tag, was geliefert wurde, und
# ob an diesem Tag ein Sonderfall eingebaut ist
# (Hinweis aus erwartung.json, markiert mit ★).
#
# Gibt es eine Datei nicht (z. B. Version 2 an einem
# normalen Tag), wird sie übersprungen und gemeldet.
# Liegt eine Datei noch ungeholt in der Ablage, wird
# sie nicht überschrieben.
# =====================================================

import json, os, shutil
from datetime import datetime, timedelta

LOKAL = "/lakehouse/default/"    # Lakehouse als lokaler Ordner
ABLAGE = PFAD_PARTNER_ABLAGE
LAGER = PFAD_BEISPIELDATEN
TAG_1 = datetime(2026, 7, 1).date()

start = datetime.strptime(von_datum, "%d.%m.%Y").date()
ende = datetime.strptime(bis_datum, "%d.%m.%Y").date()
if ende < start:
    raise ValueError(f"bis_datum {bis_datum} liegt vor von_datum {von_datum}")

quellen = QUELLEN if quelle == "alle" else [quelle]
for q in quellen:
    if q not in QUELLEN:
        raise ValueError(f"Unbekannte Quelle '{q}'. Erlaubt: alle, {', '.join(QUELLEN)}")

# Hinweise zu Sonderfällen aus erwartung.json
with open(f"{LOKAL}{LAGER}/erwartung.json", encoding="utf-8") as datei:
    hinweise = {e["ablage"]: e.get("hinweis") for e in json.load(datei)}

print(f"Lieferung: {quelle}, Version {version}" + (", veränderte Datei" if variante else "") + "\n")

anzahl = 0
tag = start
while tag <= ende:
    nummer = (tag - TAG_1).days + 1
    geliefert, fehlt, belegt, notizen = [], [], [], []
    for q in quellen:
        name = f"{q}_{tag.isoformat()}_v{version}.csv"
        von = f"{LAGER}/{'varianten' if variante else q}/{name}"
        nach = f"{ABLAGE}/{name}"
        if not os.path.exists(LOKAL + von):
            fehlt.append(q)
        elif os.path.exists(LOKAL + nach):
            belegt.append(q)
        else:
            shutil.copyfile(LOKAL + von, LOKAL + nach)
            geliefert.append(q)
            if hinweise.get(von):
                notizen.append(f"{q}: {hinweise[von]}")

    zeile = f"Tag {nummer:>2} ({tag.strftime('%d.%m.%Y')}): "
    zeile += f"✓ {', '.join(geliefert)}" if geliefert else "nichts geliefert"
    print(zeile)
    for n in notizen:
        print(f"         ★ {n}")
    if belegt:
        print(f"         ! noch ungeholt in der Ablage, nicht überschrieben: {', '.join(belegt)}")
    if fehlt and not (version == 2 and quelle == "alle"):   # v2 gibt es nur vereinzelt
        print(f"         – nicht im Lager: {', '.join(fehlt)}")
    anzahl += len(geliefert)
    tag += timedelta(days=1)

print(f"\n✓ {anzahl} Dateien in die Partner-Ablage gelegt")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
