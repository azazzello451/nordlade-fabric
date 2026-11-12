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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # NB_08 Pflege
# Hält das Lakehouse schlank und schnell. Läuft sonntags über PL_Pflege.

# CELL ********************

%run NB_00_Einstellungen_und_Regeln

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# =====================================================
# Zelle 2a (NB_08): Parameter
# Die Pipeline setzt hier ihre Lauf-ID ein.
# =====================================================
lauf_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 2b (NB_08): Lauf-ID für Handstart
# Ohne Pipeline gibt es keine Lauf-ID → eigene erzeugen.
# =====================================================
from datetime import datetime
if not lauf_id:
    lauf_id = "manuell-" + datetime.now().strftime("%Y%m%d-%H%M%S")
print(f"✓ Lauf-ID: {lauf_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3a (NB_08): Tabellen optimieren
#
# optimieren fasst in jeder Tabelle viele kleine
# Dateien zu wenigen großen zusammen (OPTIMIZE).
#   Gold:  V-Order kommt automatisch aus der
#          Tabelleneigenschaft (NB_01) → schnell lesen
#   Rest:  ohne V-Order → schnell schreiben
#   Fakt:  zusätzlich Z-Order nach datum_key. Zeilen
#          desselben Tages liegen zusammen, Abfragen
#          mit Datumsfilter lesen weniger Dateien.
# Gibt je Tabelle Dateien vorher → nachher zurück.
# =====================================================

SCHEMAS_PFLEGE = ["silber", "qualitaet", "steuerung", "gold"]
ZORDER = {"gold.fakt_ladevorgang": "datum_key"}


def alle_tabellen():
    return [f"{s}.{t.tableName}" for s in SCHEMAS_PFLEGE
            for t in spark.sql(f"SHOW TABLES IN {s}").collect()]


def dateien(tabelle):
    return spark.sql(f"DESCRIBE DETAIL {tabelle}").first()["numFiles"]


def optimieren():
    ergebnis = {}
    for t in alle_tabellen():
        vorher = dateien(t)
        z = f" ZORDER BY ({ZORDER[t]})" if t in ZORDER else ""
        spark.sql(f"OPTIMIZE {t}{z}")
        ergebnis[t] = (vorher, dateien(t))
    return ergebnis


print("✓ optimieren bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3b (NB_08): Aufräumen
#
# aufraeumen
#   1. VACUUM: löscht Dateien alter Tabellenversionen,
#      die älter als VACUUM_TAGE sind. Bis dahin bleibt
#      Time Travel für Fehlersuche möglich.
#   2. Logbuch: Einträge älter als
#      LAUFPROTOKOLL_AUFBEWAHRUNG_TAGE löschen.
#   3. Bronze: Eingangsordner älter als
#      BRONZE_AUFBEWAHRUNG_TAGE löschen – aber nur, wenn
#      keine Datei darin noch gesperrt ist oder auf
#      Prüfung wartet (dafür braucht man den Beweis).
# Gibt die Zahl gelöschter Logbuch-Zeilen und Ordner zurück.
# =====================================================

import os
import shutil
from datetime import datetime, timedelta
from pyspark.sql import functions as F

BRONZE = "/lakehouse/default/" + PFAD_BRONZE


def aufraeumen():
    for t in alle_tabellen():
        spark.sql(f"VACUUM {t} RETAIN {VACUUM_TAGE * 24} HOURS")

    alt = spark.sql(f"SELECT COUNT(*) FROM steuerung.laeufe WHERE start_zeit < "
                    f"current_timestamp() - INTERVAL {LAUFPROTOKOLL_AUFBEWAHRUNG_TAGE} DAYS").first()[0]
    spark.sql(f"DELETE FROM steuerung.laeufe WHERE start_zeit < "
              f"current_timestamp() - INTERVAL {LAUFPROTOKOLL_AUFBEWAHRUNG_TAGE} DAYS")

    offen = {r["pfad"].rsplit("/", 1)[0] for r in spark.table("qualitaet.lieferungen")
             .filter(F.col("status").isin(SCHEMA_OK, *GESPERRT)).select("pfad").collect()}
    grenze = (datetime.now() - timedelta(days=BRONZE_AUFBEWAHRUNG_TAGE)).strftime("%Y%m%d")
    geloescht = 0
    for quelle in QUELLEN:
        if not os.path.isdir(f"{BRONZE}/{quelle}"):
            continue
        for ordner in os.listdir(f"{BRONZE}/{quelle}"):
            pfad = f"{PFAD_BRONZE}/{quelle}/{ordner}"
            if ordner.startswith("eingang=") and ordner[8:16] < grenze and pfad not in offen:
                shutil.rmtree(f"{BRONZE}/{quelle}/{ordner}")
                geloescht += 1
    return alt, geloescht


print("✓ aufraeumen bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 4 (NB_08): Pflege ausführen
#
# Erst optimieren (3a), dann aufräumen (3b). VACUUM
# danach entfernt auch die Dateien, die OPTIMIZE gerade
# ersetzt hat (sobald sie älter als VACUUM_TAGE sind).
# Logbuch: Start und Ende in steuerung.laeufe.
# =====================================================

SCHRITT = "NB_08_Pflege"
lauf_starten(lauf_id, SCHRITT)

try:
    optimiert = optimieren()
    log_geloescht, ordner_geloescht = aufraeumen()
    vorher = sum(v for v, _ in optimiert.values())
    nachher = sum(n for _, n in optimiert.values())
    lauf_beenden(lauf_id, SCHRITT, "erfolgreich",
                 meldung=f"{len(optimiert)} Tabellen: {vorher} → {nachher} Dateien; "
                         f"{log_geloescht} Logbuch-Zeilen, {ordner_geloescht} Bronze-Ordner gelöscht")
except Exception as fehler_meldung:
    lauf_beenden(lauf_id, SCHRITT, "fehler", meldung=str(fehler_meldung)[:1000])
    raise

print(f"✓ Pflege fertig (Lauf {lauf_id})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 5 (NB_08): Überblick
#
# Zeigt je Tabelle die Zahl der Dateien vorher und
# nachher. Weniger Dateien = schneller lesen und
# schneller MERGEn. Ändert nichts, nur zum Hinschauen.
# =====================================================

for t, (v, n) in sorted(optimiert.items()):
    print(f"  {t:<32} {v:>5} → {n:>5} Dateien")
print(f"\nLogbuch: {log_geloescht} alte Einträge gelöscht")
print(f"Bronze:  {ordner_geloescht} alte Eingangsordner gelöscht")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
