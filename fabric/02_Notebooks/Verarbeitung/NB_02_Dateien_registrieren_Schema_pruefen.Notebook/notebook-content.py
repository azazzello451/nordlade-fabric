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
# Lädt NB_00_Einstellungen_und_Regeln. Daraus braucht dieses Notebook den Datenvertrag, den Fingerabdruck, die Schwelle, die Verantwortlichen und das Logbuch.

# CELL ********************

%run NB_00_Einstellungen_und_Regeln

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# =====================================================
# Zelle 2: Parameter
#
# lauf_id  Nummer des Laufs, unter der alles im Logbuch
#          und im Register steht. Die Pipeline gibt sie
#          später mit. Beim Start von Hand bleibt sie
#          leer, dann wird automatisch eine erzeugt
#          (z. B. manuell-20261109-1430).
# =====================================================

lauf_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Lauf-Nummer erzeugen, falls von Hand gestartet
if not lauf_id:
    lauf_id = neue_lauf_id()
print("Lauf:", lauf_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3a: Neue Dateien in Bronze finden
#
# neue_dateien_finden sucht in Bronze alle Dateien, die
# noch nicht im Register stehen – geordnet nach Eingang,
# damit das Original immer vor einer späteren Variante
# geprüft wird (wichtig für Tag 33).
# =====================================================

import os, re
from datetime import datetime
from zoneinfo import ZoneInfo

LOKAL = "/lakehouse/default/"
NAMENSMUSTER = re.compile(r"^(?P<quelle>[a-z]+)_(?P<datum>\d{4}-\d{2}-\d{2})_v(?P<version>\d+)\.csv$")
EINGANGSMUSTER = re.compile(r"^eingang=(?P<zeit>\d{8}-\d{4})$")


def neue_dateien_finden(bekannte_pfade):
    """Alle Dateien in Bronze, die noch nicht im Register stehen."""
    neu = []
    for quelle in QUELLEN:
        basis = f"{PFAD_BRONZE}/{quelle}"
        if not os.path.isdir(LOKAL + basis):
            continue
        for eingang in sorted(os.listdir(LOKAL + basis)):
            treffer = EINGANGSMUSTER.match(eingang)
            if not treffer:
                continue
            for name in sorted(os.listdir(f"{LOKAL}{basis}/{eingang}")):
                pfad = f"{basis}/{eingang}/{name}"
                if pfad not in bekannte_pfade:
                    eingang_am = datetime.strptime(treffer["zeit"], "%Y%m%d-%H%M").replace(
                        tzinfo=ZoneInfo("Europe/Berlin"))   # Ordnername ist deutsche Zeit
                    neu.append({"pfad": pfad, "datei_name": name, "eingang_am": eingang_am,
                                "ordner_quelle": quelle})
    return sorted(neu, key=lambda d: (d["eingang_am"], d["ordner_quelle"], d["datei_name"]))


print("✓ neue_dateien_finden bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3b (NB_02): Eine Datei prüfen
#
# datei_pruefen liest Fingerabdruck, Kopfzeile und
# Zeilenzahl einer Datei und entscheidet:
#   gleicher Name + gleicher Fingerabdruck → duplikat
#   gleicher Name + anderer Fingerabdruck  → schema_gesperrt
#   Spalten passen nicht zum Datenvertrag  → schema_gesperrt
#   Datei ist leer                         → schema_gesperrt
#   sonst                                  → schema_ok
# Ein neuer Name mit bekanntem Inhalt (z. B. v2 = v1)
# gilt als normale neue Version.
# BOM (unsichtbares Zeichen am Dateianfang, z. B. von
# Excel): wird beim Lesen der Kopfzeile ignoriert
# (utf-8-sig) und im Grund als Hinweis vermerkt.
# Die Status-Namen kommen aus NB_00 (Zelle 8).
# =====================================================

def datei_pruefen(datei, bekannte_dateien):
    """Prüft eine Datei und gibt ihre Registerzeile zurück.
    bekannte_dateien: Liste der Registerzeilen (dicts) mit datei_name und datei_hash."""
    name = datei["datei_name"]
    zeile = {
        "lieferung_id": f"{name.removesuffix('.csv')}@{datei['eingang_am']:%Y%m%d-%H%M}",
        "datei_name": name,
        "pfad": datei["pfad"],
        "quelle": datei["ordner_quelle"],
        "eingang_am": datei["eingang_am"],
        "schwelle": SCHWELLE,
        "datei_hash": fingerabdruck(datei["pfad"]),
    }

    # Dateiname zerlegen: quelle_JJJJ-MM-TT_vN.csv
    teile = NAMENSMUSTER.match(name)
    if not teile or teile["quelle"] != datei["ordner_quelle"]:
        return {**zeile, "status": SCHEMA_GESPERRT, "verantwortlich": VERANTWORTLICH["data_steward"],
                "grund": f"Dateiname entspricht nicht dem Muster {datei['ordner_quelle']}_JJJJ-MM-TT_vN.csv"}
    zeile["lieferdatum"] = datetime.strptime(teile["datum"], "%Y-%m-%d").date()
    zeile["version"] = int(teile["version"])

    # Kopfzeile und Zeilenzahl lesen
    with open(LOKAL + datei["pfad"], "rb") as f:
        bom = f.read(3) == b"\xef\xbb\xbf"
    with open(LOKAL + datei["pfad"], encoding="utf-8-sig") as f:
        kopf = f.readline().strip().split(",")
        zeile["zeilen"] = sum(1 for z in f if z.strip())

    # 1. Schon einmal geliefert?
    gleicher_name = [b for b in bekannte_dateien if b["datei_name"] == name]
    if any(b["datei_hash"] == zeile["datei_hash"] for b in gleicher_name):
        return {**zeile, "status": DUPLIKAT, "grund": "Datei wurde bereits unverändert geliefert"}
    if gleicher_name:
        return {**zeile, "status": SCHEMA_GESPERRT, "verantwortlich": VERANTWORTLICH["data_steward"],
                "grund": "Datei verändert ohne neue Version"}

    # 2. Passen die Spalten zum Datenvertrag?
    erwartet = [s for s, _ in DATENVERTRAG[zeile["quelle"]]]
    if kopf != erwartet:
        fehlt = [s for s in erwartet if s not in kopf]
        zuviel = [s for s in kopf if s not in erwartet]
        if fehlt or zuviel:
            grund = "; ".join(
                ([f"Spalte {', '.join(fehlt)} fehlt"] if fehlt else []) +
                ([f"gefunden: {', '.join(zuviel)}"] if zuviel else []))
            grund = grund.replace("fehlt; gefunden", "fehlt, gefunden")
        else:
            grund = "Spaltenreihenfolge weicht vom Datenvertrag ab"
        return {**zeile, "status": SCHEMA_GESPERRT, "verantwortlich": VERANTWORTLICH["data_steward"],
                "grund": grund}

    # 3. Leer?
    if zeile["zeilen"] == 0:
        return {**zeile, "status": SCHEMA_GESPERRT, "verantwortlich": VERANTWORTLICH["data_steward"],
                "grund": "Datei enthält keine Datenzeilen"}

    hinweis = "Hinweis: Datei mit BOM geliefert, toleriert" if bom else None
    return {**zeile, "status": SCHEMA_OK, "grund": hinweis}


print("✓ Prüfung einer Datei bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 4: Neue Dateien prüfen und ins Register schreiben
#
# Was diese Zelle tut:
#   1. Sie trägt den Start dieses Schritts ins Logbuch ein
#      (steuerung.laeufe).
#   2. Sie liest das Register und sucht in Bronze alle
#      Dateien, die dort noch nicht stehen.
#   3. Sie prüft jede neue Datei mit datei_pruefen
#      (Zelle 3b).
#   4. Neue Version: Ist z. B. v2 schema_ok, bekommt die
#      ältere Version derselben Lieferung den Status
#      ersetzt.
#   5. Sie schreibt alles ins Register
#      (qualitaet.lieferungen) und schließt den
#      Logbuch-Eintrag ab.
#
# Zweimal ausführen ändert nichts: Dateien, die schon
# im Register stehen, werden übersprungen.
# =====================================================

SCHRITT = "NB_02_Dateien_registrieren_Schema_pruefen"
lauf_starten(lauf_id, SCHRITT)

try:
    register = [r.asDict() for r in spark.table("qualitaet.lieferungen").select(
        "lieferung_id", "datei_name", "pfad", "quelle", "lieferdatum",
        "version", "datei_hash", "status", "grund").collect()]

    # 2. + 3. neue Dateien finden und prüfen
    ergebnisse = []
    for datei in neue_dateien_finden({r["pfad"] for r in register}):
        zeile = datei_pruefen(datei, register + ergebnisse)
        zeile["lauf_id"] = lauf_id
        ergebnisse.append(zeile)

    # 4. neue Versionen ersetzen ältere Versionen derselben Lieferung
    ersetzt = {}
    for neu in [e for e in ergebnisse if e["status"] == SCHEMA_OK]:
        for alt in register + ergebnisse:
            if (alt["quelle"] == neu["quelle"]
                    and alt.get("lieferdatum") == neu["lieferdatum"]
                    and (alt.get("version") or 0) < neu["version"]
                    and alt["status"] in (SCHEMA_OK, IN_SILBER, VON_HAND_FREIGEGEBEN, *GESPERRT)):
                ersetzt[alt["lieferung_id"]] = neu["datei_name"]
    for e in ergebnisse:
        if e["lieferung_id"] in ersetzt:
            e["grund"] = (e["grund"] + " · " if e.get("grund") else "") + f"ersetzt durch {ersetzt[e['lieferung_id']]}"
            e["status"] = ERSETZT

    # 5. neue Zeilen ins Register (MERGE auf pfad: nie doppelt)
    if ergebnisse:
        schema = spark.table("qualitaet.lieferungen").schema
        df = spark.createDataFrame([tuple(e.get(f.name) for f in schema) for e in ergebnisse], schema)
        df.createOrReplaceTempView("neue_lieferungen")
        spark.sql("""
            MERGE INTO qualitaet.lieferungen AS ziel
            USING neue_lieferungen AS neu
            ON ziel.pfad = neu.pfad
            WHEN NOT MATCHED THEN INSERT *
        """)

    # ältere Versionen aus früheren Läufen auf ersetzt setzen
    neue_ids = {e["lieferung_id"] for e in ergebnisse}
    for alt in register:
        if alt["lieferung_id"] in ersetzt and alt["lieferung_id"] not in neue_ids:
            grund = (alt["grund"] + " · " if alt["grund"] else "") + f"ersetzt durch {ersetzt[alt['lieferung_id']]}"
            spark.sql(f"""
                UPDATE qualitaet.lieferungen
                SET status = '{ERSETZT}', grund = {sql_text(grund)}
                WHERE lieferung_id = {sql_text(alt['lieferung_id'])}
            """)

    anzahl = {s: sum(1 for e in ergebnisse if e["status"] == s)
              for s in (SCHEMA_OK, SCHEMA_GESPERRT, DUPLIKAT, ERSETZT)}
    lauf_beenden(lauf_id, SCHRITT, "erfolgreich", lieferungen=len(ergebnisse),
                 meldung=", ".join(f"{v} {k}" for k, v in anzahl.items() if v))

except Exception as fehler:
    lauf_beenden(lauf_id, SCHRITT, "fehler", meldung=str(fehler)[:1000])
    raise

print(f"✓ {len(ergebnisse)} neue Dateien geprüft und registriert (Lauf {lauf_id})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 5: Überblick
#
# Zeigt die in diesem Lauf geprüften Dateien mit
# Status und Grund. Gesperrte Dateien sind mit ✗
# markiert, Duplikate und ersetzte mit –.
# =====================================================

ZEICHEN = {SCHEMA_OK: "✓", SCHEMA_GESPERRT: "✗", DUPLIKAT: "–", ERSETZT: "–"}
if not ergebnisse:
    print("Keine neuen Dateien in Bronze.")
for e in ergebnisse:
    print(f"{ZEICHEN.get(e['status'], '?')} {e['datei_name']:<34} {e['status']:<16} {e.get('grund') or ''}")
for alt in register:
    if alt["lieferung_id"] in ersetzt and alt["lieferung_id"] not in neue_ids:
        print(f"– {alt['datei_name']:<34} {ERSETZT:<16} (aus früherem Lauf) durch {ersetzt[alt['lieferung_id']]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
