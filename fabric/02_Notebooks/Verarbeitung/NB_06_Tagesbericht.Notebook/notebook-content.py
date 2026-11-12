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
# Lädt NB_00_Einstellungen_und_Regeln. Daraus braucht dieses Notebook die Status-Namen, Grenzwerte (TRENDLAENGE, MENGE_MIN_ANTEIL, FRIST_TAGE), die Verantwortlichen und das Logbuch.

# CELL ********************

%run NB_00_Einstellungen_und_Regeln

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# =====================================================
# Zelle 2a (NB_06): Parameter
#
# lauf_id  Die Nummer des Laufs. Startet die Pipeline
#          dieses Notebook, setzt sie hier ihre eigene
#          Lauf-Nummer ein. Startest du es selbst
#          (Run all), bleibt der Wert leer und Zelle 2b
#          erzeugt eine Nummer wie manuell-20261111-1530.
# =====================================================

lauf_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 2b (NB_06): Lauf-Nummer erzeugen, falls von Hand gestartet
# =====================================================

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
# Zelle 3a (NB_06): Zahlen für den Bericht sammeln
#
# daten_sammeln liest Register, Fehlerzeilen und
# Datenstand und gibt zurück:
#   zu_tun   gesperrte Lieferungen
#   neu      Lieferungen, die in keinem Bericht standen
#   wartet   Lieferungen, die noch auf Prüfung warten
#   anteil   % der neuen Ladevorgangs-Zeilen, die in
#            Ordnung waren (gesperrte zählen als 0)
#   fehler   offene Fehlerzeilen, davon überfällig
#   stand    Zeile aus gold.datenstand
# =====================================================

from pyspark.sql import functions as F
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

BERLIN = ZoneInfo("Europe/Berlin")
HEUTE = datetime.now(BERLIN).date()


def daten_sammeln():
    reg = spark.table("qualitaet.lieferungen")
    sp = ["lieferung_id", "quelle", "lieferdatum", "status", "grund", "zeilen", "fehlerzeilen", "eingang_am"]
    hole = lambda bed: [r.asDict() for r in reg.filter(bed).orderBy("quelle", "lieferdatum").select(*sp).collect()]

    neu = hole(F.col("gemeldet_am").isNull() & (F.col("status") != SCHEMA_OK))
    lv = [n for n in neu if n["quelle"] == "ladevorgaenge" and n["status"] != ERSETZT]
    alle = sum(n["zeilen"] or 0 for n in lv)
    gut = sum((n["zeilen"] or 0) - (n["fehlerzeilen"] or 0) for n in lv if n["status"] == IN_SILBER)

    grenze = datetime.now() - timedelta(days=FRIST_TAGE)
    fehler = (spark.table("qualitaet.fehlerzeilen").filter("status = 'offen'")
              .agg(F.count("*").alias("offen"),
                   F.sum((F.col("erkannt_am") < F.lit(grenze)).cast("int")).alias("ueberfaellig"))
              .first().asDict())
    stand = spark.table("gold.datenstand").first()

    return {"zu_tun": hole(F.col("status").isin(GESPERRT)), "neu": neu,
            "wartet": hole(F.col("status") == SCHEMA_OK), "fehler": fehler,
            "anteil": round(100 * gut / alle, 1) if alle else None,
            "stand": stand.asDict() if stand else {}}


print("✓ daten_sammeln bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3b (NB_06): Qualität und Technik prüfen
# trend_pruefen: Qualität TRENDLAENGE-mal gesunken oder
#   zu wenige Ladevorgänge geliefert? → Warnung
# technik_pruefen: Welcher Schritt ist bei seinem
#   LETZTEN Lauf abgebrochen? Lief die Nacht bis Gold?
# Beide liefern fertige Sätze für den Bericht.
# =====================================================

from statistics import median
from pyspark.sql.window import Window

SCHRITTE = {"NB_02_Dateien_registrieren_Schema_pruefen": "Die Eingangsprüfung (NB_02)",
            "NB_03_Stammdaten_nach_Silber": "Die Prüfung der Stammdaten (NB_03)",
            "NB_04_Ladevorgaenge_pruefen_nach_Silber": "Die Prüfung der Ladevorgänge (NB_04)",
            "NB_05_Gold_aufbauen": "Der Aufbau der Berichtstabellen (NB_05)",
            "NB_08_Pflege": "Die wöchentliche Pflege (NB_08)"}
zahl = lambda x: f"{x:.1f}".replace(".", ",")
zeit = lambda t: t.replace(tzinfo=timezone.utc).astimezone(BERLIN).strftime("%d.%m. um %H:%M")


def trend_pruefen():
    r = (spark.table("qualitaet.lieferungen")
         .filter((F.col("quelle") == "ladevorgaenge") & F.col("status").isin(IN_SILBER, SCORE_NIEDRIG))
         .orderBy(F.col("lieferdatum").desc()).limit(8).collect())[::-1]
    w, s = [], [float(x["score"]) for x in r[-(TRENDLAENGE + 1):]]
    if len(s) > TRENDLAENGE and all(b < a for a, b in zip(s, s[1:])):
        w.append(f"Die Qualität ist {TRENDLAENGE}× in Folge gesunken ({' → '.join(map(zahl, s))}).")
    if len(r) >= 2 and r[-1]["zeilen"] < MENGE_MIN_ANTEIL * median(x["zeilen"] for x in r[:-1]):
        w.append(f"Am {r[-1]['lieferdatum']:%d.%m.} kamen nur {r[-1]['zeilen']} Ladevorgänge.")
    return [(x["lieferdatum"], float(x["score"])) for x in r[-7:]], w


def technik_pruefen():
    lg = spark.table("steuerung.laeufe").filter(F.col("schritt").isin(list(SCHRITTE)))
    nr = F.row_number().over(Window.partitionBy("schritt").orderBy(F.col("start_zeit").desc()))
    s = [f"{SCHRITTE[k['schritt']]} ist am {zeit(k['start_zeit'])} abgebrochen und seitdem nicht "
         f"wieder gelaufen. (Fehler: {(k['meldung'] or '')[:80]})"
         for k in lg.withColumn("nr", nr).filter("nr = 1 AND status = 'fehler'").collect()]
    gold = lg.filter("schritt = 'NB_05_Gold_aufbauen' AND status = 'erfolgreich'").agg(F.max("ende_zeit")).first()[0]
    if not s and not (gold and gold > datetime.now() - timedelta(hours=24)):
        s.append("Die nächtliche Verarbeitung ist in den letzten 24 Std. nicht durchgelaufen"
                 + (f" (zuletzt am {zeit(gold)})." if gold else "."))
    return s


print("✓ trend_pruefen und technik_pruefen bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3c (NB_06): Bericht in Klartext
# Reihenfolge: Zu tun → Neu → Qualität → Stand.
# =====================================================

GRUND = {SCHEMA_GESPERRT: "Datei fehlerhaft", SCHLUESSEL_FEHLER: "Schlüssel fehlen/doppelt",
         SCORE_NIEDRIG: "zu viele fehlerhafte Zeilen"}
dt_ = lambda x: f"{x:%d.%m.%Y}" if x else "–"
ul = lambda z: "<ul>" + "".join(f"<li>{x}</li>" for x in z) + "</ul>"


def bericht_bauen(d, technik, scores, warnungen):
    tun = [f"Lieferung <b>{z['quelle']} vom {dt_(z['lieferdatum'])}</b> ist gesperrt ({GRUND[z['status']]}), "
           f"seit {(HEUTE - z['eingang_am'].date()).days} Tag(en). Bitte mit dem Partner klären."
           for z in d["zu_tun"]]
    if not d["neu"] and not d["wartet"]:
        tun.append("Heute ist keine neue Lieferung eingegangen – bitte beim Partner nachfragen.")
    if d["fehler"]["ueberfaellig"]:
        tun.append(f"{d['fehler']['ueberfaellig']} fehlerhafte Zeilen sind über {FRIST_TAGE} Tage ungeklärt.")
    tun += warnungen + [f"Technik: {t} Der Data Engineer ist informiert." for t in technik]

    q = {}
    for x in d["neu"]:
        q.setdefault(x["quelle"], []).append(x["status"])
    neu = [f"{k}: {len(s)} Lieferung(en), {s.count(IN_SILBER)} übernommen" for k, s in q.items()]
    neu += [f"{w['quelle']} vom {dt_(w['lieferdatum'])} wartet noch auf Prüfung" for w in d["wartet"]]
    if d["anteil"] is not None:
        neu.append(f"{zahl(d['anteil'])} % der gelieferten Ladevorgänge waren fehlerfrei.")

    st, n = d["stand"], len(tun)
    betreff = f"NordLade Tagesbericht {HEUTE:%d.%m.%Y} – " + (
        f"⚠ {n} Punkt{'e' if n > 1 else ''} zu erledigen" if n else "✓ alles in Ordnung")
    fehlt = st.get("nicht_enthalten")
    html = "".join([
        f"<h2>{betreff}</h2><h3>Zu tun</h3>", ul("⚠ " + t for t in tun) if tun else "<p>✓ Nichts zu tun.</p>",
        "<h3>Neu seit dem letzten Bericht</h3>", ul(neu) if neu else "<p>Heute keine neuen Daten.</p>",
        "<h3>Qualität der letzten Lieferungen (100 = fehlerfrei)</h3><p>",
        " · ".join(f"{t:%d.%m.} {zahl(s)}" for t, s in scores), f" – {'sinkt' if warnungen else 'stabil'}.<br>",
        f"{d['fehler']['offen']} fehlerhafte Zeilen warten auf Klärung.</p><h3>Stand der Berichte</h3>",
        f"<p>Power BI enthält alle Ladevorgänge bis einschließlich {dt_(st.get('vollstaendig_bis'))}. ",
        f"Es fehlen: {fehlt}.</p>" if fehlt else "Es fehlt nichts.</p>"])
    return betreff, html, n


print("✓ bericht_bauen bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3d (NB_06): Bericht ablegen, Lieferungen abhaken
#
# bericht_ablegen
#   1. Speichert den Bericht als HTML-Datei unter
#      Files/berichte/tagesbericht_JJJJMMTT-HHMM.html –
#      so ist er auch ohne Mail nachlesbar.
#   2. Setzt im Register gemeldet_am für alle Lieferungen
#      dieses Berichts. Sie erscheinen im nächsten
#      Bericht nicht noch einmal unter „Neu“.
#      (Gesperrte bleiben unter „Zu tun“, bis sie
#      freigegeben oder verworfen sind.)
# Gibt den Pfad der Datei zurück.
# =====================================================

import os

PFAD_BERICHTE = "Files/berichte"


def bericht_ablegen(html, neu):
    """Speichert die HTML-Datei und hakt die gemeldeten Lieferungen ab."""
    jetzt = datetime.now(ZoneInfo("Europe/Berlin"))
    pfad = f"{PFAD_BERICHTE}/tagesbericht_{jetzt:%Y%m%d-%H%M}.html"
    os.makedirs(f"/lakehouse/default/{PFAD_BERICHTE}", exist_ok=True)
    with open(f"/lakehouse/default/{pfad}", "w", encoding="utf-8") as f:
        f.write(f"<html><head><meta charset='utf-8'></head><body>{html}</body></html>")

    ids = ", ".join(sql_text(n["lieferung_id"]) for n in neu)
    if ids:
        spark.sql(f"UPDATE qualitaet.lieferungen SET gemeldet_am = current_timestamp() "
                  f"WHERE lieferung_id IN ({ids})")
    return pfad


print("✓ bericht_ablegen bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 4 (NB_06): Tagesbericht erstellen
#
# Sammeln (3a) → Qualität und Technik (3b) → Bericht (3c)
# → ablegen (3d).
# „Dringend“ für die Pipeline:
#   steward   neue Lieferung mit schluessel_fehler –
#             gesperrte Stammdaten halten alles an
#   engineer  ein Schritt ist abgebrochen oder die
#             Nacht ist nicht durchgelaufen
# Logbuch: Start und Ende in steuerung.laeufe.
# =====================================================

SCHRITT = "NB_06_Tagesbericht"
lauf_starten(lauf_id, SCHRITT)

try:
    daten = daten_sammeln()
    technik = technik_pruefen()
    scores, warnungen = trend_pruefen()
    betreff, html, punkte = bericht_bauen(daten, technik, scores, warnungen)
    datei = bericht_ablegen(html, daten["neu"])
    dringend = {
        "steward": any(n["status"] == SCHLUESSEL_FEHLER for n in daten["neu"]),
        "engineer": bool(technik),
    }
    lauf_beenden(lauf_id, SCHRITT, "erfolgreich", lieferungen=len(daten["neu"]),
                 meldung=f"{betreff} → {datei}")
except Exception as fehler_meldung:
    lauf_beenden(lauf_id, SCHRITT, "fehler", meldung=str(fehler_meldung)[:1000])
    raise

print(f"✓ {betreff}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 5 (NB_06): Bericht zeigen und an die Pipeline geben
#
# Zeigt den Bericht hier im Notebook an.
# Gibt Betreff, HTML und „dringend“ an die Pipeline
# zurück. Dort kann eine Outlook-Aktivität den Bericht
# verschicken (in der Demo deaktiviert).
# Muss die LETZTE Zelle sein: exit beendet das Notebook.
# =====================================================

import json

displayHTML(html)
notebookutils.notebook.exit(json.dumps({
    "betreff": betreff,
    "html": html,
    "datei": datei,
    "an": VERANTWORTLICH["data_steward"],
    "dringend_steward": dringend["steward"],
    "dringend_engineer": dringend["engineer"],
    "an_engineer": VERANTWORTLICH["data_engineer"],
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
