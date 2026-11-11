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
# daten_sammeln liest und gibt als dict zurück:
#   zu_tun     gesperrte Lieferungen (mit Grund, seit wann)
#   fehler     offene Fehlerzeilen; davon älter als
#              FRIST_TAGE (= Erinnerung)
#   neu        Lieferungen, die noch in keinem Bericht
#              standen (gemeldet_am leer, Status fertig)
#   wartet     Lieferungen noch im Status schema_ok
#   anteil     % der neuen Ladevorgangs-Zeilen, die nach
#              Silber durften (gesperrte zählen als 0)
#   technik    Schritte mit Status „fehler“ (24 Std.)
#   stand      Zeile aus gold.datenstand
# =====================================================

from pyspark.sql import functions as F
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

HEUTE = datetime.now(ZoneInfo("Europe/Berlin")).date()


def daten_sammeln():
    """Liest Register, Fehlerzeilen, Logbuch und Datenstand."""
    reg = spark.table("qualitaet.lieferungen")
    spalten = ["lieferung_id", "quelle", "lieferdatum", "status", "grund", "score", "zeilen", "fehlerzeilen", "eingang_am"]

    zu_tun = [r.asDict() for r in reg.filter(F.col("status").isin(GESPERRT)).orderBy("lieferdatum").select(*spalten).collect()]
    neu = [r.asDict() for r in reg.filter(F.col("gemeldet_am").isNull() & (F.col("status") != SCHEMA_OK))
                                  .orderBy("quelle", "lieferdatum").select(*spalten).collect()]
    wartet = [r.asDict() for r in reg.filter(F.col("status") == SCHEMA_OK).orderBy("lieferdatum").select(*spalten).collect()]

    lv = [n for n in neu if n["quelle"] == "ladevorgaenge" and n["status"] != ERSETZT]
    alle = sum(n["zeilen"] or 0 for n in lv)
    gut = sum((n["zeilen"] or 0) - (n["fehlerzeilen"] or 0) for n in lv if n["status"] == IN_SILBER)

    offen = spark.table("qualitaet.fehlerzeilen").filter("status = 'offen'")
    grenze = datetime.now() - timedelta(days=FRIST_TAGE)
    fehler = offen.agg(F.count("*").alias("offen"), F.min("erkannt_am").alias("aelteste"),
                       F.sum((F.col("erkannt_am") < F.lit(grenze)).cast("int")).alias("ueberfaellig")).first().asDict()

    technik = [r.asDict() for r in spark.table("steuerung.laeufe")
               .filter((F.col("status") == "fehler") & (F.col("start_zeit") > F.lit(datetime.now() - timedelta(hours=24))))
               .select("schritt", "meldung").collect()]
    stand = spark.table("gold.datenstand").first()

    return {"zu_tun": zu_tun, "neu": neu, "wartet": wartet, "fehler": fehler, "technik": technik,
            "anteil": round(100 * gut / alle, 1) if alle else None, "stand": stand.asDict() if stand else {}}


print("✓ daten_sammeln bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3b (NB_06): Trend der Ladevorgänge
#
# trend_pruefen schaut auf die letzten Ladevorgangs-
# Lieferungen und warnt (sperrt nicht):
#   Score TRENDLAENGE-mal in Folge gefallen
#   Zeilen unter MENGE_MIN_ANTEIL des Üblichen
#     (üblich = Median der 7 Lieferungen davor)
# Gibt die letzten Scores und die Warnungen zurück.
# =====================================================

from statistics import median


def trend_pruefen():
    """Gibt (scores der letzten 7 Tage, Liste der Warnungen) zurück."""
    letzte = (spark.table("qualitaet.lieferungen")
              .filter((F.col("quelle") == "ladevorgaenge") & F.col("status").isin(IN_SILBER, SCORE_NIEDRIG))
              .orderBy(F.col("lieferdatum").desc()).limit(8)
              .select("lieferdatum", "score", "zeilen").collect())[::-1]
    warnungen = []
    if len(letzte) > TRENDLAENGE:
        s = [float(r["score"]) for r in letzte[-(TRENDLAENGE + 1):]]
        if all(b < a for a, b in zip(s, s[1:])):
            warnungen.append(f"Score {TRENDLAENGE}× in Folge gefallen ({' → '.join(f'{x:.1f}' for x in s)})")
    if len(letzte) >= 2:
        ueblich = median(r["zeilen"] for r in letzte[:-1])
        if letzte[-1]["zeilen"] < MENGE_MIN_ANTEIL * ueblich:
            warnungen.append(f"nur {letzte[-1]['zeilen']} Zeilen am {letzte[-1]['lieferdatum']:%d.%m.} "
                             f"(üblich ca. {ueblich:.0f})")
    return [(r["lieferdatum"], float(r["score"])) for r in letzte[-7:]], warnungen


print("✓ trend_pruefen bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3c (NB_06): Bericht als HTML zusammensetzen
# Reihenfolge: Zu tun → Neu → Trend → Datenstand.
# Gibt (betreff, html, punkte) zurück.
# =====================================================

from html import escape


def _d(x):
    return f"{x:%d.%m.%Y}" if x else "–"


def _liste(z):
    return "<ul>" + "".join(f"<li>{x}</li>" for x in z) + "</ul>" if z else "<p>–</p>"


def bericht_bauen(d, scores, warnungen):
    tun = [f"⚠ {z['quelle']} {_d(z['lieferdatum'])} – <b>{z['status']}</b>: {escape(z['grund'] or '')}"
           f" – seit {(HEUTE - z['eingang_am'].date()).days} Tag(en)" for z in d["zu_tun"]]
    if d["fehler"]["ueberfaellig"]:
        tun.append(f"⚠ {d['fehler']['ueberfaellig']} Fehlerzeilen länger als {FRIST_TAGE} Tage offen")
    tun += [f"⚠ Trend: {escape(w)}" for w in warnungen]
    tun += [f"⚠ Technik: {t['schritt']} – {escape((t['meldung'] or '')[:200])}" for t in d["technik"]]

    q = {}
    for n in d["neu"]:
        q.setdefault(n["quelle"], []).append(n["status"])
    neu = [f"{k}: {len(s)} ({', '.join(f'{s.count(x)} {x}' for x in sorted(set(s)))})" for k, s in q.items()]
    neu += [f"wartet noch: {w['quelle']} {_d(w['lieferdatum'])}" for w in d["wartet"]]

    st = d["stand"]
    betreff = f"NordLade Tagesbericht {HEUTE:%d.%m.%Y} – " + (
        f"⚠ {len(tun)} Punkt(e) zu erledigen" if tun else "✓ alles in Ordnung")
    teile = [
        f"<h2>{escape(betreff)}</h2><h3>Zu tun</h3>", _liste(tun) if tun else "<p>✓ nichts zu tun</p>",
        f"<p>Offene Fehlerzeilen insgesamt: {d['fehler']['offen']}</p>",
        "<h3>Neu seit dem letzten Bericht</h3>", _liste(neu),
        f"<p>Durchgelassen: <b>{d['anteil'] or '–'} %</b> der neuen Ladevorgangs-Zeilen</p>",
        "<h3>Trend</h3><p>" + (" · ".join(f"{t:%d.%m.} {s:.1f}" for t, s in scores) or "–"),
        (" – ⚠ siehe oben" if warnungen else " – ✓ stabil") + "</p><h3>Datenstand</h3>",
        f"<p>Vollständig bis Ladetag <b>{_d(st.get('vollstaendig_bis'))}</b>, "
        f"nicht enthalten: {escape(st.get('nicht_enthalten') or '–')}</p>",
    ]
    return betreff, "\n".join(teile), len(tun)


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
# Sammeln (3a) → Trend (3b) → Bericht (3c) → ablegen (3d).
# „Dringend“ für die Pipeline:
#   steward   neue Lieferung mit schluessel_fehler –
#             Stammdaten gesperrt hält die ganze Strecke an
#   engineer  ein technischer Schritt ist gescheitert
# Logbuch: Start und Ende in steuerung.laeufe.
# =====================================================

SCHRITT = "NB_06_Tagesbericht"
lauf_starten(lauf_id, SCHRITT)

try:
    daten = daten_sammeln()
    scores, warnungen = trend_pruefen()
    betreff, html, punkte = bericht_bauen(daten, scores, warnungen)
    datei = bericht_ablegen(html, daten["neu"])
    dringend = {
        "steward": any(n["status"] == SCHLUESSEL_FEHLER for n in daten["neu"]),
        "engineer": bool(daten["technik"]),
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
