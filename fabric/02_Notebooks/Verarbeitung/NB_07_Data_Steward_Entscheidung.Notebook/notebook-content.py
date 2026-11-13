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

# # NB_07 Entscheidung über Problemfälle
# 
# **So gehst du vor:**
# 1. **Run all**, ohne etwas einzutragen → Du siehst alle Problemfälle und ihre Fehlerzeilen.
# 2. **Beim Partner nachfragen** und ihm die Fehler nennen.
# 3. Je nach Antwort:
# 
# | Antwort des Partners | Was du tust |
# |---|---|
# | „Wir schicken eine korrigierte Version.“ | **Nichts.** Die neue Version (z. B. `_v2`) ersetzt die alte automatisch und wird erneut geprüft. Korrigierte Zeilen werden automatisch als geklärt markiert. |
# | „Es kommt keine Korrektur.“ | In **Zelle 2a** `lieferung_id` und `bemerkung` eintragen → **Run all** → der Fall ist abgeschlossen. |
# | „Nehmt die gültigen Zeilen trotzdem.“ | Noch nicht vorgesehen. Bitte mit dem Fachbereich klären. |
# | Keine Antwort | Der Tagesbericht markiert Fehler nach 7 Tagen als **überfällig**. Nachhaken. |
# 
# ## Zwei Arten von Problemfällen
# 
# | Fall | Beispiel | Was „abschließen“ bewirkt |
# |---|---|---|
# | **1. Gesperrte Lieferung** | Score 72, nichts davon ist in den Berichten | Die Lieferung wird **verworfen**, nichts davon kommt in die Berichte |
# | **2. Einzelne Fehlerzeilen** | Score 98, 393 Zeilen sind in den Berichten, 7 wurden aussortiert | Die 7 Zeilen gelten als **erledigt**, die 393 bleiben in den Berichten |
# 
# ⚠ **Gesperrte Stammdaten (kunden, stationen, tarife) können nicht verworfen werden.** Sie werden Tag für Tag aufgebaut. Fehlt ein Tag, warten alle folgenden Tage und auch die Ladevorgänge. Hier hilft nur eine korrigierte Version, und die ist **dringend**.
# 
# **Wichtig für den Partner:** Eine Korrektur braucht eine **höhere Versionsnummer** (`_v2`, `_v3` …). Dieselbe Datei mit geändertem Inhalt wird gesperrt („ein Name, ein Inhalt“).
# 
# ## Warum eine Lieferung gesperrt wird
# 
# | Sperrgrund | Geprüft in | Was es bedeutet |
# |---|---|---|
# | **Datei unbrauchbar** (`schema_gesperrt`) | NB_02 | Die Datei passt nicht zum Datenvertrag, Details siehe unten |
# | **Schlüsselfehler** (`schluessel_fehler`) | NB_03 | Stammdaten mit leerem oder doppeltem Schlüssel (R5) |
# | **Qualität zu niedrig** (`score_niedrig`) | NB_04 | Zu viele fehlerhafte Ladevorgänge: Score unter 80 |
# 
# **Datei unbrauchbar** kann heißen:
# - Dateiname entspricht nicht dem Muster `quelle_JJJJ-MM-TT_vN.csv`
# - Datei wurde verändert, aber ohne neue Versionsnummer
# - Spalten fehlen, sind zu viel oder stehen in falscher Reihenfolge
# - Datei enthält keine Datenzeilen
# 
# ## Die Regeln
# 
# | Regel | Gilt für | Fehler, wenn … |
# |---|---|---|
# | R1 | Ladevorgänge | dieselbe `ladevorgang_id` mehrfach vorkommt: mit widersprüchlichen Werten (alle Zeilen fehlerhaft) oder als exakte Kopie (die erste bleibt gültig) |
# | R2 | Ladevorgänge | `energie_kwh` leer oder nicht größer als 0 ist |
# | R3 | Ladevorgänge | `ende_zeit` vor `start_zeit` liegt |
# | R4 | Ladevorgänge | die `station_id` unbekannt ist |
# | R5 | Stammdaten | der Schlüssel leer oder doppelt ist. Schon **eine** Zeile sperrt die ganze Lieferung. |
# 
# **Score** = Anteil fehlerfreier Ladevorgänge in Prozent (100 = fehlerfrei). Unter **80** wird gesperrt.
# 
# ## Gelöscht wird nichts
# 
# Lieferungen und Fehlerzeilen bleiben mit Status `verworfen`, Name, Zeit und Bemerkung dauerhaft erhalten. Die Originaldatei bleibt noch 90 Tage in Bronze. Der Fall erscheint danach nur nicht mehr im Tagesbericht.


# CELL ********************

%run NB_00_Einstellungen_und_Regeln

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 2a (NB_07): Eingabe
#
# Leer lassen → nur Übersicht anzeigen.
# Ausfüllen → Run all. Was passiert, hängt von der
# Lieferung ab (siehe Übersicht):
#   gesperrt      → Lieferung wird verworfen
#   nicht gesperrt → ihre offenen Fehlerzeilen werden
#                    abgeschlossen
# Nur, wenn der Partner KEINE Korrektur schickt.
# =====================================================

lieferung_id = ""
bemerkung    = ""     # Pflicht: was hat der Partner gesagt?

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3a (NB_07): Übersicht
#
# uebersicht_zeigen: drei Tabellen (gesperrte
# Lieferungen, aussortierte Zeilen je Lieferung,
# Zeilen im Detail für die Rückfrage beim Partner).
# zeigen: Titel und Tabelle, leer → „✓ keine“.
# Ändert nichts.
# =====================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def zeigen(titel, df):
    print(titel)
    if df.isEmpty():
        print("   ✓ keine\n")
    else:
        display(df)


def uebersicht_zeigen():
    register = spark.table("qualitaet.lieferungen")
    offen = spark.table("qualitaet.fehlerzeilen").filter("status = 'offen'")
    tag = F.col("lieferdatum").alias("Tag")

    zeigen("1. Gesperrte Lieferungen – noch nicht in den Berichten\n   Bei Eingabe: Lieferung wird verworfen",
           register.filter(F.col("status").isin(GESPERRT)).orderBy("lieferdatum", "quelle").select(
               tag, "lieferung_id", F.col("grund").alias("Grund"), F.col("score").alias("Score"),
               F.concat_ws(" von ", "fehlerzeilen", "zeilen").alias("Fehlerzeilen"),
               F.datediff(F.current_date(), F.coalesce("verarbeitet_am", "eingang_am")).alias("wartet seit Tagen")))

    tage = F.datediff(F.current_date(), F.to_date("seit"))
    zeigen("2. Lieferungen in den Berichten, aber mit aussortierten Zeilen\n   Bei Eingabe: aussortierte Zeilen gelten als erledigt",
           offen.groupBy("lieferung_id").agg(F.count("*").alias("offen"), F.min("erkannt_am").alias("seit"))
               .join(register.filter(~F.col("status").isin(GESPERRT)), "lieferung_id")
               .orderBy("lieferdatum").select(
               tag, "lieferung_id", F.col("offen").alias("aussortierte Zeilen"), tage.alias("seit Tagen"),
               F.when(tage > FRIST_TAGE, "ja").otherwise("nein").alias("überfällig")))

    nr = F.row_number().over(Window.partitionBy("lieferung_id").orderBy("erkannt_am", "rohzeile"))
    zeigen("3. Aussortierte Zeilen im Detail (je Lieferung die ersten 20)",
           offen.withColumn("nr", nr).filter("nr <= 20").orderBy("lieferung_id", "nr").select(
               "lieferung_id", F.col("regel").alias("Regel"),
               F.col("grund").alias("Was ist falsch"), F.col("rohzeile").alias("Originalzeile")))


print("✓ uebersicht_zeigen bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3b (NB_07): Prüfen, was geht
#
# entscheidung_pruefen gibt (Problem, Aktion) zurück:
#   gesperrt, Ladevorgänge  → Aktion "verwerfen"
#   gesperrt, Stammdaten    → Problem: nie verwerfen,
#     NB_03 braucht jeden Tag lückenlos
#   nicht gesperrt, offene Fehlerzeilen
#                           → Aktion "abschliessen"
#   sonst                   → Problem: nichts zu tun
# Bemerkung ist immer Pflicht.
# wer: Name der angemeldeten Person.
# STAMMDATEN_QUELLEN kommt aus NB_00 (Zelle 3b).
# Ändert nichts.
# =====================================================

def wer():
    """Name der angemeldeten Person."""
    try:
        return (notebookutils.runtime.context.get("userName")
                or notebookutils.env.getUserName())
    except Exception:
        return "unbekannt"


def entscheidung_pruefen(lieferung, offene_zeilen):
    if not bemerkung.strip():
        return "Bitte eine Bemerkung eintragen: Was hat der Partner gesagt?", None
    if lieferung is None:
        return f"Lieferung {lieferung_id} gibt es nicht. Bitte die ID aus der Übersicht kopieren.", None
    if lieferung["status"] in GESPERRT:
        if lieferung["quelle"] in STAMMDATEN_QUELLEN:
            return ("Stammdaten können nicht verworfen werden: Die Tage bauen aufeinander auf, "
                    "ohne diesen Tag steht die ganze Verarbeitung still. "
                    "Bitte beim Partner dringend eine korrigierte Version anfordern."), None
        return None, "verwerfen"
    if offene_zeilen == 0:
        if lieferung["status"] == VERWORFEN:
            return (f"Schon verworfen von {lieferung['entschieden_von']} "
                    f"am {lieferung['entschieden_am']:%d.%m.%Y}. Wird nicht überschrieben."), None
        return f"Keine offenen Fehlerzeilen (Status: {lieferung['status']}). Nichts zu tun.", None
    return None, "abschliessen"


print("✓ entscheidung_pruefen und wer bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3c (NB_07): Entscheidung speichern
#
# ausfuehren
#   verwerfen    → Lieferung: Status verworfen, mit
#                  wer, wann, warum. Ihre offenen
#                  Fehlerzeilen → verworfen.
#   abschliessen → nur die offenen Fehlerzeilen →
#                  verworfen, mit wer und warum.
# Gelöscht wird nichts. Eintrag im Logbuch.
# =====================================================

from datetime import datetime


def ausfuehren():
    treffer = spark.table("qualitaet.lieferungen").filter(F.col("lieferung_id") == lieferung_id).collect()
    lieferung = treffer[0].asDict() if treffer else None
    offene_zeilen = spark.table("qualitaet.fehlerzeilen").filter(
        (F.col("lieferung_id") == lieferung_id) & (F.col("status") == "offen")).count()
    problem, aktion = entscheidung_pruefen(lieferung, offene_zeilen)
    if problem:
        print(f"✗ {problem}\n  Es wurde nichts geändert.")
        return

    lauf_id = "manuell-" + datetime.now().strftime("%Y%m%d-%H%M%S")
    schritt, lid, person = "NB_07_Entscheidung", sql_text(lieferung_id), wer()
    lauf_starten(lauf_id, schritt)
    if aktion == "verwerfen":
        spark.sql(f"""
            UPDATE qualitaet.lieferungen SET status = '{VERWORFEN}', entscheidung = 'verwerfen',
                entschieden_von = {sql_text(person)}, entschieden_am = current_timestamp(),
                bemerkung = {sql_text(bemerkung)}
            WHERE lieferung_id = {lid} AND status = '{lieferung['status']}'
        """)
    was = "Lieferung verworfen" if aktion == "verwerfen" else f"{offene_zeilen} Fehlerzeilen abgeschlossen"
    spark.sql(f"""
        UPDATE qualitaet.fehlerzeilen SET status = 'verworfen', geklaert_am = current_timestamp(),
            bemerkung = {sql_text(f'{was} von {person}: {bemerkung}')}
        WHERE lieferung_id = {lid} AND status = 'offen'
    """)
    lauf_beenden(lauf_id, schritt, "erfolgreich", lieferungen=1,
                 meldung=f"{lieferung_id}: {was} von {person}: {bemerkung}"[:1000])
    print(f"✓ {lieferung_id}: {was} von {person}.\n"
          "  Der Fall erscheint nicht mehr im Tagesbericht. Gelöscht wurde nichts.")


print("✓ ausfuehren bereit")

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

# CELL ********************

# =====================================================
# Zelle 4 (NB_07): Ausführen
#
# Zelle 2a ausgefüllt → erst die Entscheidung, dann die
# aktualisierte Übersicht. Leer → nur Übersicht.
# =====================================================

if lieferung_id.strip():
    ausfuehren()
    print()
uebersicht_zeigen()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
