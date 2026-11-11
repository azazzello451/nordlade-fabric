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
# Lädt NB_00_Einstellungen_und_Regeln. Daraus braucht dieses Notebook den Datenvertrag, die Regeln R1–R4, den Score, die Schwelle, die Bilanz, die Verantwortlichen und das Logbuch.

# CELL ********************

%run NB_00_Einstellungen_und_Regeln

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# =====================================================
# Zelle 2a: Parameter
#
# lauf_id  Die Nummer des Laufs. Unter dieser Nummer
#          steht alles, was dieser Lauf tut, im Logbuch
#          und im Register.
#
#          Startet die Pipeline dieses Notebook, setzt
#          sie hier automatisch ihre eigene Lauf-Nummer
#          ein.
#          Startest du das Notebook selbst (Run all),
#          bleibt der Wert leer (""). Dann erzeugt
#          Zelle 2b eine Nummer wie
#          manuell-20261110-1121.
# =====================================================

lauf_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 2b: Lauf-Nummer erzeugen, falls von Hand gestartet
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
# Zelle 3a (NB_04): Werkzeuge – lesen, prüfen, Fehler festhalten
#
# lieferung_lesen
#   Liest eine Ladevorgangs-Datei aus Bronze mit den
#   Spalten und Typen aus dem Datenvertrag (NB_00).
#
# pruefen_und_festhalten
#   Prüft jede Zeile mit R1–R4 (Funktion aus NB_00),
#   berechnet den Score und schreibt jede fehlerhafte
#   Zeile nach qualitaet.fehlerzeilen – mit Regel(n),
#   Grund und Originalzeile.
#   Vorher werden alte Fehlerzeilen dieser Lieferung
#   gelöscht – so ändert ein zweiter Lauf nichts.
#   Gibt die geprüften Zeilen und den Score zurück.
#
# Wie viele Fehler je Regel? → aus qualitaet.fehlerzeilen
# zählen; Zeilen und Score stehen im Register.
# =====================================================

from pyspark.sql import functions as F


def lieferung_lesen(pfad):
    """Liest eine Ladevorgangs-Datei mit den Typen aus dem Datenvertrag."""
    schema = ", ".join(f"{name} {typ}" for name, typ in DATENVERTRAG["ladevorgaenge"])
    return spark.read.option("header", True).schema(schema).csv(pfad)


def pruefen_und_festhalten(df, lieferung):
    """Prüft R1–R4, schreibt die Fehlerzeilen, gibt (geprueft, ergebnis) zurück."""
    stationen = spark.table("silber.stationen").select("station_id")
    geprueft = pruefe_ladevorgaenge(df, stationen).localCheckpoint()   # Funktion aus NB_00
    ergebnis = berechne_score(geprueft)                                 # Funktion aus NB_00
    lid = lieferung["lieferung_id"]

    spark.sql(f"DELETE FROM qualitaet.fehlerzeilen WHERE lieferung_id = {sql_text(lid)}")

    # jede fehlerhafte Zeile einzeln, mit allen verletzten Regeln
    beschreibung = F.create_map(*[x for r, t in REGELN.items() for x in (F.lit(r), F.lit(t))])
    fach = [s for s, _ in DATENVERTRAG["ladevorgaenge"]]
    (geprueft.filter(F.size("fehler") > 0)
        .select(
            F.lit(lid).alias("lieferung_id"),
            "ladevorgang_id",
            F.concat_ws(",", "fehler").alias("regel"),
            F.concat_ws("; ", F.transform("fehler", lambda r: beschreibung[r])).alias("grund"),
            F.concat_ws(",", *[F.coalesce(F.col(s).cast("string"), F.lit("")) for s in fach]).alias("rohzeile"),
            F.current_timestamp().alias("erkannt_am"),
            F.lit("offen").alias("status"),
            F.lit(None).cast("timestamp").alias("geklaert_am"),
            F.lit(None).cast("string").alias("bemerkung"))
        .write.mode("append").saveAsTable("qualitaet.fehlerzeilen"))

    return geprueft, ergebnis


print("✓ lieferung_lesen und pruefen_und_festhalten bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3a: Prüfstation – Teil 2: prüfen, festhalten
#
#   2. Jede Zeile mit R1–R4 aus NB_00 prüfen.
#        []  → in Ordnung    [R2] → kWh fehlt
#   3. Score berechnen (z. B. 7 von 400 fehlerhaft → 98,2).
#   4. Alte Fehlerzeilen dieser Datei löschen (nichts doppelt).
#   5. Jede fehlerhafte Zeile nach qualitaet.fehlerzeilen
#      (Status: offen). Fehler je Regel lassen sich hieraus
#      zählen.
#   6. Geprüfte Zeilen und Score an Zelle 4 zurückgeben.
# Die Entscheidung trifft Zelle 4, Silber schreibt Zelle 3b.
# =====================================================

def pruefen_und_festhalten(df, lieferung):
    """Schritte 2–6: prüfen, Score, Fehlerzeilen schreiben, zurückgeben."""
    stationen = spark.table("silber.stationen").select("station_id")
    geprueft = pruefe_ladevorgaenge(df, stationen).localCheckpoint()   # Funktion aus NB_00
    ergebnis = berechne_score(geprueft)                                 # Funktion aus NB_00
    lid = lieferung["lieferung_id"]

    spark.sql(f"DELETE FROM qualitaet.fehlerzeilen WHERE lieferung_id = {sql_text(lid)}")

    beschreibung = F.create_map(*[x for r, t in REGELN.items() for x in (F.lit(r), F.lit(t))])
    fach = [s for s, _ in DATENVERTRAG["ladevorgaenge"]]
    (geprueft.filter(F.size("fehler") > 0)
        .select(
            F.lit(lid).alias("lieferung_id"),
            "ladevorgang_id",
            F.concat_ws(",", "fehler").alias("regel"),
            F.concat_ws("; ", F.transform("fehler", lambda r: beschreibung[r])).alias("grund"),
            F.concat_ws(",", *[F.coalesce(F.col(s).cast("string"), F.lit("")) for s in fach]).alias("rohzeile"),
            F.current_timestamp().alias("erkannt_am"),
            F.lit("offen").alias("status"),
            F.lit(None).cast("timestamp").alias("geklaert_am"),
            F.lit(None).cast("string").alias("bemerkung"))
        .write.mode("append").saveAsTable("qualitaet.fehlerzeilen"))

    return geprueft, ergebnis


print("✓ pruefen_und_festhalten bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3b: Gültige Zeilen nach Silber
#
# Wird von Zelle 4 nur aufgerufen, wenn der Score die
# Schwelle erreicht. Am Beispiel Ladevorgänge vom 1.7.
# (400 Zeilen, davon 393 gültig, 7 aussortiert):
#
#   1. Die 393 gültigen Zeilen bekommen ihre Herkunft
#      dazu (Lieferung, Lieferdatum, Version, Lauf).
#   2. MERGE nach silber.ladevorgaenge, Schlüssel
#      ladevorgang_id:
#        neue ID          → einfügen
#        ID schon da      → überschreiben, wenn die neue
#                           Lieferung nicht älter ist
#                           („neuere Lieferung gewinnt“)
#   3. Korrektur erkennen: Kommt eine ladevorgang_id
#      jetzt gültig an, die früher als Fehlerzeile
#      aussortiert wurde, wird die alte Fehlerzeile auf
#      geklaert gesetzt (z. B. Tag 65 → Tag 70).
#   4. Bilanz mit den TATSÄCHLICH geschriebenen Zahlen:
#        gelesen (400) = in Silber gemergt (393)
#                      + in Fehlerzeilen geschrieben (7)
#      Stimmt das nicht, stoppt der Lauf mit Fehler.
# =====================================================


def nach_silber(geprueft, ergebnis, lieferung):
    lid = lieferung["lieferung_id"]

    # 1. gültige Zeilen mit Herkunft
    spalten = spark.table("silber.ladevorgaenge").columns
    (geprueft.filter(F.size("fehler") == 0).drop("fehler")
        .withColumn("lieferung_id", F.lit(lid))
        .withColumn("lieferdatum", F.lit(lieferung["lieferdatum"]))
        .withColumn("version", F.lit(lieferung["version"]))
        .withColumn("geprueft_am", F.current_timestamp())
        .withColumn("lauf_id", F.lit(lauf_id))
        .select(*spalten)
        .createOrReplaceTempView("gueltige_zeilen"))

    # 2. MERGE nach Silber
    spark.sql("""
        MERGE INTO silber.ladevorgaenge AS ziel
        USING gueltige_zeilen AS neu
        ON ziel.ladevorgang_id = neu.ladevorgang_id
        WHEN MATCHED AND neu.lieferdatum >= ziel.lieferdatum THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    im_merge = int(spark.sql("DESCRIBE HISTORY silber.ladevorgaenge LIMIT 1")
                        .first()["operationMetrics"]["numSourceRows"])

    # 3. offene Fehlerzeilen früherer Lieferungen klären
    spark.sql(f"""
        MERGE INTO qualitaet.fehlerzeilen AS f
        USING (SELECT DISTINCT ladevorgang_id FROM gueltige_zeilen) AS g
        ON f.ladevorgang_id = g.ladevorgang_id
           AND f.status = 'offen' AND f.lieferung_id <> {sql_text(lid)}
        WHEN MATCHED THEN UPDATE SET status = 'geklaert', geklaert_am = current_timestamp()
    """)

    # 4. Bilanz mit geschriebenen Zahlen
    geschrieben_fehler = spark.table("qualitaet.fehlerzeilen").filter(F.col("lieferung_id") == lid).count()
    pruefe_bilanz(ergebnis["zeilen"], im_merge, geschrieben_fehler, lid)   # Funktion aus NB_00
    return im_merge, geschrieben_fehler


print("✓ Schreiben nach Silber bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 4: Ladevorgänge prüfen und entscheiden
#
# Was hier passiert, Lieferung für Lieferung:
#
#   1. Alle Ladevorgangs-Lieferungen mit schema_ok aus dem
#      Register holen, sortiert nach Lieferdatum.
#   2. Warten, wenn die Stammdaten dieser Nacht noch nicht
#      in Silber sind. Sonst könnte R4 eine neue Station
#      fälschlich als unbekannt melden.
#   3. Prüfen mit der Prüfstation (Zelle 3a) → Score.
#   4. Entscheiden:
#        Score unter SCHWELLE (80)
#          → ganze Lieferung score_niedrig, mit Grund
#            (z. B. Tag 47: Score 76)
#        Score ab 80
#          → gültige Zeilen nach Silber (Zelle 3b),
#            Status in_silber (z. B. Tag 1: Score 98,2)
#   5. Status im Register als Letztes setzen,
#      am Ende den Logbuch-Eintrag.
#
# Eine gesperrte Lieferung hält die anderen Nächte
# nicht auf – Ladevorgänge hängen nicht voneinander ab.
# Zweimal ausführen ändert nichts: fertige
# Lieferungen werden übersprungen.
# =====================================================

SCHRITT = "NB_04_Ladevorgaenge_pruefen_nach_Silber"
lauf_starten(lauf_id, SCHRITT)
protokoll = []
summe = {"gelesen": 0, "gueltig": 0, "fehlerhaft": 0}

try:
    register = spark.table("qualitaet.lieferungen")

    # 1. bis zu welchem Tag sind ALLE Stammdaten in Silber?
    stand = (register.filter(f"status = '{IN_SILBER}' AND quelle IN ('kunden', 'stationen', 'tarife')")
                     .groupBy("quelle").agg(F.max("lieferdatum").alias("bis")).collect())
    stammdaten_bis = min(z["bis"] for z in stand) if len(stand) == 3 else None

    offen = [z.asDict() for z in register
             .filter(f"quelle = 'ladevorgaenge' AND status = '{SCHEMA_OK}'")
             .orderBy("lieferdatum", "version").collect()]

    for lieferung in offen:
        tag = lieferung["lieferdatum"]

        # 2. auf Stammdaten warten
        if stammdaten_bis is None or tag > stammdaten_bis:
            protokoll.append((tag, "wartet", f"Stammdaten erst bis {stammdaten_bis} in Silber"))
            break

        # 3. prüfen
        lid = sql_text(lieferung["lieferung_id"])
        df = lieferung_lesen(lieferung["pfad"])
        geprueft, ergebnis = pruefen_und_festhalten(df, lieferung)
        score = ergebnis["score"]

        # 4. + 5. entscheiden, Status als Letztes
        if score < SCHWELLE:
            spark.sql(f"""
                UPDATE qualitaet.lieferungen SET
                    status = '{SCORE_NIEDRIG}', score = {score}, fehlerzeilen = {ergebnis['fehlerzeilen']},
                    grund = {sql_text(f'Score {score} unter Schwelle {SCHWELLE}')},
                    verantwortlich = {sql_text(VERANTWORTLICH['data_steward'])},
                    verarbeitet_am = current_timestamp()
                WHERE lieferung_id = {lid}
            """)
            protokoll.append((tag, SCORE_NIEDRIG, f"Score {score}"))
            continue                        # andere Nächte laufen weiter

        gueltig, fehlerhaft = nach_silber(geprueft, ergebnis, lieferung)
        spark.sql(f"""
            UPDATE qualitaet.lieferungen SET
                status = '{IN_SILBER}', score = {score}, fehlerzeilen = {fehlerhaft},
                verarbeitet_am = current_timestamp()
            WHERE lieferung_id = {lid}
        """)
        summe["gelesen"] += ergebnis["zeilen"]
        summe["gueltig"] += gueltig
        summe["fehlerhaft"] += fehlerhaft
        protokoll.append((tag, IN_SILBER, f"Score {score}, {gueltig} gültig, {fehlerhaft} aussortiert"))

    verarbeitet = sum(1 for p in protokoll if p[1] == IN_SILBER)
    gesperrt = sum(1 for p in protokoll if p[1] == SCORE_NIEDRIG)
    lauf_beenden(lauf_id, SCHRITT, "erfolgreich", lieferungen=verarbeitet + gesperrt,
                 gelesen=summe["gelesen"], gueltig=summe["gueltig"], fehlerhaft=summe["fehlerhaft"],
                 meldung=f"{verarbeitet} {IN_SILBER}, {gesperrt} {SCORE_NIEDRIG}")

except Exception as fehler_meldung:
    lauf_beenden(lauf_id, SCHRITT, "fehler", meldung=str(fehler_meldung)[:1000])
    raise

print(f"✓ Ladevorgänge: {verarbeitet} in Silber, {gesperrt} gesperrt (Lauf {lauf_id})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 5: Überblick
#
# Zeigt jede bearbeitete Lieferung mit Score und Zahlen:
#   ✓ in_silber   ✗ score_niedrig   … wartet
# Am Ende die Summe als Bilanz.
# Ändert nichts, nur zum Hinschauen.
# =====================================================

ZEICHEN = {IN_SILBER: "✓", SCORE_NIEDRIG: "✗", "wartet": "…"}
for tag, status, text in protokoll:
    print(f"{ZEICHEN[status]} {tag:%d.%m.%Y}  {status:<14} {text}")
print(f"\nSumme: {summe['gelesen']} gelesen = {summe['gueltig']} gültig + {summe['fehlerhaft']} aussortiert")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
