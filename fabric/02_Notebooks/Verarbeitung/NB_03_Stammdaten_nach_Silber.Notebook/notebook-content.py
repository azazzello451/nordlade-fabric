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
# Lädt NB_00_Einstellungen_und_Regeln. Daraus braucht dieses Notebook den Datenvertrag, die Schlüssel der Stammdaten, Regel R5, die Verantwortlichen und das Logbuch.

# CELL ********************

%run NB_00_Einstellungen_und_Regeln

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 2: Parameter
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
# Zelle 3a: Werkzeuge – Lieferung lesen und R5 prüfen
#
# lieferung_lesen
#   Liest eine Stammdaten-Datei aus Bronze mit den
#   Spalten und Typen aus dem Datenvertrag (NB_00).
#
# r5_pruefen
#   Prüft Regel R5 (Schlüssel leer oder doppelt) und
#   schreibt das Ergebnis in qualitaet.regelergebnis
#   und jede fehlerhafte Zeile in qualitaet.fehlerzeilen.
#   Vorher werden alte Ergebnisse dieser Lieferung
#   gelöscht – so ändert ein zweiter Lauf nichts.
#   Gibt die Zahl der fehlerhaften Zeilen zurück.
# =====================================================

from pyspark.sql import functions as F

# Welche Spalten sich wie verhalten (SCD = Slowly Changing Dimension)
STAMMDATEN = {
    "kunden":    {"ohne": ["email"], "scd2": ["heimatregion", "tarif_id"], "scd1": ["name"]},
    "stationen": {"ohne": [],        "scd2": ["region", "betreiber"],     "scd1": ["name"]},
    "tarife":    {"ohne": [],        "scd2": [],                          "scd1": ["name", "preis_pro_kwh"]},
}


def lieferung_lesen(pfad, quelle):
    """Liest eine Datei aus Bronze mit den Typen aus dem Datenvertrag."""
    schema = ", ".join(f"{name} {typ}" for name, typ in DATENVERTRAG[quelle])
    return spark.read.option("header", True).schema(schema).csv(pfad)


def r5_pruefen(df, lieferung):
    """Prüft R5, schreibt Regelergebnis und Fehlerzeilen, gibt die Fehlerzahl zurück."""
    quelle = lieferung["quelle"]
    schluessel = SCHLUESSEL[quelle]
    geprueft = pruefe_stammdaten(df, quelle)            # Funktion aus NB_00
    fehler = geprueft.filter(F.size("fehler") > 0)
    anzahl_fehler = fehler.count()
    lid = sql_text(lieferung["lieferung_id"])

    spark.sql(f"DELETE FROM qualitaet.regelergebnis WHERE lieferung_id = {lid}")
    spark.sql(f"DELETE FROM qualitaet.fehlerzeilen WHERE lieferung_id = {lid}")

    spark.createDataFrame(
        [(lieferung["lieferung_id"], "R5", df.count(), anzahl_fehler, lauf_id)],
        "lieferung_id STRING, regel STRING, zeilen_geprueft BIGINT, zeilen_fehlerhaft BIGINT, lauf_id STRING",
    ).withColumn("geprueft_am", F.current_timestamp()) \
     .select("lieferung_id", "regel", "zeilen_geprueft", "zeilen_fehlerhaft", "geprueft_am", "lauf_id") \
     .write.mode("append").saveAsTable("qualitaet.regelergebnis")

    if anzahl_fehler:
        fach = [s for s, _ in DATENVERTRAG[quelle]]
        (fehler
            .select(
                F.lit(lieferung["lieferung_id"]).alias("lieferung_id"),
                F.lit(None).cast("string").alias("ladevorgang_id"),
                F.lit("R5").alias("regel"),
                F.concat(F.lit(f"{schluessel} leer oder doppelt: "),
                         F.coalesce(F.col(schluessel), F.lit("(leer)"))).alias("grund"),
                F.concat_ws(",", *[F.col(s).cast("string") for s in fach]).alias("rohzeile"),
                F.current_timestamp().alias("erkannt_am"),
                F.lit("offen").alias("status"),
                F.lit(None).cast("timestamp").alias("geklaert_am"),
                F.lit(None).cast("string").alias("bemerkung"))
            .write.mode("append").saveAsTable("qualitaet.fehlerzeilen"))
    return anzahl_fehler


print("✓ lieferung_lesen und r5_pruefen bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3b: Werkzeug – Stammdaten nach Silber schreiben
#
# nach_silber übernimmt einen geprüften Stand:
#   SCD Typ 1 (überschreiben):
#     Namen (Kunden, Stationen) und alle Tarif-Spalten.
#     Ein korrigierter Name gilt in ALLEN Versionen.
#   SCD Typ 2 (Verlauf):
#     Kunden: Heimatregion, Tarif
#     Stationen: Region, Betreiber
#     Ändert sich einer dieser Werte, wird die aktuelle
#     Zeile geschlossen (gueltig_bis, ist_aktuell = false)
#     und eine neue Zeile angelegt.
#   Ein Hash über die Verlaufs-Spalten erkennt
#   unveränderte Zeilen – sie bekommen keine neue Version.
#
# Stichtag: Der Stand einer Nacht beschreibt den Vortag
# (wie die Ladevorgänge dieser Nacht). Darum gilt eine
# neue Version ab lieferdatum − 1 Tag.
# =====================================================

from datetime import timedelta

OFFEN_BIS = "9999-12-31"


def _hash(spalten):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(s).cast("string"), F.lit("")) for s in spalten]), 256)


def nach_silber(df, lieferung):
    """Übernimmt einen geprüften Stammdaten-Stand nach Silber. Gibt die Zahl neuer Versionen zurück."""
    quelle = lieferung["quelle"]
    regel = STAMMDATEN[quelle]
    tabelle = f"silber.{quelle}"
    schluessel = SCHLUESSEL[quelle]
    stichtag = lieferung["lieferdatum"] - timedelta(days=1)

    neu = (df.drop(*regel["ohne"])
             .withColumn("lieferung_id", F.lit(lieferung["lieferung_id"]))
             .withColumn("lieferdatum", F.lit(lieferung["lieferdatum"]))
             .withColumn("version", F.lit(lieferung["version"]))
             .withColumn("geprueft_am", F.current_timestamp())
             .withColumn("lauf_id", F.lit(lauf_id)))
    neu.createOrReplaceTempView("neuer_stand")

    # --- SCD Typ 1: überschreiben (in allen vorhandenen Versionen) ---
    setze = ", ".join(f"ziel.{s} = neu.{s}" for s in regel["scd1"])
    anders = " OR ".join(f"NOT (ziel.{s} <=> neu.{s})" for s in regel["scd1"])
    spark.sql(f"""
        MERGE INTO {tabelle} AS ziel USING neuer_stand AS neu
        ON ziel.{schluessel} = neu.{schluessel}
        WHEN MATCHED AND ({anders}) THEN UPDATE SET {setze}
    """)

    if not regel["scd2"]:                       # Tarife: nur überschreiben + neue anlegen
        spark.sql(f"""
            MERGE INTO {tabelle} AS ziel USING neuer_stand AS neu
            ON ziel.{schluessel} = neu.{schluessel}
            WHEN NOT MATCHED THEN INSERT *
        """)
        return None

    # --- SCD Typ 2: Verlauf ---
    aktuell = (spark.table(tabelle).filter("ist_aktuell")
                    .select(schluessel, _hash(regel["scd2"]).alias("_alt")))
    # localCheckpoint: Vergleich jetzt festhalten, bevor die Tabelle unten geändert wird
    vergleich = (neu.withColumn("_neu", _hash(regel["scd2"]))
                    .join(aktuell, schluessel, "left").localCheckpoint())
    geaendert = vergleich.filter(F.col("_alt").isNotNull() & (F.col("_alt") != F.col("_neu")))
    ganz_neu = vergleich.filter(F.col("_alt").isNull())

    # 1. aktuelle Version geänderter Zeilen schließen
    geaendert.select(schluessel).createOrReplaceTempView("geaendert")
    spark.sql(f"""
        MERGE INTO {tabelle} AS ziel USING geaendert AS g
        ON ziel.{schluessel} = g.{schluessel} AND ziel.ist_aktuell
        WHEN MATCHED THEN UPDATE SET
            gueltig_bis = DATE '{stichtag - timedelta(days=1)}',
            ist_aktuell = false
    """)

    # 2. neue Versionen anlegen (geänderte und ganz neue Schlüssel)
    spalten = spark.table(tabelle).columns
    neue_versionen = (geaendert.unionByName(ganz_neu)
                        .withColumn("gueltig_von", F.lit(stichtag))
                        .withColumn("gueltig_bis", F.lit(OFFEN_BIS).cast("date"))
                        .withColumn("ist_aktuell", F.lit(True))
                        .select(*spalten))
    anzahl = neue_versionen.count()
    neue_versionen.write.mode("append").saveAsTable(tabelle)
    return anzahl


print("✓ nach_silber bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 4: Stammdaten in Datumsreihenfolge verarbeiten
#
# Was diese Zelle tut, je Quelle (kunden, stationen,
# tarife):
#   1. Holt alle Lieferungen mit schema_ok aus dem
#      Register, sortiert nach Lieferdatum.
#   2. Streng der Reihe nach: Fehlt ein Tag (noch nicht
#      geliefert oder gesperrt), hält diese Quelle an.
#      Die späteren Tage warten im Status schema_ok.
#      Grund: Ein Verlauf stimmt nur in der richtigen
#      Reihenfolge.
#   3. Je Lieferung: R5 prüfen.
#        Fehler → schluessel_fehler, Quelle hält an.
#        sonst  → nach Silber (SCD 1 und 2), in_silber.
#   4. Der Status im Register wird als Letztes gesetzt,
#      danach der Logbuch-Eintrag.
# Zweimal ausführen ändert nichts: fertige
# Lieferungen werden übersprungen.
# =====================================================

from datetime import timedelta

SCHRITT = "NB_03_Stammdaten_nach_Silber"
lauf_starten(lauf_id, SCHRITT)
protokoll = []          # für den Überblick in Zelle 5

try:
    register = spark.table("qualitaet.lieferungen").filter(F.col("quelle").isin(list(STAMMDATEN)))

    for quelle in STAMMDATEN:
        r = register.filter(F.col("quelle") == quelle)
        letzter = r.filter(F.col("status") == IN_SILBER).agg(F.max("lieferdatum")).first()[0]
        offen = [z.asDict() for z in r.filter(F.col("status") == SCHEMA_OK)
                                    .orderBy("lieferdatum", "version").collect()]

        for lieferung in offen:
            tag = lieferung["lieferdatum"]
            if letzter is not None and tag <= letzter:
                # kam zu spät: ein neuerer Stand ist schon in Silber → nicht anwenden
                spark.sql(f"""
                    UPDATE qualitaet.lieferungen SET status = '{ERSETZT}',
                        grund = 'älter als der bereits verarbeitete Stand – nicht angewendet'
                    WHERE lieferung_id = {sql_text(lieferung['lieferung_id'])}
                """)
                protokoll.append((quelle, tag, ERSETZT, "älter als der verarbeitete Stand"))
                continue
            if letzter is not None and tag != letzter + timedelta(days=1):
                protokoll.append((quelle, tag, "wartet", f"Lücke: {letzter + timedelta(days=1)} fehlt noch"))
                break

            df = lieferung_lesen(lieferung["pfad"], quelle)
            fehler = r5_pruefen(df, lieferung)
            lid = sql_text(lieferung["lieferung_id"])

            if fehler:
                spark.sql(f"""
                    UPDATE qualitaet.lieferungen SET
                        status = '{SCHLUESSEL_FEHLER}', fehlerzeilen = {fehler},
                        grund = {sql_text(f'R5: {fehler} Zeilen mit leerem oder doppeltem Schlüssel')},
                        verantwortlich = {sql_text(VERANTWORTLICH['data_steward'])},
                        verarbeitet_am = current_timestamp()
                    WHERE lieferung_id = {lid}
                """)
                protokoll.append((quelle, tag, SCHLUESSEL_FEHLER, f"R5: {fehler} Zeilen"))
                break                                   # Quelle hält an

            neue = nach_silber(df, lieferung)
            spark.sql(f"""
                UPDATE qualitaet.lieferungen SET
                    status = '{IN_SILBER}', fehlerzeilen = 0, verarbeitet_am = current_timestamp()
                WHERE lieferung_id = {lid}
            """)
            letzter = tag
            protokoll.append((quelle, tag, IN_SILBER,
                              "" if neue is None else f"{neue} neue Versionen"))

    verarbeitet = sum(1 for p in protokoll if p[2] == IN_SILBER)
    gesperrt = sum(1 for p in protokoll if p[2] == SCHLUESSEL_FEHLER)
    lauf_beenden(lauf_id, SCHRITT, "erfolgreich", lieferungen=verarbeitet + gesperrt,
                 meldung=f"{verarbeitet} {IN_SILBER}, {gesperrt} {SCHLUESSEL_FEHLER}")

except Exception as fehler_meldung:
    lauf_beenden(lauf_id, SCHRITT, "fehler", meldung=str(fehler_meldung)[:1000])
    raise

print(f"✓ Stammdaten: {verarbeitet} in Silber, {gesperrt} gesperrt (Lauf {lauf_id})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 5: Überblick
#
# Zeigt je Quelle, wie viele Tage in Silber kamen,
# und alle Besonderheiten: neue Versionen (Verlauf),
# Sperren, wartende Tage wegen einer Lücke.
# =====================================================

for quelle in STAMMDATEN:
    zeilen = [p for p in protokoll if p[0] == quelle]
    fertig = [p for p in zeilen if p[2] == IN_SILBER]
    if fertig:
        print(f"{quelle:<10} {len(fertig)} Tage in Silber ({fertig[0][1]:%d.%m.} bis {fertig[-1][1]:%d.%m.})")
    else:
        print(f"{quelle:<10} nichts Neues in Silber")
    for p in zeilen:
        if p[2] != IN_SILBER or p[3] not in ("", "0 neue Versionen"):
            print(f"           {p[1]:%d.%m.%Y}  {p[2]:<18} {p[3]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
