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

# ## Zelle 1: Einstellungen und Regeln laden
# Lädt NB_00_Einstellungen_und_Regeln. Daraus braucht dieses Notebook die Status-Namen, das Logbuch (lauf_starten, lauf_beenden) und sql_text.

# CELL ********************

%run NB_00_Einstellungen_und_Regeln

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# =====================================================
# Zelle 2a : Parameter
#
# lauf_id  Die Nummer des Laufs. Startet die Pipeline
#          dieses Notebook, setzt sie hier ihre eigene
#          Lauf-Nummer ein. Startest du es selbst
#          (Run all), bleibt der Wert leer und Zelle 2b
#          erzeugt eine Nummer wie manuell-20261111-1430.
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
# Zelle 3a: Hilfe – Gold-Tabelle ersetzen
#
# _schreiben
#   Ersetzt den Inhalt einer Gold-Tabelle komplett.
#   Die Tabelle selbst (Spalten, Typen, V-Order aus
#   NB_01) bleibt erhalten – nur die Zeilen sind neu.
#   Die Spalten werden in die Reihenfolge der Tabelle
#   gebracht, damit nichts verrutscht.
# =====================================================

from pyspark.sql import functions as F


def _schreiben(df, tabelle):
    """Ersetzt den Inhalt einer Gold-Tabelle, Aufbau und V-Order bleiben."""
    df.select(*spark.table(tabelle).columns).write.insertInto(tabelle, overwrite=True)


print("✓ _schreiben bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3b: Dimensionen Kunde, Station, Tarif
#
# dimensionen_bauen
#   Baut gold.dim_kunde, gold.dim_station und
#   gold.dim_tarif jedes Mal komplett neu aus Silber.
#   Gold rechnet nichts Fachliches – der Verlauf (SCD 2)
#   wird schon in Silber gepflegt (NB_03). Gold ist nur
#   eine leseoptimierte Kopie für Power BI.
#
# Surrogatschlüssel (_sk): eine Zahl je Zeile, berechnet
# aus ID + gueltig_von (Hash). Dieselbe Zeile bekommt bei
# jedem Neubau dieselbe Zahl – der Fakt zeigt also immer
# auf die richtige Version des Kunden.
# =====================================================

def dimensionen_bauen():
    """Baut dim_kunde, dim_station, dim_tarif neu. Gibt die Zeilenzahlen zurück."""
    kunde = spark.table("silber.kunden").withColumn("kunde_sk", F.xxhash64("kunde_id", "gueltig_von"))
    _schreiben(kunde, "gold.dim_kunde")

    station = spark.table("silber.stationen").withColumn("station_sk", F.xxhash64("station_id", "gueltig_von"))
    _schreiben(station, "gold.dim_station")

    tarif = spark.table("silber.tarife").withColumn("tarif_sk", F.xxhash64("tarif_id"))
    _schreiben(tarif, "gold.dim_tarif")

    return {t: spark.table(f"gold.{t}").count() for t in ("dim_kunde", "dim_station", "dim_tarif")}


print("✓ dimensionen_bauen bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3c: Kalender (gold.dim_datum)
#
# kalender_bauen
#   Eine Zeile je Tag – vom 1.1. bis 31.12. der Jahre,
#   in denen Ladevorgänge liegen. Mit deutschen Namen für
#   Monat und Wochentag, Kalenderwoche (ISO) und
#   Wochenende ja/nein.
#   Wozu: Power BI braucht einen lückenlosen Kalender,
#   auch für Tage ohne Ladevorgang.
#   datum_key = JJJJMMTT als Zahl (z. B. 20260701).
# =====================================================

MONATE = ["Januar", "Februar", "März", "April", "Mai", "Juni", "Juli",
          "August", "September", "Oktober", "November", "Dezember"]
WOCHENTAGE = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]


def kalender_bauen():
    """Baut gold.dim_datum neu. Gibt die Zahl der Tage zurück."""
    jahre = (spark.table("silber.ladevorgaenge")
             .agg(F.min(F.year("start_zeit")).alias("von"), F.max(F.year("start_zeit")).alias("bis"))
             .first())
    von, bis = (jahre["von"], jahre["bis"]) if jahre["von"] else (2026, 2026)

    monate = F.array(*[F.lit(m) for m in MONATE])
    tage = F.array(*[F.lit(t) for t in WOCHENTAGE])
    iso_tag = ((F.dayofweek("datum") + 5) % 7) + 1          # 1 = Montag … 7 = Sonntag

    kalender = (spark.sql(f"SELECT explode(sequence(DATE'{von}-01-01', DATE'{bis}-12-31')) AS datum")
        .withColumn("datum_key", F.date_format("datum", "yyyyMMdd").cast("int"))
        .withColumn("jahr", F.year("datum"))
        .withColumn("quartal", F.quarter("datum"))
        .withColumn("monat", F.month("datum"))
        .withColumn("monat_name", monate[F.month("datum") - 1])
        .withColumn("kalenderwoche", F.weekofyear("datum"))
        .withColumn("wochentag", iso_tag)
        .withColumn("wochentag_name", tage[iso_tag - 1])
        .withColumn("ist_wochenende", iso_tag >= 6))
    _schreiben(kalender, "gold.dim_datum")
    return spark.table("gold.dim_datum").count()


print("✓ kalender_bauen bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3d (NB_05): Fakt – neue Ladevorgänge lesen
#
# neue_ladevorgaenge
#   1. Wasserstand lesen: bis zu welcher Version von
#      silber.ladevorgaenge hat Gold schon gelesen?
#   2. Nur die Änderungen danach holen (Change Data
#      Feed). Kam eine Zeile mehrmals vor, gilt die
#      neueste.
#   3. Schlüssel nachschlagen – so, wie Kunde und Station
#      am LADETAG waren (Ladetag liegt zwischen
#      gueltig_von und gueltig_bis).
#   Gibt (Fakt-Zeilen, von_version, bis_version) zurück;
#   None, wenn es nichts Neues gibt.
# =====================================================

from pyspark.sql.window import Window

QUELLE = "silber.ladevorgaenge"


def neue_ladevorgaenge():
    """Liest neue/geänderte Ladevorgänge und hängt die Schlüssel an."""
    stand = spark.sql(f"SELECT letzte_version FROM steuerung.wasserstand "
                      f"WHERE quell_tabelle = '{QUELLE}'").collect()
    von = stand[0]["letzte_version"] + 1 if stand else 0
    bis = spark.sql(f"DESCRIBE HISTORY {QUELLE} LIMIT 1").first()["version"]
    if von > bis:
        return None, von, bis                                # nichts Neues

    neueste = Window.partitionBy("ladevorgang_id").orderBy(F.col("_commit_version").desc())
    neu = (spark.read.format("delta").option("readChangeFeed", "true")
           .option("startingVersion", von).option("endingVersion", bis).table(QUELLE)
           .filter(F.col("_change_type").isin("insert", "update_postimage"))
           .withColumn("_nr", F.row_number().over(neueste)).filter("_nr = 1")
           .withColumn("ladetag", F.to_date("start_zeit")))

    def dim(tabelle, id_spalte, sk):
        d = spark.table(tabelle)
        return d.select(d[id_spalte].alias("_id"), sk, d.gueltig_von.alias("_von"), d.gueltig_bis.alias("_bis"))

    def nachschlagen(df, dimension, id_spalte):
        bedingung = (df[id_spalte] == dimension._id) & df.ladetag.between(dimension._von, dimension._bis)
        return df.join(dimension, bedingung, "left").drop("_id", "_von", "_bis")

    tarif = spark.table("gold.dim_tarif").select(F.col("tarif_id").alias("_tid"), "tarif_sk")
    neu = nachschlagen(neu, dim("gold.dim_kunde", "kunde_id", "kunde_sk"), "kunde_id")
    neu = nachschlagen(neu, dim("gold.dim_station", "station_id", "station_sk"), "station_id")
    neu = neu.join(tarif, neu.tarif_id == tarif._tid, "left")

    fakt = neu.select(
        "ladevorgang_id",
        F.date_format("ladetag", "yyyyMMdd").cast("int").alias("datum_key"),
        "kunde_sk", "station_sk", "tarif_sk",
        "ladepunkt_id", "start_zeit", "ende_zeit",
        ((F.unix_timestamp("ende_zeit") - F.unix_timestamp("start_zeit")) / 60).cast("int").alias("dauer_minuten"),
        "energie_kwh", "umsatz_eur", "marge_eur", "lieferung_id")
    return fakt.localCheckpoint(), von, bis


print("✓ neue_ladevorgaenge bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3e (NB_05): Fakt – schreiben und Wasserstand setzen
#
# fakt_schreiben
#   1. Per MERGE in gold.fakt_ladevorgang: neue
#      Ladevorgänge einfügen, korrigierte überschreiben.
#   2. Wasserstand auf die gelesene Silber-Version setzen
#      – erst NACH dem Schreiben. Bricht vorher etwas ab,
#      liest der nächste Lauf dieselben Änderungen noch
#      einmal; der MERGE macht das ohne Doppelte.
#   Gibt die Zahl der geschriebenen Ladevorgänge zurück.
# =====================================================

def fakt_schreiben(fakt, bis):
    """Schreibt die Fakt-Zeilen per MERGE und setzt den Wasserstand."""
    fakt.createOrReplaceTempView("neue_fakten")
    spark.sql("""
        MERGE INTO gold.fakt_ladevorgang AS ziel
        USING neue_fakten AS neu
        ON ziel.ladevorgang_id = neu.ladevorgang_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.sql(f"""
        MERGE INTO steuerung.wasserstand AS ziel
        USING (SELECT '{QUELLE}' AS quell_tabelle, CAST({bis} AS BIGINT) AS letzte_version,
                      current_timestamp() AS aktualisiert_am) AS neu
        ON ziel.quell_tabelle = neu.quell_tabelle
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return fakt.count()


print("✓ fakt_schreiben bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3f (NB_05): Datenstand (gold.datenstand)
#
# datenstand_schreiben
#   Eine einzige Zeile, die im Bericht oben steht:
#     letzte_lieferung   neuester Liefertag in Silber
#     vollstaendig_bis   letzter LADETAG ohne Lücke davor.
#                        Lieferung vom Tag D enthält die
#                        Ladevorgänge von D-1.
#     nicht_enthalten    gesperrte Lieferungen, die fehlen
#                        (z. B. „16.08.2026 (score_niedrig)“)
#   Wozu: Wer den Bericht liest, sieht sofort, ob die
#   Zahlen vollständig sind – statt sich zu wundern.
# =====================================================

from datetime import timedelta


def datenstand_schreiben():
    """Schreibt gold.datenstand neu. Gibt die Zeile als dict zurück."""
    register = spark.table("qualitaet.lieferungen").filter("quelle = 'ladevorgaenge'")
    da = sorted({r["lieferdatum"] for r in register
                 .filter(F.col("status").isin(IN_SILBER, VON_HAND_FREIGEGEBEN))
                 .select("lieferdatum").distinct().collect()})
    gesperrt = (register.filter(F.col("status").isin(GESPERRT))
                .orderBy("lieferdatum").select("lieferdatum", "status").collect())

    letzte = da[-1] if da else None
    lueckenlos = da[0] if da else None
    for tag in da[1:]:
        if tag != lueckenlos + timedelta(days=1):
            break
        lueckenlos = tag

    zeile = {
        "letzte_lieferung": letzte,
        "vollstaendig_bis": lueckenlos - timedelta(days=1) if lueckenlos else None,
        "nicht_enthalten": ", ".join(f"{r['lieferdatum']:%d.%m.%Y} ({r['status']})" for r in gesperrt) or None,
    }
    neu = (spark.createDataFrame([zeile], "letzte_lieferung DATE, vollstaendig_bis DATE, nicht_enthalten STRING")
           .withColumn("aktualisiert_am", F.current_timestamp()))
    _schreiben(neu, "gold.datenstand")
    return zeile


print("✓ datenstand_schreiben bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 4: Gold aufbauen
#
# Reihenfolge:
#   1. Dimensionen (3b) und Kalender (3c) – zuerst,
#      damit der Fakt sie nachschlagen kann.
#   2. Fakt nur mit neuen Ladevorgängen (3d, 3e).
#   3. Datenstand (3f).
# Logbuch: Start und Ende in steuerung.laeufe.
# Zweimal ausführen ändert nichts: Dimensionen werden
# gleich neu gebaut, der Fakt findet nichts Neues.
# =====================================================

SCHRITT = "NB_05_Gold_aufbauen"
lauf_starten(lauf_id, SCHRITT)

try:
    dims = dimensionen_bauen()
    dims["dim_datum"] = kalender_bauen()
    fakt, von, bis = neue_ladevorgaenge()
    neu = fakt_schreiben(fakt, bis) if fakt is not None else 0
    stand = datenstand_schreiben()
    was = f"{neu} Ladevorgänge nach Gold (Silber-Version {von}–{bis})" if neu else "keine neuen Ladevorgänge"
    lauf_beenden(lauf_id, SCHRITT, "erfolgreich", gelesen=neu,
                 meldung=f"{was}, vollständig bis Ladetag {stand['vollstaendig_bis']}")
except Exception as fehler_meldung:
    lauf_beenden(lauf_id, SCHRITT, "fehler", meldung=str(fehler_meldung)[:1000])
    raise

print(f"✓ Gold: {neu} Ladevorgänge neu oder geändert (Lauf {lauf_id})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 5: Überblick
#
# Zeigt die Größe jeder Gold-Tabelle und den Datenstand.
# Kontrolle: Im Szenario hat jeder Ladevorgang einen
# Kunden, eine Station und einen Tarif. Findet einer
# trotzdem keinen (leerer Schlüssel), steht hier eine
# Warnung – so fällt es auf, statt still im Bericht zu
# fehlen. Ändert nichts, nur zum Hinschauen.
# =====================================================

for name, anzahl in dims.items():
    print(f"  {name:<18} {anzahl:>7} Zeilen")
fakt = spark.table("gold.fakt_ladevorgang")
print(f"  {'fakt_ladevorgang':<18} {fakt.count():>7} Zeilen ({neu} in diesem Lauf)")

ohne = fakt.filter("kunde_sk IS NULL OR station_sk IS NULL OR tarif_sk IS NULL").count()
print(f"\n{'✓' if ohne == 0 else '⚠'} {ohne} Ladevorgänge ohne passenden Kunden, Station oder Tarif")
print(f"\nDatenstand: letzte Lieferung {stand['letzte_lieferung']}, "
      f"vollständig bis Ladetag {stand['vollstaendig_bis']}")
print(f"Nicht enthalten: {stand['nicht_enthalten'] or '–'}")

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
