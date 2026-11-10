# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# =====================================================
# Zelle 1: Einstellungen
#
# Alle festen Werte des Projekts an einer Stelle:
#   - Grenzwerte für die Qualitätsprüfung,
#   - Fristen und Aufbewahrungszeiten,
#   - wer bei Problemen informiert wird,
#   - Ordnerpfade, Quellen und Schemas.
#
# Wer einen Wert ändern will, ändert ihn nur hier.
# Alle anderen Notebooks übernehmen ihn automatisch.
# =====================================================

# --- Qualität ---
SCHWELLE = 80              # Score unter 80: Lieferung in Quarantäne
TRENDLAENGE = 3            # so oft in Folge gefallen: Warnung senden
MENGE_MIN_ANTEIL = 0.5     # weniger als 50 % der üblichen Zeilen: Warnung senden

# --- Fristen und Aufbewahrung (Tage) ---
FRIST_TAGE = 7                        # Erinnerung für offene Fälle
BRONZE_AUFBEWAHRUNG_TAGE = 90         # Originaldateien
VACUUM_TAGE = 7                       # alte Tabellenversionen
LAUFPROTOKOLL_AUFBEWAHRUNG_TAGE = 365 # steuerung.laeufe

# --- Verantwortliche ---
VERANTWORTLICH = {
    "data_steward":  "contact@vkushnir.de",
    "data_engineer": "contact@vkushnir.de",
}

# --- Ordner unter Files ---
PFAD_BEISPIELDATEN = "Files/beispieldaten"
PFAD_PARTNER_ABLAGE = "Files/partner_ablage"
PFAD_BRONZE = "Files/bronze"

# --- Quellen und Schemas ---
QUELLEN = ["ladevorgaenge", "kunden", "stationen", "tarife"]
SCHEMAS = ["steuerung", "qualitaet", "silber", "gold"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 2: Datenvertrag
#
# Der Datenvertrag ist die Abmachung mit dem Partner:
# So muss jede gelieferte Datei aussehen.
#
# Für jede der vier Quellen (Ladevorgänge, Kunden,
# Stationen, Tarife) steht hier:
#   - welche Spalten es gibt,
#   - in welcher Reihenfolge,
#   - mit welchem Datentyp.
#
# Jede neue Datei wird mit diesem Vertrag verglichen.
# Fehlt eine Spalte oder heißt sie anders, wird die
# ganze Lieferung gestoppt.
# =====================================================

DATENVERTRAG = {
    "ladevorgaenge": [
        ("ladevorgang_id", "string"),
        ("ladepunkt_id",   "string"),
        ("station_id",     "string"),
        ("kunde_id",       "string"),
        ("tarif_id",       "string"),
        ("start_zeit",     "timestamp"),
        ("ende_zeit",      "timestamp"),
        ("energie_kwh",    "decimal(10,3)"),
        ("umsatz_eur",     "decimal(10,2)"),
        ("marge_eur",      "decimal(10,2)"),
    ],
    "kunden": [
        ("kunde_id",       "string"),
        ("name",           "string"),
        ("email",          "string"),
        ("heimatregion",   "string"),
        ("tarif_id",       "string"),
    ],
    "stationen": [
        ("station_id",     "string"),
        ("name",           "string"),
        ("region",         "string"),
        ("betreiber",      "string"),
    ],
    "tarife": [
        ("tarif_id",       "string"),
        ("name",           "string"),
        ("preis_pro_kwh",  "decimal(6,4)"),
    ],
}

# Schlüsselspalte je Quelle (für Regel R1 und R5)
SCHLUESSEL = {
    "ladevorgaenge": "ladevorgang_id",
    "kunden":        "kunde_id",
    "stationen":     "station_id",
    "tarife":        "tarif_id",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3: Regeln für Ladevorgänge (R1–R4)
#
# Jede Zeile wird mit vier Regeln geprüft.
# Das Ergebnis steht in einer neuen Spalte "fehler":
#   []            → Zeile ist in Ordnung
#   ["R2", "R3"]  → Zeile verstößt gegen R2 und R3
#
# R1 unterscheidet zwei Fälle derselben ladevorgang_id:
#   exakte Kopie (alle Werte gleich)
#     → die erste Zeile bleibt gültig, jede weitere
#       Kopie ist ein Fehler (harmlos, wird bereinigt)
#   Widerspruch (gleiche ID, andere Werte)
#     → alle Zeilen dieser ID sind Fehler, denn niemand
#       weiß, welche stimmt (Partner muss klären)
#
# Fehlerhafte Zeilen werden nicht gelöscht, nur markiert.
# =====================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Beschreibung jeder Regel (für Berichte und Fehlerzeilen)
REGELN = {
    "R1": "ladevorgang_id mehrfach: widersprüchliche Werte oder überzählige exakte Kopie",
    "R2": "energie_kwh ist leer oder nicht größer als 0",
    "R3": "ende_zeit liegt vor start_zeit",
    "R4": "station_id ist unbekannt",
    "R5": "Schlüssel in Stammdaten leer oder doppelt",
}


def pruefe_ladevorgaenge(df, df_stationen):
    """Prüft jede Zeile mit R1–R4 und ergänzt die Spalte 'fehler'."""
    spalten = df.columns

    # Für R1: Fingerabdruck des ganzen Inhalts einer Zeile
    inhalt = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(s).cast("string"), F.lit("")) for s in spalten]), 256)
    je_id = Window.partitionBy("ladevorgang_id")
    je_id_sortiert = Window.partitionBy("ladevorgang_id").orderBy("_inhalt")

    # Für R4: Liste der bekannten Stationen
    bekannt = (df_stationen.select("station_id").distinct()
                           .withColumn("_bekannt", F.lit(True)))

    df = (df.withColumn("_inhalt", inhalt)
            .withColumn("_anzahl", F.count("*").over(je_id))
            .withColumn("_varianten", F.size(F.collect_set("_inhalt").over(je_id)))
            .withColumn("_nr", F.row_number().over(je_id_sortiert))
            .join(bekannt, on="station_id", how="left"))

    widerspruch = (F.col("_anzahl") > 1) & (F.col("_varianten") > 1)
    kopie = (F.col("_anzahl") > 1) & (F.col("_varianten") == 1) & (F.col("_nr") > 1)

    fehler = F.array_compact(F.array(
        F.when(widerspruch | kopie, F.lit("R1")),
        F.when(F.col("energie_kwh").isNull() | (F.col("energie_kwh") <= 0), F.lit("R2")),
        F.when(F.col("ende_zeit") < F.col("start_zeit"), F.lit("R3")),
        F.when(F.col("_bekannt").isNull(), F.lit("R4")),
    ))

    return df.withColumn("fehler", fehler).select(*spalten, "fehler")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 4: Regel R5 für Stammdaten und Score
#
# R5: Der Schlüssel (kunde_id, station_id, tarif_id)
#     darf nicht leer und nicht doppelt sein.
#     Schon ein Fehler sperrt die ganze Lieferung.
#
# Score: 100 minus Prozent der fehlerhaften Zeilen.
#        Beispiel: 20 von 400 Zeilen fehlerhaft → Score 95
# =====================================================


def pruefe_stammdaten(df, quelle):
    """Prüft Stammdaten mit R5 und ergänzt die Spalte 'fehler'."""
    schluessel = SCHLUESSEL[quelle]
    je_schluessel = Window.partitionBy(schluessel)

    leer = F.col(schluessel).isNull() | (F.trim(F.col(schluessel)) == "")
    doppelt = F.count("*").over(je_schluessel) > 1

    fehler = F.array_compact(F.array(
        F.when(leer | doppelt, F.lit("R5")),
    ))
    return df.withColumn("fehler", fehler)


def berechne_score(df_geprueft):
    """Zählt Zeilen und Fehlerzeilen und berechnet den Score."""
    zeilen = df_geprueft.count()
    fehlerzeilen = df_geprueft.filter(F.size("fehler") > 0).count()

    if zeilen == 0:
        score = 0.0
    else:
        score = round(100 - 100 * fehlerzeilen / zeilen, 1)

    return {"zeilen": zeilen, "fehlerzeilen": fehlerzeilen, "score": score}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 5: Fingerabdruck
#
# Berechnet für eine Datei eine Prüfnummer (SHA-256).
#   gleiche Datei            → gleiche Nummer
#   ein Zeichen anders       → völlig andere Nummer
#
# Damit erkennt NB_02 doppelte und veränderte Dateien.
# Die Datei selbst wird nur gelesen, nie verändert.
# =====================================================

import hashlib


def fingerabdruck(pfad):
    """Gibt den SHA-256-Fingerabdruck einer Datei zurück.
    pfad: zum Beispiel 'Files/bronze/ladevorgaenge/ladevorgaenge_2026-07-14_v1.csv'
    """
    lokal = "/lakehouse/default/" + pfad     # Lakehouse als lokaler Ordner
    h = hashlib.sha256()
    with open(lokal, "rb") as datei:         # "rb" = nur lesen, als Bytes
        for block in iter(lambda: datei.read(1024 * 1024), b""):
            h.update(block)                  # Stück für Stück, je 1 MB
    return h.hexdigest()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 6: Bilanz
#
# Prüft, dass keine Zeile verloren geht:
#   gelesen = gültig + fehlerhaft
#
# Stimmt die Rechnung nicht, stoppt der Lauf mit
# einer Fehlermeldung. Die Pipeline geht dann in den
# Fehlerpfad und informiert den Data Engineer.
# =====================================================


def pruefe_bilanz(gelesen, gueltig, fehlerhaft, lieferung=""):
    """Stoppt mit einem Fehler, wenn Zeilen verloren gegangen sind."""
    if gelesen != gueltig + fehlerhaft:
        raise ValueError(
            f"Bilanzfehler {lieferung}: gelesen {gelesen}, "
            f"gültig {gueltig}, fehlerhaft {fehlerhaft}, "
            f"Differenz {gelesen - gueltig - fehlerhaft}"
        )
    return True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 7: Logbuch der Läufe
#
# Was diese Zelle bereitstellt:
#   lauf_starten  trägt beim Start eines Schritts eine
#                 Zeile mit Status "laeuft" in
#                 steuerung.laeufe ein.
#   lauf_beenden  ergänzt am Ende Endzeit, Status
#                 (erfolgreich oder fehler), Zahlen und
#                 eine kurze Meldung.
#   neue_lauf_id  erzeugt eine Lauf-Nummer, wenn ein
#                 Notebook von Hand gestartet wird.
#   sql_text      setzt einen Text sicher in einen
#                 SQL-Befehl ein (Anführungszeichen).
#
# Wozu: Am Morgen sieht man in einer Tabelle, welcher
# Schritt wann lief, ob er geklappt hat und wie viele
# Lieferungen und Zeilen er bearbeitet hat.
# Zeiten sind in UTC (Fabric-Standard).
# =====================================================

from datetime import datetime


def neue_lauf_id():
    return datetime.now().strftime("manuell-%Y%m%d-%H%M%S")


def sql_text(wert):
    return "NULL" if wert is None else "'" + str(wert).replace("'", "''") + "'"


def _sql_zahl(wert):
    return "NULL" if wert is None else str(int(wert))


def lauf_starten(lauf_id, schritt):
    spark.sql(f"""
        INSERT INTO steuerung.laeufe VALUES (
            {sql_text(lauf_id)}, {sql_text(schritt)}, current_timestamp(), NULL,
            'laeuft', NULL, NULL, NULL, NULL, NULL)
    """)


def lauf_beenden(lauf_id, schritt, status, lieferungen=None, gelesen=None,
                 gueltig=None, fehlerhaft=None, meldung=None):
    spark.sql(f"""
        UPDATE steuerung.laeufe SET
            ende_zeit = current_timestamp(),
            status = {sql_text(status)},
            lieferungen = {_sql_zahl(lieferungen)},
            zeilen_gelesen = {_sql_zahl(gelesen)},
            zeilen_gueltig = {_sql_zahl(gueltig)},
            zeilen_fehlerhaft = {_sql_zahl(fehlerhaft)},
            meldung = {sql_text(meldung)}
        WHERE lauf_id = {sql_text(lauf_id)}
          AND schritt = {sql_text(schritt)}
          AND status = 'laeuft'
    """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 8: Namen der Status einer Lieferung
#
# Jede Lieferung im Register (qualitaet.lieferungen)
# hat genau einen Status. Der Name sagt, an welchem
# Tor sie gerade steht und mit welchem Ergebnis.
#
# Alle Notebooks benutzen nur diese Namen hier, nie
# eigenen Text. Soll ein Name sich ändern, ändert man
# ihn nur an dieser Stelle.
#
#   Weg einer guten Lieferung:
#     schema_ok  →  in_silber
#
#   gesperrt (Data Steward muss handeln):
#     schema_gesperrt    Form der Datei falsch (NB_02)
#     schluessel_fehler  Stammdaten: Schlüssel leer
#                        oder doppelt, R5 (NB_03)
#     score_niedrig      Ladevorgänge: Score unter
#                        SCHWELLE (NB_04)
#
#   sonst:
#     duplikat              schon einmal unverändert da
#     ersetzt               von neuerer Version abgelöst
#     von_hand_freigegeben  Data Steward lässt eine
#                           gesperrte Lieferung durch
#     verworfen             Data Steward lehnt sie ab
# =====================================================

SCHEMA_OK            = "schema_ok"
SCHEMA_GESPERRT      = "schema_gesperrt"
SCHLUESSEL_FEHLER    = "schluessel_fehler"
SCORE_NIEDRIG        = "score_niedrig"
IN_SILBER            = "in_silber"
DUPLIKAT             = "duplikat"
ERSETZT              = "ersetzt"
VON_HAND_FREIGEGEBEN = "von_hand_freigegeben"
VERWORFEN            = "verworfen"

# alle gesperrten Status zusammen (für Berichte und Meldungen)
GESPERRT = [SCHEMA_GESPERRT, SCHLUESSEL_FEHLER, SCORE_NIEDRIG]

print("✓ Status-Namen geladen")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
