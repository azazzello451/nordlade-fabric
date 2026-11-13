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
# Zelle 3c (NB_00): Anreichern – Zusammenhänge
#
# Hilfsspalten, die mehrere Zeilen vergleichen:
#   _r1                gleiche ID mehrfach (Widerspruch
#                      oder überzählige exakte Kopie)
#   _kopie_andere_id   gleicher Inhalt, andere ID
#   _lp_ueberschneidung  Ladepunkt zeitgleich belegt
#   _kunde_parallel    Kunde zeitgleich an 2 Stationen
# =====================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _hash(spalten):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(s).cast("string"), F.lit("")) for s in spalten]), 256)


def _zusammenhang(df):
    sp = df.columns
    je_id = Window.partitionBy("ladevorgang_id")
    lp = Window.partitionBy("ladepunkt_id").orderBy("start_zeit", "ladevorgang_id")
    kd = Window.partitionBy("kunde_id").orderBy("start_zeit", "ladevorgang_id")
    nein = F.lit(False)
    vorher_ende = F.max("ende_zeit").over(lp.rowsBetween(Window.unboundedPreceding, -1))
    nachher_start = F.min("start_zeit").over(lp.rowsBetween(1, Window.unboundedFollowing))
    anders = lambda f: f("station_id").over(kd) != F.col("station_id")
    return (df.withColumn("_inhalt", _hash(sp))
              .withColumn("_anzahl", F.count("*").over(je_id))
              .withColumn("_varianten", F.size(F.collect_set("_inhalt").over(je_id)))
              .withColumn("_nr", F.row_number().over(je_id.orderBy("_inhalt")))
              .withColumn("_r1", (F.col("_anzahl") > 1) & ((F.col("_varianten") > 1) | (F.col("_nr") > 1)))
              .withColumn("_kopie_andere_id", F.dense_rank().over(
                  Window.partitionBy(_hash([s for s in sp if s != "ladevorgang_id"])).orderBy("ladevorgang_id")) > 1)
              .withColumn("_lp_ueberschneidung", F.coalesce(vorher_ende > F.col("start_zeit"), nein)
                          | F.coalesce(nachher_start < F.col("ende_zeit"), nein))
              .withColumn("_kunde_parallel",
                  F.coalesce((F.lag("ende_zeit").over(kd) > F.col("start_zeit")) & anders(F.lag), nein)
                  | F.coalesce((F.lead("start_zeit").over(kd) < F.col("ende_zeit")) & anders(F.lead), nein)))


print("✓ Zusammenhänge bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3d (NB_00): Anreichern
#
# anreichern hängt an jede Zeile die Hilfsspalten (_),
# die die Regeln im Katalog brauchen:
#   Stammdaten:    _schluessel_leer, _schluessel_anzahl
#   Ladevorgänge:  Zusammenhänge (Zelle 3c), Dauer in
#                  Minuten, Ladetag, Station und Kunde
#                  bekannt?, Tarifpreis (aus Silber)
# Braucht eine neue Regel neue Daten, wird nur hier
# ergänzt – auch das bleibt in NB_00.
# =====================================================


def _bekannt(tabelle, spalte, merker):
    return spark.table(tabelle).select(spalte).distinct().withColumn(merker, F.lit(True))


def anreichern(df, quelle, lieferung):
    if quelle in STAMMDATEN_QUELLEN:
        s = SCHLUESSEL[quelle]
        return (df.withColumn("_schluessel_leer", F.col(s).isNull() | (F.trim(F.col(s)) == ""))
                  .withColumn("_schluessel_anzahl", F.count("*").over(Window.partitionBy(s))))

    tarife = spark.table("silber.tarife").select("tarif_id", F.col("preis_pro_kwh").alias("_preis"))
    return (_zusammenhang(df)
            .withColumn("_dauer_min", (F.unix_timestamp("ende_zeit") - F.unix_timestamp("start_zeit")) / 60)
            .withColumn("_lieferdatum", F.lit(lieferung["lieferdatum"]).cast("date"))
            .join(_bekannt("silber.stationen", "station_id", "_station_bekannt"), "station_id", "left")
            .join(_bekannt("silber.kunden", "kunde_id", "_kunde_bekannt"), "kunde_id", "left")
            .join(tarife, "tarif_id", "left"))


print("✓ anreichern bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3e (NB_00): Prüfmaschine und Score
#
# pruefen nimmt alle Regeln des Katalogs, die für die
# Quelle gelten, und ergänzt zwei Spalten:
#   fehler    verletzte Regeln der Art sperrt/fehler
#   hinweise  verletzte Regeln der Art hinweis
#   []  → in Ordnung    ["R2", "R9"] → zwei Verstöße
# Fehlerhafte Zeilen werden nicht gelöscht, nur markiert.
# Gibt es für die Quelle keine aktive Regel (Tabelle
# leer oder nicht lesbar), stoppt der Lauf – lieber
# anhalten als ungeprüft durchlassen.
#
# Score: 100 minus Prozent der Zeilen mit Fehlern.
#        Hinweise zählen nicht.
#        Beispiel: 20 von 400 fehlerhaft → Score 95
# =====================================================


def _liste(regeln, arten):
    teile = [F.when(F.expr(r.bedingung), F.lit(r.kuerzel)) for r in regeln if r.art in arten]
    return F.array_compact(F.array(*teile)) if teile else F.array().cast("array<string>")


def pruefen(df, quelle, lieferung):
    regeln = [r for r in REGELWERK if quelle in r.quellen]
    if not regeln:
        raise ValueError(f"Keine aktiven Regeln für {quelle} geladen – Lauf gestoppt, "
                         "damit nichts ungeprüft durchgeht. steuerung.regeln prüfen (NB_00, Zelle 3g).")
    spalten = df.columns
    return (anreichern(df, quelle, lieferung)
            .withColumn("fehler", _liste(regeln, ("sperrt", "fehler")))
            .withColumn("hinweise", _liste(regeln, ("hinweis",)))
            .select(*spalten, "fehler", "hinweise"))


def berechne_score(df_geprueft):
    """Zählt Zeilen und Fehlerzeilen und berechnet den Score."""
    zeilen = df_geprueft.count()
    fehlerzeilen = df_geprueft.filter(F.size("fehler") > 0).count()
    score = 0.0 if zeilen == 0 else round(100 - 100 * fehlerzeilen / zeilen, 1)
    return {"zeilen": zeilen, "fehlerzeilen": fehlerzeilen, "score": score}


print("✓ pruefen und berechne_score bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3f (NB_00): Prüfen und festhalten
#
# pruefen_und_festhalten – für jede Quelle gleich:
#   1. jede Zeile mit dem Katalog prüfen, Score
#   2. alte Einträge dieser Lieferung löschen
#      (zweiter Lauf ändert nichts)
#   3. qualitaet.fehlerzeilen schreiben:
#        Zeilen mit Fehlern   → Status offen
#        Zeilen mit Hinweisen → Status hinweis
#      je mit Regel(n), Grund und Originalzeile
#   4. geprüfte Zeilen und Score zurückgeben
# NB_03 und NB_04 rufen nur diese Funktion auf.
# =====================================================


def pruefen_und_festhalten(df, lieferung):
    quelle, lid = lieferung["quelle"], lieferung["lieferung_id"]
    geprueft = pruefen(df, quelle, lieferung).localCheckpoint()
    ergebnis = berechne_score(geprueft)

    spark.sql(f"DELETE FROM qualitaet.fehlerzeilen WHERE lieferung_id = {sql_text(lid)}")

    beschreibung = F.create_map(*[x for k, t in REGELN.items() for x in (F.lit(k), F.lit(t))])
    fach = [s for s, _ in DATENVERTRAG[quelle]]
    id_spalte = F.col("ladevorgang_id") if quelle == "ladevorgaenge" else F.lit(None).cast("string")

    def zeilen(spalte, status):
        return geprueft.filter(F.size(spalte) > 0).select(
            F.lit(lid).alias("lieferung_id"),
            id_spalte.alias("ladevorgang_id"),
            F.concat_ws(",", spalte).alias("regel"),
            F.concat_ws("; ", F.transform(spalte, lambda r: beschreibung[r])).alias("grund"),
            F.concat_ws(",", *[F.coalesce(F.col(s).cast("string"), F.lit("")) for s in fach]).alias("rohzeile"),
            F.current_timestamp().alias("erkannt_am"),
            F.lit(status).alias("status"),
            F.lit(None).cast("timestamp").alias("geklaert_am"),
            F.lit(None).cast("string").alias("bemerkung"))

    (zeilen("fehler", "offen").unionByName(zeilen("hinweise", "hinweis"))
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
# Zelle 3g (NB_00): Regeln aus der Steuerungstabelle
#
# Liest die aktiven Regeln aus steuerung.regeln
# (Aufbau: NB_01, Zelle 4b). Pflege nur in der Tabelle.
#
# Jede Regel wird beim Laden geprüft. Ist eine kaputt,
# stoppt alles sofort mit ihrem Kürzel. Ist die Tabelle
# nicht lesbar: Warnung mit Grund, Zelle 3e stoppt.
# =====================================================

from collections import namedtuple
from datetime import date

Regel = namedtuple("Regel", "kuerzel quellen art beschreibung bedingung")
STAMMDATEN_QUELLEN = ("kunden", "stationen", "tarife")
ARTEN = ("sperrt", "fehler", "hinweis")


def _nicht_lesbar(tabelle):
    try:
        spark.table(tabelle).schema
        return None
    except Exception as e:
        return str(e).splitlines()[0][:160]


def _kaputt(regel, probe):
    if regel.art not in ARTEN:
        return f"Art „{regel.art}“ unbekannt, erlaubt: {', '.join(ARTEN)}"
    for q in regel.quellen:
        if q not in probe:
            return f"Quelle „{q}“ unbekannt"
        try:
            probe[q].select(F.expr(regel.bedingung)).schema
        except Exception as e:
            return f"Bedingung passt nicht zu {q}: {str(e).splitlines()[0][:160]}"
    return None


def regeln_laden():
    grund = _nicht_lesbar("steuerung.regeln")
    if grund:
        print(f"⚠ Keine Regeln geladen – steuerung.regeln nicht lesbar: {grund}")
        return []
    regeln = [Regel(z.kuerzel, tuple(q.strip() for q in z.quellen.split(",")), z.art, z.beschreibung, z.bedingung)
              for z in spark.table("steuerung.regeln").filter("aktiv")
                            .orderBy(F.expr("int(substr(kuerzel, 2))")).collect()]
    if not any(_nicht_lesbar(t) for t in ("silber.stationen", "silber.kunden", "silber.tarife")):
        leer = lambda q: spark.createDataFrame([], ", ".join(f"{n} {t}" for n, t in DATENVERTRAG[q]))
        probe = {q: anreichern(leer(q), q, {"lieferdatum": date(2000, 1, 1)}) for q in QUELLEN}
        fehler = [f"{r.kuerzel}: {grund}" for r in regeln if (grund := _kaputt(r, probe))]
        if fehler:
            raise ValueError("Fehlerhafte Regeln in steuerung.regeln:\n  " + "\n  ".join(fehler))
    return regeln


REGELWERK = regeln_laden()
REGELN = {r.kuerzel: r.beschreibung for r in REGELWERK}
print(f"✓ Regeln: {len(REGELWERK)} aktive aus steuerung.regeln geladen und geprüft")

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
