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

# CELL ********************

%run NB_00_Einstellungen_und_Regeln

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Zelle 1: Einstellungen und Regeln laden
# 
# Lädt das Notebook NB_00_Einstellungen_und_Regeln.
# 
# Danach kennt dieses Notebook alle Werte und Funktionen aus NB_00, zum Beispiel die Ordnerpfade, die Namen der Schemas und den Datenvertrag mit allen Spalten und Datentypen.
# 
# So steht jeder Wert nur an einer Stelle: Ändert sich etwas in NB_00, gilt es automatisch auch hier.

# CELL ********************

# =====================================================
# Zelle 2: Ordner unter Files anlegen
#
# Legt die Ordner für Testdaten, Partner-Ablage und
# Bronze an (Bronze mit einem Unterordner je Quelle).
# Vorhandene Ordner bleiben unverändert.
# =====================================================

ordner = [PFAD_BEISPIELDATEN, PFAD_PARTNER_ABLAGE, PFAD_BRONZE]
ordner += [f"{PFAD_BRONZE}/{quelle}" for quelle in QUELLEN]

for pfad in ordner:
    notebookutils.fs.mkdirs(pfad)
    print("✓", pfad)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3: Schemas anlegen
#
# Legt unter Tables die vier Bereiche an:
#   steuerung  – Logbuch der Nachtläufe
#   qualitaet  – Register der Lieferungen und Fehler
#   silber     – bereinigte Daten
#   gold       – Sternschema für die Berichte
# Vorhandene Schemas bleiben unverändert.
# =====================================================

for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print("✓ Schema", schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 4: Tabellen der Steuerung
#
# Hilfsfunktion tabelle_anlegen: legt eine leere
# Delta-Tabelle mit festen Spalten an – nur, wenn
# sie noch fehlt. Wird auch in den Zellen 5–7 genutzt.
#
# steuerung.laeufe      – je Lauf und Schritt: Start,
#                         Ende, Status, Zeilenzahlen
# steuerung.wasserstand – bis zu welcher Version Gold
#                         die Silber-Daten gelesen hat
# =====================================================


def tabelle_anlegen(name, spalten, eigenschaften=""):
    """Legt eine leere Delta-Tabelle an, falls sie fehlt."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {name} (
            {spalten}
        ) USING DELTA
        {eigenschaften}
    """)
    print("✓ Tabelle", name)


tabelle_anlegen("steuerung.laeufe", """
    lauf_id            STRING,
    schritt            STRING,
    start_zeit         TIMESTAMP,
    ende_zeit          TIMESTAMP,
    status             STRING,
    lieferungen        INT,
    zeilen_gelesen     BIGINT,
    zeilen_gueltig     BIGINT,
    zeilen_fehlerhaft  BIGINT,
    meldung            STRING
""")

tabelle_anlegen("steuerung.wasserstand", """
    quell_tabelle      STRING,
    letzte_version     BIGINT,
    aktualisiert_am    TIMESTAMP
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 4b (NB_01): Steuerungstabelle der Regeln
#
# steuerung.regeln – eine Zeile = eine Prüfregel:
#   kuerzel       R1, R2, …
#   quellen       für welche Quellen, mit Komma getrennt
#   art           sperrt  schon eine Zeile sperrt die
#                         ganze Lieferung
#                 fehler  Zeile raus, zählt im Score
#                 hinweis Zeile geht durch, nur melden
#   beschreibung  Klartext für Fehlerzeilen und Berichte
#   bedingung     SQL-Ausdruck, wahr = Regel verletzt;
#                 Spalten mit _ liefert NB_00 (anreichern)
#   aktiv         false = abgeschaltet, bleibt Nachweis
#   geaendert_am  wann zuletzt geändert
# Jede Änderung steht in der Delta-Historie
# (DESCRIBE HISTORY steuerung.regeln).
# Die Startregeln füllen die Zellen 4c und 4d.
# =====================================================

tabelle_anlegen("steuerung.regeln", """
    kuerzel        STRING,
    quellen        STRING,
    art            STRING,
    beschreibung   STRING,
    bedingung      STRING,
    aktiv          BOOLEAN,
    geaendert_am   TIMESTAMP
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 4c (NB_01): Startregeln, Teil 1 (R1–R9)
#
# Die Regeln, mit denen das Projekt startet. Eingetragen
# werden sie in Zelle 4d. Danach werden Regeln nur noch
# in der Tabelle gepflegt.
# =====================================================

LV, SD = "ladevorgaenge", "kunden,stationen,tarife"
STARTREGELN_TEIL_1 = [
 ("R1", LV, "fehler", "ladevorgang_id mehrfach: widersprüchliche Werte oder überzählige exakte Kopie", "_r1"),
 ("R2", LV, "fehler", "energie_kwh ist leer oder nicht größer als 0", "energie_kwh IS NULL OR energie_kwh <= 0"),
 ("R3", LV, "fehler", "ende_zeit liegt nicht nach start_zeit",
  "start_zeit IS NULL OR ende_zeit IS NULL OR ende_zeit <= start_zeit"),
 ("R4", LV, "fehler", "station_id ist unbekannt", "_station_bekannt IS NULL"),
 ("R5", SD, "sperrt", "Schlüssel in Stammdaten leer oder doppelt", "_schluessel_leer OR _schluessel_anzahl > 1"),
 ("R6", LV, "fehler", "kunde_id ist unbekannt", "_kunde_bekannt IS NULL"),
 ("R7", LV, "fehler", "tarif_id ist unbekannt", "_preis IS NULL"),
 ("R8", LV, "fehler", "ladepunkt_id passt nicht zur Station (LP-<Station>-<1 bis 4>)",
  "coalesce(NOT ladepunkt_id RLIKE concat('^LP-', substr(station_id, 4), '-[1-4]$'), true)"),
 ("R9", LV, "fehler", "umsatz_eur weicht um mehr als 1 Cent von energie_kwh × Tarifpreis ab",
  "_preis IS NOT NULL AND energie_kwh IS NOT NULL AND coalesce(abs(umsatz_eur - energie_kwh * _preis) > 0.01, true)"),
]

print("✓ Startregeln Teil 1 bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 4d (NB_01): Startregeln, Teil 2 (R10–R17)
#                   und eintragen
#
# Trägt nur Regeln ein, deren Kürzel noch fehlt (MERGE):
# von Hand geänderte Regeln werden nie überschrieben,
# zweimal ausführen ändert nichts.
# =====================================================

STARTREGELN = STARTREGELN_TEIL_1 + [
 ("R10", LV, "fehler", "marge_eur ist nicht 20 % von umsatz_eur",
  "umsatz_eur IS NOT NULL AND coalesce(marge_eur <> round(umsatz_eur * 0.2, 2), true)"),
 ("R11", LV, "fehler", "start_zeit liegt nicht am Ladetag (Lieferdatum minus 1 Tag)",
  "coalesce(to_date(start_zeit) <> date_sub(_lieferdatum, 1), true)"),
 ("R12", LV, "fehler", "Ladeleistung physikalisch unmöglich (über 400 kW)",
  "_dauer_min > 0 AND energie_kwh * 60 / _dauer_min > 400"),
 ("R13", LV, "fehler", "inhaltlich doppelt unter anderer ladevorgang_id (überzählige Kopie)", "_kopie_andere_id"),
 ("R14", LV, "hinweis", "Ladedauer über 12 Stunden", "_dauer_min > 720"),
 ("R15", LV, "hinweis", "energie_kwh über 150 (mehr, als ein Fahrzeugakku fasst)", "energie_kwh > 150"),
 ("R16", LV, "hinweis", "Ladepunkt gleichzeitig doppelt belegt", "_lp_ueberschneidung"),
 ("R17", LV, "hinweis", "Kunde gleichzeitig an zwei Stationen", "_kunde_parallel"),
]

(spark.createDataFrame(STARTREGELN, "kuerzel string, quellen string, art string, beschreibung string, bedingung string")
      .createOrReplaceTempView("startregeln"))
spark.sql("""
    MERGE INTO steuerung.regeln AS ziel
    USING startregeln AS neu ON ziel.kuerzel = neu.kuerzel
    WHEN NOT MATCHED THEN INSERT (kuerzel, quellen, art, beschreibung, bedingung, aktiv, geaendert_am)
         VALUES (neu.kuerzel, neu.quellen, neu.art, neu.beschreibung, neu.bedingung, true, current_timestamp())
""")
print(f"✓ steuerung.regeln: {spark.table('steuerung.regeln').count()} Regeln")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 5: Tabellen der Qualität
#
# qualitaet.lieferungen    – das Register: eine Zeile je
#                            gelieferter Datei. Zeigt, wo
#                            die Datei in Bronze liegt (pfad),
#                            ihren Fingerabdruck und ihren
#                            Status (z. B. schema_ok,
#                            in_silber, score_niedrig).
#                            Zugleich die Liste der
#                            gesperrten Lieferungen.
# qualitaet.fehlerzeilen   – jede fehlerhafte Zeile einzeln,
#                            mit Regel, Grund und der
#                            Originalzeile. Fehler je Regel
#                            lassen sich hieraus zählen.
# =====================================================

tabelle_anlegen("qualitaet.lieferungen", """
    lieferung_id     STRING,
    datei_name       STRING,
    pfad             STRING,
    quelle           STRING,
    lieferdatum      DATE,
    version          INT,
    datei_hash       STRING,
    zeilen           BIGINT,
    fehlerzeilen     BIGINT,
    score            DECIMAL(5,1),
    schwelle         INT,
    status           STRING,
    grund            STRING,
    verantwortlich   STRING,
    entscheidung     STRING,
    entschieden_von  STRING,
    entschieden_am   TIMESTAMP,
    bemerkung        STRING,
    eingang_am       TIMESTAMP,
    verarbeitet_am   TIMESTAMP,
    gemeldet_am      TIMESTAMP,
    lauf_id          STRING
""")

tabelle_anlegen("qualitaet.fehlerzeilen", """
    lieferung_id    STRING,
    ladevorgang_id  STRING,
    regel           STRING,
    grund           STRING,
    rohzeile        STRING,
    erkannt_am      TIMESTAMP,
    status          STRING,
    geklaert_am     TIMESTAMP,
    bemerkung       STRING
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 6: Tabellen in Silber
#
# Die Fachspalten kommen direkt aus dem Datenvertrag
# (NB_00) – sie stehen also nur an einer Stelle.
# Dazu kommen:
#   Technik  – woher die Zeile stammt (Lieferung, Lauf)
#   Verlauf  – bei Kunden und Stationen: von wann bis
#              wann eine Zeile gültig war (SCD Typ 2)
#
# silber.ladevorgaenge  – mit Change Data Feed, damit
#                         Gold nur Änderungen lesen muss
# silber.kunden         – ohne E-Mail (Datensparsamkeit)
# silber.stationen      – mit Verlauf
# silber.tarife         – nur aktueller Stand
# =====================================================


def spalten_aus_vertrag(quelle, ohne=()):
    """Baut die Spaltenliste aus dem Datenvertrag."""
    return ",\n".join(
        f"{name} {typ.upper()}"
        for name, typ in DATENVERTRAG[quelle]
        if name not in ohne
    )


TECHNIK = """lieferung_id STRING,
    lieferdatum  DATE,
    version      INT,
    geprueft_am  TIMESTAMP,
    lauf_id      STRING"""

VERLAUF = """gueltig_von  DATE,
    gueltig_bis  DATE,
    ist_aktuell  BOOLEAN"""


tabelle_anlegen(
    "silber.ladevorgaenge",
    spalten_aus_vertrag("ladevorgaenge") + ",\n" + TECHNIK,
    "TBLPROPERTIES (delta.enableChangeDataFeed = true)",
)

tabelle_anlegen(
    "silber.kunden",
    spalten_aus_vertrag("kunden", ohne=("email",)) + ",\n" + VERLAUF + ",\n" + TECHNIK,
)

tabelle_anlegen(
    "silber.stationen",
    spalten_aus_vertrag("stationen") + ",\n" + VERLAUF + ",\n" + TECHNIK,
)

tabelle_anlegen(
    "silber.tarife",
    spalten_aus_vertrag("tarife") + ",\n" + TECHNIK,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 7: Tabellen in Gold (Sternschema)
#
# gold.fakt_ladevorgang – eine Zeile je Ladevorgang.
#                         Zeigt per Surrogatschlüssel (_sk)
#                         auf die Dimensionen, so wie sie
#                         zum Ladezeitpunkt waren.
# gold.dim_kunde        – ohne Name und E-Mail
# gold.dim_station      – mit Region (für Zeilenrechte)
# gold.dim_tarif        – Tarife
# gold.dim_datum        – Kalender
# gold.datenstand       – wie aktuell und vollständig
#                         die Berichte sind
#
# Optimierung: Alle Gold-Tabellen mit V-Order, damit
# Power BI (Direct Lake) sie schnell lesen kann.
# Keine Partitionierung (Datenmenge zu klein).
# Die regelmäßige Pflege (OPTIMIZE, Z-Order, VACUUM)
# übernimmt NB_08.
# =====================================================

GOLD_OPTIMIERUNG = "TBLPROPERTIES ('delta.parquet.vorder.enabled' = 'true')"

tabelle_anlegen("gold.fakt_ladevorgang", """
    ladevorgang_id   STRING,
    datum_key        INT,
    kunde_sk         BIGINT,
    station_sk       BIGINT,
    tarif_sk         BIGINT,
    ladepunkt_id     STRING,
    start_zeit       TIMESTAMP,
    ende_zeit        TIMESTAMP,
    dauer_minuten    INT,
    energie_kwh      DECIMAL(10,3),
    umsatz_eur       DECIMAL(10,2),
    marge_eur        DECIMAL(10,2),
    lieferung_id     STRING
""", GOLD_OPTIMIERUNG)

tabelle_anlegen("gold.dim_kunde", """
    kunde_sk      BIGINT,
    kunde_id      STRING,
    heimatregion  STRING,
    tarif_id      STRING,
    gueltig_von   DATE,
    gueltig_bis   DATE,
    ist_aktuell   BOOLEAN
""", GOLD_OPTIMIERUNG)

tabelle_anlegen("gold.dim_station", """
    station_sk    BIGINT,
    station_id    STRING,
    name          STRING,
    region        STRING,
    betreiber     STRING,
    gueltig_von   DATE,
    gueltig_bis   DATE,
    ist_aktuell   BOOLEAN
""", GOLD_OPTIMIERUNG)

tabelle_anlegen("gold.dim_tarif", """
    tarif_sk       BIGINT,
    tarif_id       STRING,
    name           STRING,
    preis_pro_kwh  DECIMAL(6,4)
""", GOLD_OPTIMIERUNG)

tabelle_anlegen("gold.dim_datum", """
    datum_key       INT,
    datum           DATE,
    jahr            INT,
    quartal         INT,
    monat           INT,
    monat_name      STRING,
    kalenderwoche   INT,
    wochentag       INT,
    wochentag_name  STRING,
    ist_wochenende  BOOLEAN
""", GOLD_OPTIMIERUNG)

tabelle_anlegen("gold.datenstand", """
    letzte_lieferung   DATE,
    vollstaendig_bis   DATE,
    nicht_enthalten    STRING,
    aktualisiert_am    TIMESTAMP
""", GOLD_OPTIMIERUNG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 8: Kontrolle
#
# Vergleicht die vorhandenen Tabellen mit der Liste der
# erwarteten Tabellen.
#   alles da  → eine Erfolgsmeldung
#   etwas fehlt → Liste der fehlenden Tabellen, Stopp
# =====================================================

ERWARTET = {
    "steuerung": ["laeufe", "wasserstand", "regeln"],
    "qualitaet": ["lieferungen", "fehlerzeilen"],
    "silber":    ["ladevorgaenge", "kunden", "stationen", "tarife"],
    "gold":      ["fakt_ladevorgang", "dim_kunde", "dim_station",
                  "dim_tarif", "dim_datum", "datenstand"],
}

anzahl = sum(len(t) for t in ERWARTET.values())
fehlend = []

for schema, erwartet in ERWARTET.items():
    vorhanden = {r.tableName for r in spark.sql(f"SHOW TABLES IN {schema}").collect()}
    fehlend += [f"{schema}.{t}" for t in erwartet if t not in vorhanden]

if fehlend:
    raise ValueError(
        f"Es fehlen {len(fehlend)} von {anzahl} Tabellen:\n  " + "\n  ".join(fehlend)
    )

print(f"✓ Alle {anzahl} Tabellen vorhanden")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
