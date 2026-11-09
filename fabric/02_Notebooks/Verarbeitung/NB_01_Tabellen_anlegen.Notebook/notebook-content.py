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
# Zelle 5: Tabellen der Qualität
#
# qualitaet.lieferungen    – eine Zeile je Datei:
#                            Register und Quarantäne-
#                            Liste zugleich (Spalte status)
# qualitaet.regelergebnis  – je Lieferung und Regel:
#                            geprüfte und fehlerhafte Zeilen
# qualitaet.fehlerzeilen   – jede fehlerhafte Zeile mit
#                            Regel, Grund und Original
# =====================================================

tabelle_anlegen("qualitaet.lieferungen", """
    lieferung_id     STRING,
    datei_name       STRING,
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

tabelle_anlegen("qualitaet.regelergebnis", """
    lieferung_id       STRING,
    regel              STRING,
    zeilen_geprueft    BIGINT,
    zeilen_fehlerhaft  BIGINT,
    geprueft_am        TIMESTAMP,
    lauf_id            STRING
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
    "steuerung": ["laeufe", "wasserstand"],
    "qualitaet": ["lieferungen", "regelergebnis", "fehlerzeilen"],
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
