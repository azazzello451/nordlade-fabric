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
# Lädt NB_00_Einstellungen_und_Regeln. Daraus braucht dieses Notebook den Pfad zum Lager, den Datenvertrag (Spaltennamen) und die Schwelle für die Erwartungswerte.

# CELL ********************

%run NB_00_Einstellungen_und_Regeln

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 2: Grundlagen
#
# Was diese Zelle tut:
#   1. Sie legt fest, für welche Tage Testdaten
#      entstehen: 90 Tage ab dem 1. Juli 2026.
#   2. Sie räumt den Ordner beispieldaten leer, damit
#      keine alten Dateien übrig bleiben.
#   3. Sie stellt die Funktion schreibe_csv bereit.
#      Diese schreibt eine Testdatei (z. B. die
#      Ladevorgänge vom 14. Juli) in den Ordner und
#      trägt in eine Liste ein, was in der Datei steckt:
#      wie viele Zeilen, wie viele davon fehlerhaft.
#      Aus dieser Liste entsteht am Ende die Datei
#      erwartung.json. Mit ihr lässt sich später
#      kontrollieren, ob die Qualitätsprüfung alle
#      eingebauten Fehler gefunden hat.
#
# Warum immer dieselben Daten entstehen:
#   Der Zufallsgenerator startet immer mit demselben
#   Wert (SEED = 42). Deshalb erzeugt jeder Lauf genau
#   dieselben Dateien.
# =====================================================

import csv, json, random
from datetime import date, datetime, timedelta

SEED = 42
START_DATUM = date(2026, 7, 1)   # Nacht 1
NAECHTE = 90
ANZAHL_KUNDEN = 2000
ANZAHL_STATIONEN = 150
LAGER = PFAD_BEISPIELDATEN
LOKAL = "/lakehouse/default/"    # Lakehouse als lokaler Ordner

# Lager leeren und Ordner neu anlegen
notebookutils.fs.rm(LAGER, True)
for unterordner in QUELLEN + ["varianten"]:
    notebookutils.fs.mkdirs(f"{LAGER}/{unterordner}")

ERWARTUNG = []   # wird von schreibe_csv gefüllt


def datum_der_nacht(nacht):
    """Nacht 1 = 1.7.2026, Nacht 2 = 2.7.2026 ..."""
    return START_DATUM + timedelta(days=nacht - 1)


def dateiname(quelle, nacht, version):
    """Zum Beispiel ladevorgaenge_2026-07-14_v1.csv"""
    return f"{quelle}_{datum_der_nacht(nacht).isoformat()}_v{version}.csv"


def schreibe_csv(quelle, nacht, version, kopf, zeilen, erwartung, ordner=None):
    """Schreibt eine CSV-Datei ins Lager und notiert die erwarteten Ergebnisse."""
    name = dateiname(quelle, nacht, version)
    pfad = f"{LAGER}/{ordner or quelle}/{name}"
    with open(LOKAL + pfad, "w", newline="", encoding="utf-8") as datei:
        schreiber = csv.writer(datei)
        schreiber.writerow(kopf)
        schreiber.writerows(zeilen)
    ERWARTUNG.append({
        "datei": name,
        "ablage": pfad,
        "quelle": quelle,
        "nacht": nacht,
        "lieferdatum": datum_der_nacht(nacht).isoformat(),
        "version": version,
        **erwartung,
    })


print(f"✓ Zeitraum: {datum_der_nacht(1)} bis {datum_der_nacht(NAECHTE)} ({NAECHTE} Nächte)")
print(f"✓ Lager geleert: {LAGER}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3: Stammdaten erzeugen
#
# Was diese Zelle tut:
#   Sie erzeugt drei Listen, die jeden Tag vollständig
#   geliefert werden:
#     - 6 Tarife (Preis pro kWh)
#     - 150 Ladestationen (mit Region und Betreiber)
#     - 2.000 Kunden (mit Heimatregion und Tarif)
#
# Absichtlich eingebaute Änderungen:
#   ab Tag 15: Kunde KD-00010 wechselt den Tarif
#   ab Tag 35: Kunde KD-00020 zieht in eine andere Region
#   ab Tag 40: Station ST-010 bekommt einen neuen Betreiber
#   ab Tag 50: Tippfehler im Namen von KD-00030 korrigiert
#   → Damit lässt sich später zeigen, dass Silber die
#     alten Werte aufbewahrt (Verlauf).
#
# Absichtlich eingebauter Fehler:
#   Tag 55, Version 1: eine Kundennummer kommt doppelt vor
#   → Die Prüfung muss die ganze Lieferung sperren.
#   Tag 55, Version 2: korrigierte Liste
# =====================================================

random.seed(SEED)   # immer dieselben Stammdaten

REGIONEN = ["Nord", "Ost", "Sued", "West"]
BETREIBER = ["NordLade Eigenbetrieb", "Stadtwerke Nordheim", "VoltPark", "LadeKontor"]
VORNAMEN = ["Anna", "Ben", "Clara", "David", "Emma", "Finn", "Greta", "Hannes",
            "Ida", "Jonas", "Klara", "Lukas", "Mia", "Noah", "Paula", "Tim"]
NACHNAMEN = ["Becker", "Fischer", "Hoffmann", "Koch", "Mueller", "Neumann",
             "Richter", "Schmidt", "Schulz", "Wagner", "Weber", "Wolf"]

# --- Tarife (6) ---
TARIFE = [
    ("TA-01", "Basis", "0.5900"), ("TA-02", "Basis Plus", "0.5400"),
    ("TA-03", "Pendler", "0.4900"), ("TA-04", "Vielfahrer", "0.4400"),
    ("TA-05", "Flotte", "0.3900"), ("TA-06", "Gast", "0.6900"),
]
PREIS = {t[0]: float(t[2]) for t in TARIFE}

# --- Stationen (150) ---
STATIONEN = [
    [f"ST-{i:03d}", f"Ladepark {i:03d}", REGIONEN[(i - 1) % 4], random.choice(BETREIBER)]
    for i in range(1, ANZAHL_STATIONEN + 1)
]

# --- Kunden (2000) ---
KUNDEN = []
for i in range(1, ANZAHL_KUNDEN + 1):
    KUNDEN.append([
        f"KD-{i:05d}",
        f"{random.choice(VORNAMEN)} {random.choice(NACHNAMEN)}",
        f"kunde{i:05d}@example.com",
        random.choice(REGIONEN),
        random.choice([t[0] for t in TARIFE]),
    ])
KUNDEN[29][1] = "Anna Mueler"   # Tippfehler, wird ab Tag 50 korrigiert


def stammdaten_der_nacht(nacht):
    """Gibt den Stand von Kunden und Stationen an diesem Tag zurück."""
    kunden = [k.copy() for k in KUNDEN]
    stationen = [s.copy() for s in STATIONEN]
    if nacht >= 15:
        kunden[9][4] = "TA-04" if kunden[9][4] != "TA-04" else "TA-05"
    if nacht >= 35:
        kunden[19][3] = "Nord" if kunden[19][3] != "Nord" else "Sued"
    if nacht >= 40:
        stationen[9][3] = "VoltPark" if stationen[9][3] != "VoltPark" else "LadeKontor"
    if nacht >= 50:
        kunden[29][1] = "Anna Mueller"
    return kunden, stationen


def pruefe_r5(zeilen):
    """Zählt Zeilen mit leerer oder doppelter Nummer (Regel R5)."""
    anzahl = {}
    for z in zeilen:
        anzahl[z[0]] = anzahl.get(z[0], 0) + 1
    return sum(1 for z in zeilen if not z[0] or anzahl[z[0]] > 1)


def schreibe_stammdaten(quelle, nacht, version, zeilen):
    kopf = [s for s, _ in DATENVERTRAG[quelle]]
    fehler = pruefe_r5(zeilen)
    schreibe_csv(quelle, nacht, version, kopf, zeilen, {
        "zeilen": len(zeilen),
        "fehlerzeilen": fehler,
        "score": None,
        "status": "quarantaene" if fehler else "angenommen",
        "grund": "R5: Schlüssel leer oder doppelt" if fehler else None,
    })


for nacht in range(1, NAECHTE + 1):
    kunden, stationen = stammdaten_der_nacht(nacht)
    schreibe_stammdaten("tarife", nacht, 1, [list(t) for t in TARIFE])
    schreibe_stammdaten("stationen", nacht, 1, stationen)
    if nacht == 55:
        fehlerhaft = kunden.copy()
        fehlerhaft[100] = fehlerhaft[100].copy()
        fehlerhaft[100][0] = fehlerhaft[99][0]       # Kundennummer doppelt
        schreibe_stammdaten("kunden", nacht, 1, fehlerhaft)
        schreibe_stammdaten("kunden", nacht, 2, kunden)
    else:
        schreibe_stammdaten("kunden", nacht, 1, kunden)

print(f"✓ Stammdaten: {len(TARIFE)} Tarife, {len(STATIONEN)} Stationen, {len(KUNDEN)} Kunden je Tag")
print(f"✓ {len(ERWARTUNG)} Stammdaten-Dateien geschrieben")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 4: Werkzeuge für die Ladevorgänge
#
# Was diese Zelle tut:
#   Sie erzeugt noch keine Dateien, sondern stellt zwei
#   Funktionen bereit, die Zelle 5 für jeden Tag nutzt.
#
#   erzeuge_ladevorgaenge
#     Erzeugt die Ladevorgänge eines Tages (390–420
#     Stück). In jeden Tag werden absichtlich 7
#     fehlerhafte Zeilen eingebaut, je Regel mindestens
#     eine:
#       R1: zwei Zeilen mit derselben Vorgangsnummer
#       R2: zwei Zeilen ohne oder mit 0 kWh
#       R3: zwei Zeilen, bei denen das Ende vor dem
#           Start liegt
#       R4: eine Zeile mit einer Station, die es nicht
#           gibt (ST-999)
#
#   erwartung_ladevorgaenge
#     Zählt nach, wie viele Zeilen fehlerhaft sind und
#     welcher Score herauskommen muss. Das ist bewusst
#     eigener, einfacher Code – unabhängig von den
#     Regeln in NB_00. So prüfen sich beide gegenseitig.
#
# Warum die Menge je Wochentag schwankt:
#   Echte Daten sind nie jeden Tag gleich. Die Werte
#   sind so gewählt, dass die Menge nie drei Tage in
#   Folge sinkt – sonst gäbe es außerhalb der
#   Trendwoche falsche Trend-Warnungen.
# =====================================================

MENGE_JE_WOCHENTAG = [400, 410, 395, 415, 405, 420, 390]
ZEITFORMAT = "%Y-%m-%d %H:%M:%S"
KOPF_LADEVORGAENGE = [s for s, _ in DATENVERTRAG["ladevorgaenge"]]


def erzeuge_ladevorgaenge(nacht, anzahl, kunden, zusatz_r2=0, ohne_fehler_bei=()):
    """Erzeugt die Ladevorgänge eines Tages als Liste von Zeilen."""
    random.seed(SEED + nacht)                  # jeder Tag für sich wiederholbar
    ladetag = datum_der_nacht(nacht) - timedelta(days=1)   # geladen am Vortag
    zeilen = []
    for i in range(1, anzahl + 1):
        kunde = random.choice(kunden)
        station = random.randint(1, ANZAHL_STATIONEN)
        start = datetime.combine(ladetag, datetime.min.time()) + timedelta(
            minutes=random.randint(0, 22 * 60))
        ende = start + timedelta(minutes=random.randint(15, 180))
        energie = round(random.uniform(5, 80), 3)
        umsatz = round(energie * PREIS[kunde[4]], 2)
        zeilen.append([
            f"LV-{nacht:03d}-{i:04d}",
            f"LP-{station:03d}-{random.randint(1, 4)}",
            f"ST-{station:03d}",
            kunde[0],
            kunde[4],
            start.strftime(ZEITFORMAT),
            ende.strftime(ZEITFORMAT),
            f"{energie:.3f}",
            f"{umsatz:.2f}",
            f"{umsatz * 0.2:.2f}",
        ])

    # Fehler einbauen – jede Zeile bekommt höchstens einen
    frei = [i for i in range(anzahl) if i not in ohne_fehler_bei]
    wahl = random.sample(frei, 8 + zusatz_r2)
    r1a, r1b, r2a, r2b, r3a, r3b, r4, _ = wahl[:8]
    zeilen[r1b][0] = zeilen[r1a][0]                       # R1: Nummer doppelt
    zeilen[r2a][7] = ""                                   # R2: kWh leer
    zeilen[r2b][7] = "0.000"                              # R2: kWh = 0
    for r in (r3a, r3b):                                  # R3: Ende vor Start
        zeilen[r][5], zeilen[r][6] = zeilen[r][6], zeilen[r][5]
    zeilen[r4][2] = "ST-999"                              # R4: Station unbekannt
    for r in wahl[8:]:                                    # zusätzliche R2-Fehler
        zeilen[r][7] = ""
    return zeilen


BEKANNTE_STATIONEN = {s[0] for s in STATIONEN}


def erwartung_ladevorgaenge(zeilen):
    """Zählt nach, wie viele Zeilen fehlerhaft sein müssen und welcher Score herauskommt."""
    anzahl_id = {}
    for z in zeilen:
        anzahl_id[z[0]] = anzahl_id.get(z[0], 0) + 1
    fehler = 0
    for z in zeilen:
        r1 = anzahl_id[z[0]] > 1
        r2 = z[7] == "" or float(z[7]) <= 0
        r3 = z[6] < z[5]
        r4 = z[2] not in BEKANNTE_STATIONEN
        fehler += r1 or r2 or r3 or r4
    score = round(100 - 100 * fehler / len(zeilen), 1)
    return {
        "zeilen": len(zeilen),
        "fehlerzeilen": fehler,
        "score": score,
        "status": "quarantaene" if score < SCHWELLE else "angenommen",
        "grund": f"Score {score} unter Schwelle {SCHWELLE}" if score < SCHWELLE else None,
    }


print("✓ Werkzeuge für die Ladevorgänge bereit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 5: Ladevorgänge für alle 90 Tage schreiben
#
# Was diese Zelle tut:
#   Für jeden Tag entsteht eine Datei mit den
#   Ladevorgängen. Normale Tage haben 7 Fehlerzeilen
#   und einen Score über 80.
#
# Absichtlich eingebaute Sonderfälle:
#   Tag 20     Eine Spalte heißt "energie" statt
#              "energie_kwh". Version 2 ist korrigiert.
#   Tag 25     Version 2 hat zehnfach zu hohe Beträge.
#              Version 1 war richtig.
#   Tag 30     Wird später zweimal geliefert (Duplikat).
#   Tag 33     Eine zweite Datei mit gleichem Namen,
#              aber anderem Inhalt (Ordner varianten).
#   Tag 41–47  Die Fehler nehmen zu, Tag 47 fällt
#              unter den Score 80.
#   Tag 60     Wird erst nach Tag 62 geliefert.
#   Tag 65/70  Eine fehlerhafte Zeile von Tag 65 kommt
#              an Tag 70 korrigiert noch einmal.
#   Tag 75     Nur 30 % der üblichen Menge.
#
# Tag 30 und 60 sehen in den Daten normal aus. Die
# Besonderheit entsteht erst bei der Lieferung (NB_T2).
# =====================================================

TREND_FEHLERANTEIL = {41: 0.04, 42: 0.07, 43: 0.10, 44: 0.13, 45: 0.16, 46: 0.19, 47: 0.24}
HINWEIS = {
    30: "NB_T2 liefert diese Datei zweimal: die zweite Lieferung ist ein Duplikat",
    60: "NB_T2 liefert diesen Tag erst nach Tag 62: Lücke, dann Nachlieferung",
    75: "nur 30 % der üblichen Menge: Mengen-Warnung",
}
KORREKTUR_ID = "LV-065-0100"


def schreibe_ladevorgaenge(nacht, version, zeilen, kopf=KOPF_LADEVORGAENGE,
                           erwartung=None, hinweis=None, ordner=None):
    erwartung = erwartung or erwartung_ladevorgaenge(zeilen)
    erwartung["hinweis"] = hinweis or HINWEIS.get(nacht)
    schreibe_csv("ladevorgaenge", nacht, version, kopf, zeilen, erwartung, ordner)


for nacht in range(1, NAECHTE + 1):
    kunden, _ = stammdaten_der_nacht(nacht)
    anzahl = MENGE_JE_WOCHENTAG[(nacht - 1) % 7]
    if nacht == 75:
        anzahl = round(0.3 * 400)
    zusatz = round(anzahl * TREND_FEHLERANTEIL[nacht]) - 7 if nacht in TREND_FEHLERANTEIL else 0

    zeilen = erzeuge_ladevorgaenge(nacht, anzahl, kunden, zusatz_r2=zusatz,
                                   ohne_fehler_bei=(99,) if nacht == 65 else ())

    if nacht == 20:
        # Version 1: falscher Spaltenname → die Lieferung muss gestoppt werden
        falscher_kopf = ["energie" if s == "energie_kwh" else s for s in KOPF_LADEVORGAENGE]
        schreibe_ladevorgaenge(20, 1, zeilen, kopf=falscher_kopf, erwartung={
            "zeilen": len(zeilen), "fehlerzeilen": None, "score": None,
            "status": "quarantaene",
            "grund": "Spalte energie_kwh fehlt, gefunden: energie",
        }, hinweis="Spaltenname falsch, Version 2 folgt")
        schreibe_ladevorgaenge(20, 2, zeilen, hinweis="korrigierte Version, ersetzt Version 1")

    elif nacht == 25:
        schreibe_ladevorgaenge(25, 1, zeilen, hinweis="richtige Version, wird nach Version 2 wiederhergestellt")
        falsch = [z.copy() for z in zeilen]
        for z in falsch:                                  # Beträge ×10
            z[8] = f"{float(z[8]) * 10:.2f}"
            z[9] = f"{float(z[9]) * 10:.2f}"
        schreibe_ladevorgaenge(25, 2, falsch,
                               hinweis="falsche Beträge (×10): besteht die Regeln, Data Steward kehrt zu Version 1 zurück")

    elif nacht == 33:
        schreibe_ladevorgaenge(33, 1, zeilen, hinweis="Original")
        veraendert = [z.copy() for z in zeilen]
        veraendert[0][7] = f"{float(veraendert[0][7] or 0) + 1:.3f}"   # ein Wert anders
        erw = erwartung_ladevorgaenge(veraendert)
        erw.update(status="quarantaene", grund="Datei verändert ohne neue Version")
        schreibe_ladevorgaenge(33, 1, veraendert, erwartung=erw, ordner="varianten",
                               hinweis="gleicher Name wie das Original, anderer Inhalt: wird nach dem Original geliefert")

    elif nacht == 65:
        korrektur_zeile = zeilen[99].copy()               # richtige Werte merken
        zeilen[99][7] = ""                                # Fehler einbauen (R2)
        schreibe_ladevorgaenge(65, 1, zeilen, hinweis=f"{KORREKTUR_ID} ist fehlerhaft (R2), kommt an Tag 70 korrigiert")

    elif nacht == 70:
        zeilen.append(korrektur_zeile)
        schreibe_ladevorgaenge(70, 1, zeilen, hinweis=f"enthält {KORREKTUR_ID} von Tag 65 korrigiert")

    elif nacht in TREND_FEHLERANTEIL:
        schreibe_ladevorgaenge(nacht, 1, zeilen, hinweis="Trendwoche: Fehler nehmen zu")

    else:
        schreibe_ladevorgaenge(nacht, 1, zeilen)

anzahl_lv = sum(e["zeilen"] for e in ERWARTUNG
                if e["quelle"] == "ladevorgaenge" and e["version"] == 1 and "varianten" not in e["ablage"])
print(f"✓ Ladevorgänge geschrieben: {anzahl_lv} Zeilen (Version 1, ohne Varianten)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 6: erwartung.json schreiben und Überblick
#
# Was diese Zelle tut:
#   1. Sie speichert die Liste, die beim Schreiben der
#      Dateien entstanden ist, als erwartung.json.
#      Darin steht für jede Datei, was die Prüfung
#      später herausfinden MUSS: Anzahl Zeilen,
#      Anzahl Fehlerzeilen, Score und ob die Lieferung
#      angenommen oder gesperrt wird – mit Begründung.
#   2. Sie zeigt alle Sonderfälle als Tabelle.
#
# Wozu: Wenn später die echte Datenstrecke läuft, wird
# ihr Ergebnis mit erwartung.json verglichen. Stimmt
# alles überein, ist bewiesen, dass die Prüfung jeden
# eingebauten Fehler findet.
# =====================================================

with open(LOKAL + f"{LAGER}/erwartung.json", "w", encoding="utf-8") as datei:
    json.dump(ERWARTUNG, datei, ensure_ascii=False, indent=2)

lv = [e for e in ERWARTUNG if e["quelle"] == "ladevorgaenge"]
print(f"✓ {len(ERWARTUNG)} Dateien im Lager, davon {len(lv)} Ladevorgänge")
print(f"✓ erwartung.json geschrieben: {LAGER}/erwartung.json\n")

print("Sonderfälle:")
print(f"{'Datei':<50}{'Zeilen':>7}{'Fehler':>8}{'Score':>7}  Status")
for e in ERWARTUNG:
    if e.get("hinweis") or e["status"] != "angenommen":
        ordner = " (varianten)" if "varianten" in e["ablage"] else ""
        print(f"{e['datei'] + ordner:<50}{e['zeilen']:>7}{str(e['fehlerzeilen']):>8}"
              f"{str(e['score']):>7}  {e['status']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
