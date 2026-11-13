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

# Blindtest: erzeugt 5 Ladevorgangs-Dateien (v9, Tage 86–90) mit Fehlern, die eine unabhängige KI-Sitzung ohne Kenntnis der Prüfregeln eingebaut hat. Lösungsblatt in Files/blindtest. Erst nach dem Gesamttest in die Ablage legen.

# CELL ********************

import csv
import io
import os
import random
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP

SEED = 58219
ZIEL = "/lakehouse/default/Files/blindtest/"
rng = random.Random(SEED)

SPALTEN = ["ladevorgang_id", "ladepunkt_id", "station_id", "kunde_id", "tarif_id",
           "start_zeit", "ende_zeit", "energie_kwh", "umsatz_eur", "marge_eur"]
PREISE = {"TA-01": Decimal("0.59"), "TA-02": Decimal("0.54"), "TA-03": Decimal("0.49"),
          "TA-04": Decimal("0.44"), "TA-05": Decimal("0.39"), "TA-06": Decimal("0.69")}
LIEFERUNGEN = [("2026-09-24", 86), ("2026-09-25", 87), ("2026-09-26", 88),
               ("2026-09-27", 89), ("2026-09-28", 90)]

AKTIV = sorted(rng.sample(range(1, 151), 118))
LP_ANZAHL = {s: rng.choice([2, 2, 3, 4, 4]) for s in range(1, 151)}
LEISTUNG = {s: rng.choice([11, 22, 22, 22, 50, 150]) for s in range(1, 151)}
KUNDEN = [f"KD-{i:05d}" for i in range(1, 2001)]
TARIF_VON = {k: rng.choices(list(PREISE), weights=[14, 22, 25, 12, 9, 18])[0] for k in KUNDEN}
GEWICHT = [1, 1, 1, 1, 1, 2, 3, 5, 7, 7, 6, 6, 7, 7, 6, 6, 7, 8, 8, 7, 5, 4, 3, 2]
PUFFER_LP = timedelta(minutes=3)
PUFFER_KD = timedelta(minutes=20)


def q2(x, modus=ROUND_HALF_UP):
    return x.quantize(Decimal("0.01"), rounding=modus)


def q3(x):
    return Decimal(str(x)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)


def ts(d):
    return d.strftime("%Y-%m-%d %H:%M:%S")


def dauer(v):
    return (v["ende"] - v["start"]).total_seconds() / 60


def ueberlappt(liste, s, e, puffer):
    return any(s < b + puffer and a < e + puffer for a, b in liste)


class Lieferung:
    def __init__(self, datum, nacht):
        self.datum = datum
        self.datei = f"ladevorgaenge_{datum}_v9.csv"
        self.nacht = nacht
        self.tag = datetime.strptime(datum, "%Y-%m-%d") - timedelta(days=1)
        self.lp = {}
        self.kd = {}
        self.vorgaenge = []
        self.naechste = 1
        self.bom = False
        self.schnitt = None
        self.zeilen = {}
        self.befunde = []
        self.hinweise = []
        self.formal_ok = True

    def lp_frei(self, lp, s, e):
        return not ueberlappt(self.lp.get(lp, []), s, e, PUFFER_LP)

    def kd_frei(self, kd, s, e):
        return not ueberlappt(self.kd.get(kd, []), s, e, PUFFER_KD)

    def belegen(self, v):
        self.lp.setdefault(v["lp"], []).append((v["start"], v["ende"]))
        self.kd.setdefault(v["kd"], []).append((v["start"], v["ende"]))

    def neue_id(self):
        i = f"LV-{self.nacht:03d}-{self.naechste:04d}"
        self.naechste += 1
        return i

    def nummerieren(self):
        self.vorgaenge.sort(key=lambda v: (v["st"], v["start"], v["lp"]))
        for v in self.vorgaenge:
            v["id"] = self.neue_id()


def berechne(v, preis=None, modus=ROUND_HALF_UP):
    p = PREISE[v["ta"]] if preis is None else preis
    v["u"] = q2(v["e"] * p, modus)
    v["m"] = q2(v["u"] * Decimal("0.2"), modus)
    return v


def vorgang(st, n, kd, start, minuten, kwh, ta=None):
    v = {"id": None, "st": st, "lp": f"LP-{st:03d}-{n}", "kd": kd,
         "ta": ta or TARIF_VON[kd], "start": start,
         "ende": start + timedelta(minutes=minuten), "e": q3(kwh)}
    return berechne(v)


def zufallsvorgang(t, st=None, kd=None, start=None, pruefe_kd=True):
    for _ in range(20000):
        s_nr = st if st is not None else rng.choice(AKTIV)
        k = kd if kd is not None else rng.choice(KUNDEN)
        if start is None:
            beginn = t.tag + timedelta(hours=rng.choices(range(24), weights=GEWICHT)[0],
                                       minutes=rng.randrange(60))
        else:
            beginn = start
        kw = LEISTUNG[s_nr]
        kwh = rng.uniform(5, 60)
        minuten = int(round(max(10, min(120, kwh / kw * 60 * rng.uniform(1.08, 1.6)))))
        kwh = min(kwh, kw * minuten / 60 * 0.93)
        if kwh < 5.2:
            continue
        v = vorgang(s_nr, rng.randint(1, LP_ANZAHL[s_nr]), k, beginn, minuten, kwh)
        if v["ende"].date() != t.tag.date():
            continue
        if not t.lp_frei(v["lp"], v["start"], v["ende"]):
            continue
        if pruefe_kd and not t.kd_frei(k, v["start"], v["ende"]):
            continue
        t.belegen(v)
        return v
    raise RuntimeError("Kein freier Termin gefunden")


def manuell(t, st, kd, start, minuten, kwh, ta=None, pruefe_kd=True):
    ns = list(range(1, LP_ANZAHL[st] + 1))
    rng.shuffle(ns)
    for n in ns:
        v = vorgang(st, n, kd, start, minuten, kwh, ta)
        if t.lp_frei(v["lp"], v["start"], v["ende"]) and (not pruefe_kd or t.kd_frei(kd, v["start"], v["ende"])):
            t.belegen(v)
            return v
    return None


def platziere(t, stationen, kunden, startfunktion, minuten, kwh):
    while True:
        v = manuell(t, rng.choice(stationen), rng.choice(kunden), startfunktion(), minuten, kwh)
        if v:
            t.vorgaenge.append(v)
            return v


def basis(t, anzahl):
    for _ in range(anzahl):
        t.vorgaenge.append(zufallsvorgang(t))


def vertauscht(st):
    s = f"{st:03d}"
    if s[0] == "0" and s[1] != "0" and s[2] != "0" and s[1] != s[2]:
        return int(s[0] + s[2] + s[1])
    return None


def zeile(v):
    return [v["id"], v["lp"], f"ST-{v['st']:03d}", v["kd"], v["ta"], ts(v["start"]), ts(v["ende"]),
            f"{v['e']:.3f}", f"{v['u']:.2f}", f"{v['m']:.2f}"]


A = Lieferung(*LIEFERUNGEN[0])
basis(A, rng.randint(388, 396))
a1 = rng.choice([v for v in A.vorgaenge if dauer(v) >= 45 and 7 <= v["start"].hour <= 19])
a2 = None
while a2 is None:
    beginn = a1["start"] + timedelta(minutes=rng.randint(12, int(dauer(a1)) - 20))
    minuten = rng.randint(35, 75)
    kwh = max(5.6, min(rng.uniform(10, 45), LEISTUNG[a1["st"]] * minuten / 60 * 0.9))
    kd = rng.choice(KUNDEN)
    if A.kd_frei(kd, beginn, beginn + timedelta(minutes=minuten)):
        a2 = vorgang(a1["st"], int(a1["lp"].rsplit("-", 1)[1]), kd, beginn, minuten, kwh)
        A.belegen(a2)
        A.vorgaenge.append(a2)
a3 = rng.choice([v for v in A.vorgaenge
                 if dauer(v) >= 35 and 8 <= v["start"].hour <= 18 and v is not a1 and v is not a2])
a4 = None
while a4 is None:
    st = rng.choice([s for s in AKTIV if abs(s - a3["st"]) >= 50])
    beginn = a3["start"] + timedelta(minutes=rng.randint(6, int(dauer(a3)) - 12))
    minuten = rng.randint(30, 80)
    kwh = min(rng.uniform(12, 48), LEISTUNG[st] * minuten / 60 * 0.9)
    if kwh >= 5.2:
        a4 = manuell(A, st, a3["kd"], beginn, minuten, kwh, pruefe_kd=False)
A.vorgaenge.append(a4)
A.nummerieren()
gesperrt = {v["st"] for v in (a1, a2, a3, a4)}
kandidaten = sorted({v["st"] for v in A.vorgaenge} - gesperrt)
rng.shuffle(kandidaten)
a5_st, a5_liste = None, []
for st in kandidaten:
    gruppe = [v for v in A.vorgaenge if v["st"] == st]
    abw = [v for v in gruppe if berechne(dict(v), modus=ROUND_DOWN)["u"] != v["u"]
           or berechne(dict(v), modus=ROUND_DOWN)["m"] != v["m"]]
    if len(gruppe) >= 4 and len(abw) >= 3:
        a5_st, a5_liste = st, abw
        for v in gruppe:
            berechne(v, modus=ROUND_DOWN)
        break
pool = [v for v in A.vorgaenge if v["st"] not in gesperrt and v["st"] != a5_st]
a6 = rng.choice(pool)
a7 = dict(a6)
a7["id"] = A.neue_id()
A.vorgaenge.append(a7)
a8 = rng.choice([v for v in pool if v is not a6 and vertauscht(v["st"])])
a8_alt = a8["lp"]
a8["lp"] = f"LP-{vertauscht(a8['st']):03d}-{a8['lp'].rsplit('-', 1)[1]}"

B = Lieferung(*LIEFERUNGEN[1])
basis(B, rng.randint(394, 402))
b_st = rng.choice([s for s in AKTIV if LEISTUNG[s] >= 22])
for start in (B.tag + timedelta(minutes=rng.randint(28, 52)),
              B.tag + timedelta(hours=1, minutes=rng.randint(5, 40))):
    B.vorgaenge.append(zufallsvorgang(B, st=b_st, start=start))
for _ in range(3):
    B.vorgaenge.append(zufallsvorgang(B, st=b_st))
b_grp = [v for v in B.vorgaenge if v["st"] == b_st]
freie_kunden = [k for k in KUNDEN if k not in B.kd]
for v in b_grp:
    v["start"] -= timedelta(hours=2)
    v["ende"] -= timedelta(hours=2)
    v["kd"] = freie_kunden.pop(rng.randrange(len(freie_kunden)))
    v["ta"] = TARIF_VON[v["kd"]]
    berechne(v)
b_t = None
while b_t is None:
    st = rng.choice([s for s in AKTIV if s != b_st])
    b_t = manuell(B, st, "KD-00000", B.tag + timedelta(hours=10, minutes=rng.randint(5, 50)),
                  2, 0.412, ta="TA-03")
B.vorgaenge.append(b_t)
B.nummerieren()
moeglich = [i for i in range(140, len(B.vorgaenge) - 60)
            if all(B.vorgaenge[j]["st"] != b_st and B.vorgaenge[j] is not b_t for j in range(i, i + 5))]
b_i0 = rng.choice(moeglich)
b_weg = B.vorgaenge[b_i0:b_i0 + 5]
del B.vorgaenge[b_i0:b_i0 + 5]
b_vor, b_nach = B.vorgaenge[b_i0 - 1], B.vorgaenge[b_i0]

C = Lieferung(*LIEFERUNGEN[2])
basis(C, rng.randint(392, 404))
mittel = [s for s in AKTIV if LEISTUNG[s] >= 22]
schnell = [s for s in AKTIV if LEISTUNG[s] >= 50]
c1 = platziere(C, mittel, KUNDEN, lambda: C.tag + timedelta(hours=23, minutes=rng.randint(36, 44)), 38, 12.874)
c2 = platziere(C, mittel, [k for k in KUNDEN if TARIF_VON[k] == "TA-05"],
               lambda: C.tag + timedelta(hours=rng.randint(9, 17), minutes=rng.randrange(60)), 46, 15.5)
c3 = platziere(C, schnell, KUNDEN,
               lambda: C.tag + timedelta(hours=rng.randint(7, 20), minutes=rng.randrange(60)), 10, 5.0)
c4 = platziere(C, schnell, KUNDEN,
               lambda: C.tag + timedelta(hours=rng.randint(6, 19), minutes=rng.randrange(60)), 120, 60.0)
C.nummerieren()

D = Lieferung(*LIEFERUNGEN[3])
basis(D, rng.randint(390, 400))
D.nummerieren()
D.bom = True
D.formal_ok = False
d_grp = [v for v in D.vorgaenge if v["ta"] == "TA-06"]
for v in d_grp:
    berechne(v, Decimal("0.65"))
d_x = rng.choice([v for v in D.vorgaenge if v["ta"] != "TA-06" and dauer(v) >= 20])
d_x_ende = d_x["ende"]
d_x["ende"] = d_x["start"]

E = Lieferung(*LIEFERUNGEN[4])
basis(E, rng.randint(389, 398))
e_st = rng.choice([s for s in AKTIV if 10 <= s <= 99 and sum(v["st"] == s for v in E.vorgaenge) >= 3])
e_grp = [v for v in E.vorgaenge if v["st"] == e_st]
for v in e_grp:
    v["lp"] = f"LP-{v['st']}-{v['lp'].rsplit('-', 1)[1]}"
E.nummerieren()
d_pool = [v for v in D.vorgaenge if v["ta"] != "TA-06" and v is not d_x]
e_paare = [(v, dict(v)) for v in rng.sample(d_pool, 3)]
e_neu = [zufallsvorgang(D) for _ in range(3)]
nachlieferung = [k for _, k in e_paare] + e_neu[:-1]
rng.shuffle(nachlieferung)
nachlieferung.append(e_neu[-1])
for v in nachlieferung:
    v["id"] = E.neue_id()
    E.vorgaenge.append(v)
E.schnitt = rng.randint(17, 41)
E.formal_ok = False

os.makedirs(ZIEL, exist_ok=True)
ausgabe = []
for t in (A, B, C, D, E):
    puffer = io.StringIO()
    w = csv.writer(puffer, lineterminator="\n")
    w.writerow(SPALTEN)
    for v in t.vorgaenge:
        w.writerow(zeile(v))
    text = puffer.getvalue()
    if t.schnitt:
        kopf = text[:-1].rfind("\n") + 1
        text = text[:kopf] + text[kopf:kopf + t.schnitt]
    with open(os.path.join(ZIEL, t.datei), "w", encoding="utf-8-sig" if t.bom else "utf-8", newline="") as f:
        f.write(text)
    t.zeilen = {v["id"]: i + 2 for i, v in enumerate(t.vorgaenge)}
    ausgabe.append((os.path.join(ZIEL, t.datei), len(t.vorgaenge)))

# --- Ausgabe ---


def ids_text(t, ids):
    return ", ".join(f"{i} (Zeile {t.zeilen[i]})" if i in t.zeilen else f"{i} (nicht in der Datei)" for i in ids)


def zr(v):
    return f"{ts(v['start'])} bis {ts(v['ende'])}"


def soll(v):
    return berechne(dict(v))


bsp5 = a5_liste[0]
A.befunde = [
    ("Kontext: Überschneidung am selben Ladepunkt", [a1["id"], a2["id"]],
     f"Beide Vorgänge laufen am Ladepunkt {a1['lp']}: {zr(a1)} und {zr(a2)}. Ein Ladepunkt kann nicht zwei "
     f"Fahrzeuge gleichzeitig laden. Jede Zeile für sich ist gültig.",
     "Eine Sitzung wurde nicht sauber beendet (StopTransaction verloren, Kabel ohne Abmeldung gezogen) und das "
     "Backend setzt die Endzeit später aus einem Zählerstand; oder zwei Stecker sind im Connector-Mapping auf "
     "dieselbe ladepunkt_id gemappt."),
    ("Kontext: derselbe Kunde gleichzeitig an zwei Stationen", [a3["id"], a4["id"]],
     f"Kunde {a3['kd']} lädt an ST-{a3['st']:03d} ({zr(a3)}) und parallel an ST-{a4['st']:03d} ({zr(a4)}).",
     "Geklonte oder weitergegebene RFID-Karte, Flottenkarte von mehreren Fahrern genutzt oder Roaming-Doppelautorisierung. "
     "Fällt nur beim Vergleich mehrerer Zeilen auf."),
    ("Häufung: Abschneiden statt Runden an einer Station", [v["id"] for v in a5_liste],
     f"Alle Vorgänge von ST-{a5_st:03d} rechnen Umsatz und Marge mit Abschneiden statt kaufmännischem Runden. "
     f"Aufgeführt sind die Zeilen, in denen das einen Unterschied macht (je 1 Cent zu wenig). Beispiel {bsp5['id']}: "
     f"geliefert {bsp5['u']:.2f}/{bsp5['m']:.2f}, korrekt {soll(bsp5)['u']:.2f}/{soll(bsp5)['m']:.2f}.",
     "Station oder Abrechnungsmodul rechnet in Ganzzahl-Cent mit int()/Truncation. Pro Zeile kaum sichtbar, "
     "summiert über Monate aber messbar, und beim Abgleich mit Kassensystemen tauchen Cent-Differenzen auf."),
    ("Kontext: doppelt gelieferter Vorgang mit neuer ID", [a6["id"], a7["id"]],
     f"Inhaltlich identisch (Ladepunkt {a6['lp']}, Kunde {a6['kd']}, {zr(a6)}, {a6['e']:.3f} kWh, {a6['u']:.2f} EUR), "
     f"aber zwei verschiedene IDs; die Kopie steht am Dateiende. Die ID-Eindeutigkeit ist formal erfüllt, der Kunde wird doppelt belastet.",
     "Retry nach Timeout im Export bzw. At-least-once-Zustellung einer Message-Queue; der Exporter vergibt beim zweiten Versuch eine neue laufende Nummer."),
    ("Einzelzeile: Ladepunkt passt nicht zur Station (Zahlendreher)", [a8["id"]],
     f"ladepunkt_id {a8['lp']} gehört zu einer anderen Station als station_id ST-{a8['st']:03d} (richtig wäre {a8_alt}).",
     "Zahlendreher bei der manuellen Pflege der Stammdaten bzw. des Ladepunkt-Mappings bei Inbetriebnahme."),
]

b_frueh = [v for v in b_grp if v["start"].date() != B.tag.date()]
B.befunde = [
    ("Systematisch (eine Station): UTC statt Ortszeit", [v["id"] for v in sorted(b_grp, key=lambda x: x["id"])],
     f"Alle Vorgänge von ST-{b_st:03d} haben Zeitstempel in UTC statt MESZ, sind also 2 Stunden zu früh. "
     f"Bei {len(b_frueh)} davon ({', '.join(v['id'] for v in b_frueh)}) liegt der Start dadurch sogar am 2026-09-23 "
     f"und damit auf dem falschen Ladetag. Die übrigen sehen völlig normal aus.",
     "Nach einem Firmware-Update oder einer neuen Backend-Anbindung liefert eine Station UTC ohne Zeitzonenkennung; "
     "das Format bleibt korrekt, nur der Wert ist verschoben."),
    ("Einzelzeile: Testladung mit interner Testkarte", [b_t["id"]],
     f"kunde_id KD-00000 (außerhalb KD-00001 bis KD-02000), Dauer 2 Minuten, 0.412 kWh: alles unterhalb der üblichen Spannen.",
     "Servicetechniker prüft nach Wartung den Ladepunkt mit einer internen Testkarte; der Filter für Testkarten fehlt im Export."),
    ("Kontext: Lücke in der Nummerierung", [v["id"] for v in b_weg],
     f"Fünf aufeinanderfolgende IDs fehlen; die Nummerierung springt von {b_vor['id']} auf {b_nach['id']}. "
     f"Die Datei hat trotzdem rund 400 Zeilen und sieht vollständig aus.",
     "Export in Seiten (Paging) mit Offset-Fehler oder ein verworfener Teil-Batch; Vorgänge fehlen in der Abrechnung."),
]

C.hinweise = [
    (c1["id"], f"Start {ts(c1['start'])}, Ende am Folgetag {ts(c1['ende'])}. Korrekt: Der Vorgang beginnt am Ladetag."),
    (c2["id"], f"15.500 kWh × 0.39 = 6.045 → kaufmännisch 6.05 (Banker's Rounding oder float-round() ergäbe 6.04). Korrekt geliefert: 6.05 / 1.21."),
    (c3["id"], "Exakt 10 Minuten und exakt 5.000 kWh (untere Grenzen)."),
    (c4["id"], "Exakt 120 Minuten und exakt 60.000 kWh (obere Grenzen)."),
]

bsp_d = d_grp[0]
D.befunde = [
    ("Formal: UTF-8 mit BOM", [],
     "Die Datei beginnt mit dem Byte Order Mark EF BB BF. Die erste Spaltenüberschrift lautet technisch '\\ufeffladevorgang_id'; "
     "ein Parser ohne BOM-Behandlung findet die Spalte ladevorgang_id nicht.",
     "Exportjob wurde auf Excel- bzw. .NET-Standard umgestellt (Encoding.UTF8 schreibt ein BOM); im Texteditor unsichtbar."),
    ("Systematisch: veralteter Preis für TA-06", [v["id"] for v in d_grp],
     f"Alle {len(d_grp)} TA-06-Vorgänge sind mit 0.65 statt 0.69 EUR/kWh berechnet. Die Marge ist korrekt 20 % des falschen Umsatzes, "
     f"also intern konsistent. Beispiel {bsp_d['id']}: geliefert {bsp_d['u']:.2f}, korrekt {soll(bsp_d)['u']:.2f}.",
     "Preiserhöhung wurde im Tarifsystem gepflegt, der Abrechnungsexport nutzt aber noch eine gecachte alte Preistabelle."),
    ("Einzelzeile: Endzeit gleich Startzeit", [d_x["id"]],
     f"ende_zeit = start_zeit ({ts(d_x['start'])}) bei {d_x['e']:.3f} kWh; tatsächliches Ende wäre {ts(d_x_ende)} gewesen.",
     "StopTransaction ging verloren (Station offline); das Backend schließt die Sitzung mit einem Ersatzwert."),
]

E.befunde = [
    ("Formal: letzte Zeile abgeschnitten", [e_neu[-1]["id"]],
     f"Die letzte Zeile (Zeile {E.zeilen[e_neu[-1]['id']]}) endet nach {E.schnitt} Zeichen, ohne Zeilenende und mit zu wenigen Spalten.",
     "Datei wurde abgeholt, während der Upload noch lief, oder die Verbindung brach ab; es gibt keinen Done-Marker und keine Prüfsumme."),
    ("Häufung/Dateiübergreifend: Nachlieferung des Vortags inkl. Duplikaten", [v["id"] for v in nachlieferung],
     "Sechs Vorgänge am Dateiende stammen vom 2026-09-26, also vom Ladetag der vorherigen Lieferung. Drei davon sind inhaltlich identisch "
     "mit bereits gelieferten Vorgängen aus ladevorgaenge_2026-09-27_v9.csv: "
     + "; ".join(f"{k['id']} = {o['id']} (dort Zeile {D.zeilen[o['id']]})" for o, k in e_paare)
     + ". Die anderen drei sind echte Nachzügler (der letzte davon ist die abgeschnittene Zeile).",
     "Station war offline und synchronisiert nachträglich; der Export filtert nach Eingangszeit statt Startzeit und schickt beim "
     "erneuten Abgleich auch bereits gelieferte Sitzungen mit. Nur im Vergleich über Dateigrenzen hinweg sichtbar."),
    ("Systematisch (eine Station): Ladepunkt-ID ohne führende Nullen", [v["id"] for v in e_grp],
     f"Bei ST-{e_st:03d} lautet die ladepunkt_id LP-{e_st}-n statt LP-{e_st:03d}-n.",
     "Station wurde auf ein neues Backend bzw. einen OCPP-Proxy migriert, der die Stationsnummer als Ganzzahl formatiert."),
]

md = ["# Lösungsblatt Blindtest Ladevorgänge", "", f"Seed: {SEED}", "",
      "| Datei | Ladetag | Datenzeilen | Fehler | Formal korrekt |", "|---|---|---|---|---|"]
for t in (A, B, C, D, E):
    md.append(f"| {t.datei} | {t.tag:%Y-%m-%d} | {len(t.vorgaenge)} | {len(t.befunde)} | {'ja' if t.formal_ok else 'nein'} |")
for t in (A, B, C, D, E):
    md += ["", f"## {t.datei} (Ladetag {t.tag:%Y-%m-%d})", ""]
    if not t.befunde:
        md.append("Keine Fehler. Enthält bewusst Grenzfälle, die gültig sind (wer hier anschlägt, hat einen Fehlalarm):")
        md.append("")
        for i, h in t.hinweise:
            md.append(f"- {i} (Zeile {t.zeilen[i]}): {h}")
    for n, (titel, ids, was, warum) in enumerate(t.befunde, 1):
        md += [f"### {n}. {titel}", "",
               f"- **Betroffen:** {ids_text(t, ids) if ids else 'gesamte Datei (Zeile 1)'}",
               f"- **Was ist falsch:** {was}",
               f"- **Warum kommt das vor:** {warum}", ""]
with open(os.path.join(ZIEL, "loesungsblatt.md"), "w", encoding="utf-8") as f:
    f.write("\n".join(md) + "\n")

for pfad, n in ausgabe:
    print(f"{pfad}: {n} Datenzeilen + Kopfzeile")
print(f"{os.path.join(ZIEL, 'loesungsblatt.md')}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 2 (NB_T3): Blindtest-Dateien in die Ablage
#
# Kopiert die 5 CSV-Dateien aus Files/blindtest in die
# Partner-Ablage – so, als hätte der Partner sie
# geliefert. Kopie statt Verschieben: Die Originale
# bleiben für eine Wiederholung. Das Lösungsblatt
# bleibt, wo es ist.
# =====================================================

import os
import shutil

QUELLE = "/lakehouse/default/Files/blindtest"
ZIEL = "/lakehouse/default/Files/partner_ablage"

dateien = sorted(f for f in os.listdir(QUELLE) if f.endswith(".csv"))
for f in dateien:
    shutil.copy2(f"{QUELLE}/{f}", f"{ZIEL}/{f}")

print(f"✓ {len(dateien)} Dateien in die Ablage gelegt:")
for f in dateien:
    print(f"  {f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
