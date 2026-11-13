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

# # NB_T0 Zurücksetzen (nur Test)
# Löscht alle Daten der Strecke für einen Test von null. Lager (`beispieldaten`) und `blindtest` bleiben.
# 1. In Zelle 2a `BESTAETIGUNG = "ALLES LOESCHEN"` eintragen → Run all
# 2. Danach **NB_01** ausführen (legt alles leer neu an)
# 3. Zelle 2a wieder leeren

# CELL ********************

# =====================================================
# Zelle 2a (NB_T0): Bestätigung
#
# Schutz vor versehentlichem Löschen: Nur wenn hier
# genau ALLES LOESCHEN steht, wird etwas gelöscht.
# Nach dem Lauf wieder leeren.
# =====================================================

BESTAETIGUNG = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# Zelle 3 (NB_T0): Alles zurücksetzen
#
# Löscht für einen Test von null:
#   Tabellen  alle in steuerung, qualitaet, silber, gold
#   Dateien   bronze, partner_ablage, berichte
# Bleibt:     beispieldaten (Lager + erwartung.json),
#             blindtest
# Sicherungen:
#   - nur im Workspace, dessen Name auf _dev endet
#   - nur mit BESTAETIGUNG = "ALLES LOESCHEN"
# Danach NB_01 ausführen: legt alles leer neu an.
# =====================================================

import os
import shutil

SCHEMAS_LEEREN = ["steuerung", "qualitaet", "silber", "gold"]
ORDNER_LEEREN = ["bronze", "partner_ablage", "berichte"]

workspace = notebookutils.runtime.context.get("currentWorkspaceName") or ""
if not workspace.endswith("_dev"):
    raise ValueError(f"Abbruch: Workspace „{workspace}“ ist nicht dev. Hier wird nichts gelöscht.")
if BESTAETIGUNG != "ALLES LOESCHEN":
    raise ValueError('Abbruch: In Zelle 2a BESTAETIGUNG = "ALLES LOESCHEN" eintragen.')

tabellen = 0
for s in SCHEMAS_LEEREN:
    for t in spark.sql(f"SHOW TABLES IN {s}").collect():
        spark.sql(f"DROP TABLE {s}.{t.tableName}")
        tabellen += 1

for o in ORDNER_LEEREN:
    shutil.rmtree(f"/lakehouse/default/Files/{o}", ignore_errors=True)

print(f"✓ {workspace}: {tabellen} Tabellen und die Ordner {', '.join(ORDNER_LEEREN)} gelöscht.")
print("  Jetzt NB_01 ausführen, danach BESTAETIGUNG wieder leeren.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
