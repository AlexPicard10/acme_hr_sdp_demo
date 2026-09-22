# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Générateur de données HR synthétiques — notebook (démo SDP "Common Base")
# MAGIC
# MAGIC Générateur de données HR synthétiques, pensé pour être exécuté **directement dans le workspace**
# MAGIC (Git folder) — support de formation.
# MAGIC
# MAGIC - **Widgets** au lieu d'arguments CLI (`mode`, `employees`, `catalog`, `schema`).
# MAGIC - Écrit les extraits JSONL **directement dans le Volume UC** (`.../landing/employees|absences/`).
# MAGIC - Persiste le **roster dans le Volume** (`.../landing/_state/roster.json`) pour garder des GID
# MAGIC   stables entre les runs → indispensable pour démontrer le CDC / SCD (mode `increment`).
# MAGIC
# MAGIC > Prérequis : le schéma + le Volume `landing` existent (voir `00-MasterData-build.py`).
# MAGIC > Le pipeline lit `.../landing/employees` et `.../landing/absences` ; le sous-dossier `_state/`
# MAGIC > n'est pas ingéré.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Paramètres (widgets)

# COMMAND ----------

dbutils.widgets.dropdown("mode", "seed", ["seed", "increment"], "Mode")
dbutils.widgets.text("employees", "50000", "Nb employés (seed)")
dbutils.widgets.text("catalog", "alp_demo_catalog", "Catalog")
dbutils.widgets.text("schema", "acme_hr", "Schema")

mode = dbutils.widgets.get("mode")
n_employees = int(dbutils.widgets.get("employees"))
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

VOLUME_ROOT = f"/Volumes/{catalog}/{schema}/landing"
STATE_PATH = f"{VOLUME_ROOT}/_state/roster.json"
print(f"mode={mode} | employees={n_employees} | cible={VOLUME_ROOT}")

# COMMAND ----------

# MAGIC %md ## Logique de génération (identique au script CLI — Faker fr_FR, seed fixe)

# COMMAND ----------

import datetime as dt
import json
import os
import random

from faker import Faker

DEPARTMENT_IDS = [f"D{n:03d}" for n in range(1, 13)]  # D001..D012 (cf. 00-MasterData-build.py)
CONTRACT_TYPES = ["CDI", "CDI", "CDI", "CDD", "Alternance", "Stage", "Intérim"]  # CDI surpondéré
JOB_TITLES = [
    "Technicien", "Ingénieur", "Chef de projet", "Analyste", "Responsable",
    "Chargé d'affaires", "Gestionnaire", "Directeur", "Consultant", "Opérateur",
]
WORK_LOCATIONS = [
    "Paris La Défense", "Lyon Part-Dieu", "Lille", "Bordeaux", "Marseille",
    "Strasbourg", "Toulouse", "Bruxelles",
]
ABSENCE_TYPES = [
    ("Congés payés", 30), ("Maladie", 25), ("RTT", 20),
    ("Formation", 12), ("Congé maternité", 5), ("Congé sans solde", 8),
]

fake = Faker("fr_FR")
Faker.seed(42)
random.seed(42)


def _birth_hire_dates():
    birth = fake.date_of_birth(minimum_age=18, maximum_age=63)
    earliest_hire = birth + dt.timedelta(days=18 * 365)
    hire = fake.date_between(start_date=earliest_hire, end_date="today")
    return birth.isoformat(), hire.isoformat()


def build_roster(n):
    roster = []
    for i in range(1, n + 1):
        gender = random.choice(["F", "M"])
        first = fake.first_name_female() if gender == "F" else fake.first_name_male()
        last = fake.last_name()
        birth_date, hire_date = _birth_hire_dates()
        roster.append({
            "employee_gid": f"GID{i:07d}",
            "first_name": first, "last_name": last, "gender": gender,
            "birth_date": birth_date, "hire_date": hire_date,
            "department_id": random.choice(DEPARTMENT_IDS),
            "job_title": random.choice(JOB_TITLES),
            "contract_type": random.choices(CONTRACT_TYPES)[0],
            "work_location": random.choice(WORK_LOCATIONS),
            "manager_gid": f"GID{random.randint(1, max(1, n // 20)):07d}",
            "email": f"{first.lower()}.{last.lower()}@acme-demo.com".replace(" ", "").replace("'", ""),
            "fte": random.choice([1.0, 1.0, 1.0, 0.8, 0.5]),
            "status": "Active",
        })
    return roster


def mutate_roster(roster):
    """Petits changements pour l'extrait incrémental (démontre CDC/SCD)."""
    for emp in random.sample(roster, k=max(1, len(roster) * 3 // 100)):        # ~3% mobilités
        emp["department_id"] = random.choice(DEPARTMENT_IDS)
    for emp in random.sample(roster, k=max(1, len(roster) // 100)):            # ~1% promotions
        emp["job_title"] = random.choice(JOB_TITLES)
    for emp in random.sample(roster, k=max(1, len(roster) // 200)):            # ~0.5% départs
        emp["status"] = "Inactive"
    for emp in random.sample(roster, k=max(1, len(roster) // 100)):            # CDD/Intérim -> CDI
        if emp["contract_type"] in ("CDD", "Intérim"):
            emp["contract_type"] = "CDI"
    next_id = max(int(e["employee_gid"][3:]) for e in roster) + 1              # embauches
    for j in range(random.randint(5, 20)):
        gender = random.choice(["F", "M"])
        first = fake.first_name_female() if gender == "F" else fake.first_name_male()
        last = fake.last_name()
        birth_date, _ = _birth_hire_dates()
        roster.append({
            "employee_gid": f"GID{next_id + j:07d}",
            "first_name": first, "last_name": last, "gender": gender,
            "birth_date": birth_date, "hire_date": dt.date.today().isoformat(),
            "department_id": random.choice(DEPARTMENT_IDS),
            "job_title": random.choice(JOB_TITLES),
            "contract_type": random.choices(CONTRACT_TYPES)[0],
            "work_location": random.choice(WORK_LOCATIONS),
            "manager_gid": f"GID{random.randint(1, len(roster) // 20):07d}",
            "email": f"{first.lower()}.{last.lower()}@acme-demo.com".replace(" ", "").replace("'", ""),
            "fte": 1.0, "status": "Active",
        })
    return roster


def employee_extract(roster, extract_date, extract_ts):
    """Un enregistrement master par employé.

    - `extract_date` : jour métier de l'extrait (DATE, pour l'affichage).
    - `extract_ts`   : instant précis de l'extraction (TIMESTAMP) = **clé de séquence CDC**.
      On séquence le CDC par un timestamp monotone (et non par la date) pour que deux extraits
      produits le MÊME jour restent strictement ordonnés (indispensable en atelier).
    """
    return [dict(emp, extract_date=extract_date, extract_ts=extract_ts) for emp in roster]


def absence_events(roster, start, end, n_events):
    events, active = [], [e for e in roster if e["status"] == "Active"]
    labels = [t for t, _ in ABSENCE_TYPES]
    weights = [w for _, w in ABSENCE_TYPES]
    for k in range(n_events):
        emp = random.choice(active)
        s = fake.date_between(start_date=start, end_date=end)
        atype = random.choices(labels, weights=weights)[0]
        days = {
            "Congés payés": random.randint(1, 15), "Maladie": random.randint(1, 10),
            "RTT": 1, "Formation": random.randint(1, 5),
            "Congé maternité": random.randint(60, 120), "Congé sans solde": random.randint(1, 20),
        }[atype]
        e = s + dt.timedelta(days=days - 1)
        events.append({
            "absence_id": f"ABS-{s.year}-{k:08d}",
            "employee_gid": emp["employee_gid"],
            "absence_type": atype,
            "start_date": s.isoformat(),
            "end_date": e.isoformat(),
            "days": days,
            "event_timestamp": dt.datetime.combine(s, dt.time(8, 0)).isoformat(),
        })
    return events


# COMMAND ----------

# MAGIC %md ## I/O — écriture directe dans le Volume UC (pas d'upload SDK)

# COMMAND ----------

def write_jsonl(records, dataset, filename):
    target_dir = f"{VOLUME_ROOT}/{dataset}"
    os.makedirs(target_dir, exist_ok=True)  # les Volumes UC sont accessibles en écriture (FUSE)
    path = f"{target_dir}/{filename}"
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"  ✓ {len(records):>6} lignes -> {path}")
    return path


def save_state(roster):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(roster, f)


def load_state():
    if not os.path.exists(STATE_PATH):
        raise SystemExit("Aucun roster dans le Volume. Lancez d'abord le notebook en mode 'seed'.")
    with open(STATE_PATH, encoding="utf-8") as f:
        return json.load(f)


# COMMAND ----------

# MAGIC %md ## Exécution

# COMMAND ----------

ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
today = dt.date.today().isoformat()
extract_ts = dt.datetime.now().isoformat(timespec="microseconds")  # clé de séquence CDC (monotone)

if mode == "seed":
    print(f"[SEED] {n_employees} employés + historique d'absences (12 mois)…")
    roster = build_roster(n_employees)
    save_state(roster)
    write_jsonl(employee_extract(roster, today, extract_ts), "employees", f"employees_{today}_{ts}.json")
    abs_records = absence_events(roster, dt.date.today() - dt.timedelta(days=365), dt.date.today(),
                                 n_employees * 3)
    write_jsonl(abs_records, "absences", f"absences_seed_{ts}.json")
else:  # increment
    print("[INCREMENT] Nouvel extrait employés (changements) + nouvelles absences…")
    roster = mutate_roster(load_state())
    save_state(roster)
    write_jsonl(employee_extract(roster, today, extract_ts), "employees", f"employees_{today}_{ts}.json")
    abs_records = absence_events(roster, dt.date.today() - dt.timedelta(days=14), dt.date.today(), 200)
    write_jsonl(abs_records, "absences", f"absences_{ts}.json")

print("\nTerminé. Relancez le pipeline pour ingérer les nouveaux fichiers (Auto Loader).")

# COMMAND ----------

# MAGIC %md ## Contrôle rapide du contenu du Volume

# COMMAND ----------

for ds in ("employees", "absences"):
    print(f"== {ds} ==")
    for f in dbutils.fs.ls(f"{VOLUME_ROOT}/{ds}"):
        print(f"  {f.name}\t{f.size} bytes")