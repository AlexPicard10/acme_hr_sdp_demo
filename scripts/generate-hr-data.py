#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Générateur de données HR synthétiques pour la démo LDP "Common Base" (enablement HR).

Remplace le scraper du projet modèle par des données 100% synthétiques (Faker fr_FR).
Produit deux datasets, écrits en JSONL (un objet JSON par ligne) puis uploadés dans un Volume UC :

    /Volumes/alp_demo_catalog/acme_hr/landing/employees/  -> extraits "master" employés (CESAM/SESAM)
    /Volumes/alp_demo_catalog/acme_hr/landing/absences/   -> événements d'absence

L'upload utilise le SDK Databricks (Files API). L'auth suit la résolution standard du SDK :
définissez `DATABRICKS_CONFIG_PROFILE=<votre-profil>` (ou utilisez le profil `DEFAULT`).
Prérequis : le Volume existe (exécuter d'abord src/00-Utiles/00-MasterData-build.py).

Usage
-----
    # Charge initiale : roster d'employés + historique d'absences (12 mois)
    python scripts/generate-hr-data.py --seed [--employees 2500]

    # Extrait incrémental : nouvel extrait employés (avec quelques changements) + nouvelles absences
    python scripts/generate-hr-data.py --increment

    # Générer localement sans uploader dans le Volume (inspection)
    python scripts/generate-hr-data.py --seed --no-upload

Le roster est persisté dans data/state/roster.json pour garder des GID stables entre les runs
(condition indispensable pour démontrer le CDC / SCD sur employee_gid).
"""
import argparse
import datetime as dt
import json
import os
import random
import sys

try:
    from faker import Faker
except ImportError:
    sys.exit("Faker manquant. Installez les dépendances : pip install -r requirements.txt")

# ----------------------------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------------------------
VOLUME_ROOT = "/Volumes/alp_demo_catalog/acme_hr/landing"
LOCAL_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
STATE_FILE = os.path.join(LOCAL_DIR, "state", "roster.json")

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


# ----------------------------------------------------------------------------------------------
# Roster (persistance des GID)
# ----------------------------------------------------------------------------------------------
def _birth_hire_dates():
    birth = fake.date_of_birth(minimum_age=18, maximum_age=63)
    earliest_hire = birth + dt.timedelta(days=18 * 365)
    hire = fake.date_between(start_date=earliest_hire, end_date="today")
    return birth.isoformat(), hire.isoformat()


def build_roster(n_employees):
    roster = []
    for i in range(1, n_employees + 1):
        gender = random.choice(["F", "M"])
        first = fake.first_name_female() if gender == "F" else fake.first_name_male()
        last = fake.last_name()
        birth_date, hire_date = _birth_hire_dates()
        roster.append({
            "employee_gid": f"GID{i:07d}",
            "first_name": first,
            "last_name": last,
            "gender": gender,
            "birth_date": birth_date,
            "hire_date": hire_date,
            "department_id": random.choice(DEPARTMENT_IDS),
            "job_title": random.choice(JOB_TITLES),
            "contract_type": random.choices(CONTRACT_TYPES)[0],
            "work_location": random.choice(WORK_LOCATIONS),
            "manager_gid": f"GID{random.randint(1, max(1, n_employees // 20)):07d}",
            "email": f"{first.lower()}.{last.lower()}@acme-demo.com".replace(" ", "").replace("'", ""),
            "fte": random.choice([1.0, 1.0, 1.0, 0.8, 0.5]),
            "status": "Active",
        })
    return roster


def mutate_roster(roster):
    """Applique de petits changements pour l'extrait incrémental (démontre CDC/SCD)."""
    # ~3% de mobilités internes (changement de département) -> déclenche SCD2 TRACK HISTORY
    for emp in random.sample(roster, k=max(1, len(roster) * 3 // 100)):
        emp["department_id"] = random.choice(DEPARTMENT_IDS)
    # ~1% de promotions (changement de poste)
    for emp in random.sample(roster, k=max(1, len(roster) // 100)):
        emp["job_title"] = random.choice(JOB_TITLES)
    # ~0.5% de départs (statut Inactive) -> mis à jour dans l'état courant (SCD1)
    for emp in random.sample(roster, k=max(1, len(roster) // 200)):
        emp["status"] = "Inactive"
    # Quelques CDD -> CDI
    for emp in random.sample(roster, k=max(1, len(roster) // 100)):
        if emp["contract_type"] in ("CDD", "Intérim"):
            emp["contract_type"] = "CDI"
    # Nouvelles embauches
    next_id = max(int(e["employee_gid"][3:]) for e in roster) + 1
    for j in range(random.randint(5, 20)):
        gender = random.choice(["F", "M"])
        first = fake.first_name_female() if gender == "F" else fake.first_name_male()
        last = fake.last_name()
        birth_date, _ = _birth_hire_dates()
        roster.append({
            "employee_gid": f"GID{next_id + j:07d}",
            "first_name": first, "last_name": last, "gender": gender,
            "birth_date": birth_date,
            "hire_date": dt.date.today().isoformat(),
            "department_id": random.choice(DEPARTMENT_IDS),
            "job_title": random.choice(JOB_TITLES),
            "contract_type": random.choices(CONTRACT_TYPES)[0],
            "work_location": random.choice(WORK_LOCATIONS),
            "manager_gid": f"GID{random.randint(1, len(roster) // 20):07d}",
            "email": f"{first.lower()}.{last.lower()}@acme-demo.com".replace(" ", "").replace("'", ""),
            "fte": 1.0, "status": "Active",
        })
    return roster


# ----------------------------------------------------------------------------------------------
# Génération des extraits
# ----------------------------------------------------------------------------------------------
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


# ----------------------------------------------------------------------------------------------
# I/O : écriture JSONL locale + upload dans le Volume UC
# ----------------------------------------------------------------------------------------------
def write_jsonl(records, dataset, filename):
    local_path = os.path.join(LOCAL_DIR, dataset)
    os.makedirs(local_path, exist_ok=True)
    full = os.path.join(local_path, filename)
    with open(full, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"  ✓ {len(records):>6} lignes -> {full}")
    return full


def _volume_client():
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        sys.exit("databricks-sdk manquant. Installez les dépendances : pip install -r requirements.txt")
    # Auth via la résolution standard du SDK (profil DATABRICKS_CONFIG_PROFILE, DEFAULT, ou env).
    return WorkspaceClient()


def upload_volume(w, local_file, dataset, filename):
    volume_path = f"{VOLUME_ROOT}/{dataset}/{filename}"
    try:
        with open(local_file, "rb") as f:
            w.files.upload(volume_path, f, overwrite=True)
        print(f"  ✓ upload {volume_path}")
    except Exception as e:  # noqa: BLE001 — on veut un message lisible en démo
        print(f"  ✗ upload KO ({volume_path}) : {e}")


def save_state(roster):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(roster, f)


def load_state():
    if not os.path.exists(STATE_FILE):
        sys.exit("Aucun roster trouvé. Lancez d'abord --seed.")
    with open(STATE_FILE, encoding="utf-8") as f:
        return json.load(f)


# ----------------------------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(description="Générateur de données HR synthétiques (démo LDP).")
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--seed", action="store_true", help="Charge initiale (roster + 12 mois d'absences).")
    mode.add_argument("--increment", action="store_true", help="Extrait incrémental (changements + nouvelles absences).")
    p.add_argument("--employees", type=int, default=2500, help="Nombre d'employés pour --seed (défaut 2500).")
    p.add_argument("--no-upload", action="store_true", help="Génère en local sans uploader dans le Volume UC.")
    args = p.parse_args()

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    today = dt.date.today().isoformat()
    extract_ts = dt.datetime.now().isoformat(timespec="microseconds")  # clé de séquence CDC (monotone)

    if args.seed:
        print(f"[SEED] Génération de {args.employees} employés + historique d'absences…")
        roster = build_roster(args.employees)
        save_state(roster)

        emp_file = write_jsonl(employee_extract(roster, today, extract_ts), "employees", f"employees_{today}_{ts}.json")
        n_abs = args.employees * 3  # ~3 absences/employé sur l'année
        abs_records = absence_events(roster, dt.date.today() - dt.timedelta(days=365), dt.date.today(), n_abs)
        abs_file = write_jsonl(abs_records, "absences", f"absences_seed_{ts}.json")

    else:  # --increment
        print("[INCREMENT] Nouvel extrait employés (avec changements) + nouvelles absences…")
        roster = mutate_roster(load_state())
        save_state(roster)

        emp_file = write_jsonl(employee_extract(roster, today, extract_ts), "employees", f"employees_{today}_{ts}.json")
        abs_records = absence_events(roster, dt.date.today() - dt.timedelta(days=14), dt.date.today(), 200)
        abs_file = write_jsonl(abs_records, "absences", f"absences_{ts}.json")

    if args.no_upload:
        print("\n(--no-upload) Fichiers écrits localement uniquement.")
        return

    print(f"\nUpload vers le Volume UC ({VOLUME_ROOT})…")
    w = _volume_client()
    upload_volume(w, emp_file, "employees", os.path.basename(emp_file))
    upload_volume(w, abs_file, "absences", os.path.basename(abs_file))
    print("\nTerminé.")


if __name__ == "__main__":
    main()
