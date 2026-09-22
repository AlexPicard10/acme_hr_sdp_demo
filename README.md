# ACME HR "Common Base" — Démo Data + SDP (enablement Dataiku ➜ Databricks)

Asset de démonstration pour faire **monter en compétence les équipes HR (société fictive ACME)** sur Databricks
dans le cadre du **Dataiku Sunset**. Il reproduit, de bout en bout et avec des **données HR
synthétiques**, le pattern du pilote **socle commun HR** (*Common Base*) :

> extraits HR (type CESAM/SESAM) déposés dans un **Volume Unity Catalog** → ingestion **Auto Loader**
> → **Spark Declarative Pipeline** (bronze → silver → gold) → **Materialized Views** + **View**, le
> tout gouverné par **Unity Catalog**, déployé par un **Databricks Asset Bundle** et livré en **CI/CD**.

C'est l'équivalent Databricks d'un flow Dataiku, pensé comme support d'atelier.

> **SDP** (Spark Declarative Pipelines) = **LDP** (Lakeflow Declarative Pipelines), anciennement DLT —
> termes interchangeables.

## Ce que la démo illustre (fonctionnalités LDP)

| Fonctionnalité | Où | Équivalent Dataiku |
|---|---|---|
| **Streaming Table** + **Auto Loader** (`STREAM read_files`) | `bronze_employees`, `bronze_absences` | Dataset d'entrée (CDH / Volume) |
| **Expectations** (contrôles qualité déclaratifs) | `silver_employees`, `silver_absences` | Recipes de préparation + checks |
| Colonnes calculées (`age_bracket`, `seniority_years`) + join référentiel | `silver_employees` | Recipe *Prepare* + *Join* |
| **Auto CDC — SCD Type 1** (dernière version / employé) | `gold_employees_current` | Zone dédup / dernière valeur |
| **Auto CDC — SCD Type 2** (historique mobilité) | `gold_employees_history` | Historisation |
| **Materialized View** (agrégats stockés) | `gold_headcount_by_department`, `gold_absenteeism_by_department` | Datasets de sortie agrégés |
| **View** (calcul à la volée, UC) | `gold_hr_common_base` | Dataset exposé aux consommateurs |

Scénario : **Employés + Absences + Départements** (effectifs, pyramide des âges, mixité, absentéisme).

## Architecture

```
Volume UC  /Volumes/alp_demo_catalog/acme_hr/landing/
   ├── employees/   extraits master employés (JSONL, GID stables)
   └── absences/    événements d'absence (JSONL)
        │  Auto Loader
        ▼
BRONZE  bronze_employees · bronze_absences                     (streaming tables)
        ▼  nettoyage, typage, colonnes calculées, expectations, join acme_hr.departments
SILVER  silver_employees · silver_absences                     (streaming tables + expectations)
        ▼
GOLD    gold_employees_current        (Auto CDC SCD1 — le "socle commun")
        gold_employees_history        (Auto CDC SCD2 — historique)
        gold_headcount_by_department    (materialized view)
        gold_absenteeism_by_department  (materialized view)
        gold_hr_common_base           (view UC de consommation)
```

Catalog `alp_demo_catalog` · schéma unique `acme_hr` · référentiel `departments` ·
données brutes dans le Volume `alp_demo_catalog.acme_hr.landing`. Tables préfixées par couche
(`bronze_` / `silver_` / `gold_`). Les autres démos auront leur propre schéma dans ce catalog.

## Prérequis

- Databricks CLI ≥ 1.0 authentifié sur le profil **`<your-profile>`**.
- Droits `ALL_PRIVILEGES` (ou CREATE SCHEMA / VOLUME) sur le catalog **`alp_demo_catalog`**.
- Python 3.11+ : `pip install -r requirements.txt` (Faker + databricks-sdk, pour le générateur).

> Le stockage est un **Volume Unity Catalog** — plus aucun bucket S3, rôle IAM ou credential à
> provisionner. Le Volume vit dans le compte du votre workspace Databricks et est gouverné par UC.

## Déroulé (pas à pas)

### 1. Setup Unity Catalog (schémas + Volume) & master data
Exécuter le notebook `src/00-Utiles/00-MasterData-build.py` dans le votre workspace Databricks. Il crée le
schéma `acme_hr`, le **Volume** `alp_demo_catalog.acme_hr.landing` (sous-dossiers `employees/`,
`absences/`) qui recevra les extraits bruts, et le référentiel `alp_demo_catalog.acme_hr.departments`.

### 2. Générer et charger les données brutes dans le Volume
```bash
python scripts/generate-hr-data.py --seed            # charge initiale (~2500 employés + 12 mois d'absences)
databricks fs ls dbfs:/Volumes/alp_demo_catalog/acme_hr/landing/employees \
  --profile <your-profile>
```
L'upload passe par la Files API du SDK Databricks (profil `<your-profile>`) — aucun
accès AWS requis.

### 3. Déployer et lancer le pipeline LDP
```bash
databricks bundle validate -t dev --profile <your-profile>
databricks bundle deploy   -t dev --profile <your-profile>
databricks bundle run hr_pipeline_dlt -t dev --profile <your-profile>
```

### 4. Explorer les résultats
Lancer les requêtes de `src/01-HR_DLT/03-Explorations/hr_exploration.sql` sur un SQL Warehouse
(streaming tables, SCD1/SCD2, materialized views, view).

### 5. Démontrer l'incrémental + le CDC
```bash
python scripts/generate-hr-data.py --increment       # nouvel extrait avec mobilités/embauches/départs
databricks bundle run hr_pipeline_dlt -t dev --profile <your-profile>
```
Seuls les nouveaux fichiers sont ingérés (Auto Loader, directory-listing sur le Volume).
`gold_employees_current` reflète l'état à jour (SCD1) ; `gold_employees_history` accumule
l'historique des mobilités (SCD2).

## CI/CD (GitHub Actions)

Le projet illustre le cycle **DAB + CI/CD** typique d'un projet Databricks versionné dans Git :

| Workflow | Déclencheur | Action |
|---|---|---|
| [`.github/workflows/validate.yml`](.github/workflows/validate.yml) | Pull request vers `main` | `databricks bundle validate --target dev` |
| [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml) | Push / merge sur `main` | `databricks bundle validate` puis `deploy --target prod` |

**Authentification CI** : machine-to-machine via **service principal OAuth**. Configurer 3 secrets
(dépôt ou organisation GitHub) :

| Secret | Valeur |
|---|---|
| `DATABRICKS_HOST` | `https://<your-workspace>.cloud.databricks.com` |
| `DATABRICKS_CLIENT_ID` | Application ID du service principal |
| `DATABRICKS_CLIENT_SECRET` | Secret OAuth du service principal |

Le service principal doit avoir les droits de déploiement (pipelines, schéma `prod`) sur le
workspace, plus `READ VOLUME` sur `alp_demo_catalog.acme_hr.landing`. Le CLI est installé
par l'action officielle `databricks/setup-cli`. Le flux : *créer une branche → PR (validate) → merge
sur main (deploy prod) → run du pipeline* (manuel ou décommenter l'étape "Run pipeline" dans `deploy.yml`).

### Démo en local (équivalent des étapes CI/CD)
```bash
databricks bundle validate --target dev  --profile <your-profile>   # ce que fait la CI
databricks bundle deploy   --target prod --profile <your-profile>   # ce que fait la CD
```

## Aller plus loin (optionnel)
- **Dashboard AI/BI** sur `gold_headcount_by_department`, `gold_absenteeism_by_department` et
  `gold_hr_common_base` (effectifs par BU, pyramide des âges, taux d'absentéisme par mois/type).
- **Job / Workflow** planifié remplaçant le scénario Dataiku (schedule + notifications).
- Ajouter d'autres sources HR (paie, formation) en réutilisant le même squelette bronze→silver→gold.

## Structure
```
acme_hr_sdp_demo/
├── databricks.yml                     # bundle DAB clean (variables, targets dev/prod)
├── requirements.txt / pyproject.toml
├── .github/workflows/
│   ├── validate.yml                   # CI : bundle validate sur PR
│   └── deploy.yml                     # CD : bundle deploy sur merge main
├── resources/
│   └── hr_pipeline_dlt.pipeline.yml   # pipeline SDP serverless
├── scripts/
│   └── generate-hr-data.py            # générateur HR synthétique -> Volume UC
└── src/
    ├── 00-Utiles/00-MasterData-build.py
    └── 01-HR_DLT/
        ├── 00-Data-Sources/           # bronze (Auto Loader)
        ├── 01-Employees/              # silver + gold (SCD1/SCD2)
        ├── 02-Absences/               # silver + materialized views + view
        └── 03-Explorations/           # requêtes de validation (hors pipeline)
```
