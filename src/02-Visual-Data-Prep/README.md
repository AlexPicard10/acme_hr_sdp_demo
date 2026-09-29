# Visual Data Prep : agrégats gold en no-code + IA (Lakeflow Designer)

Le pipeline SDP (`src/01-HR_DLT/`) produit les tables fiables : `silver_absences`,
`gold_employees_current` (SCD1), `gold_employees_history` (SCD2) et la materialized view `gold_employee_360`.

Les **agrégats de consommation** sont construits **sans code**, dans un **Visual Data Prep**
(Lakeflow Designer). C'est le parcours d'un analyste HR : il glisse des opérateurs sur un canvas,
voit l'aperçu des données à chaque étape et publie des **Materialized Views** gouvernées par Unity
Catalog. Un opérateur **AI Function** (`ai_classify`) classe les motifs d'absence saisis en texte libre.

| Sortie (Materialized View) | Construite avec |
|---|---|
| `gold_headcount_by_department` | Source → Filter → Prepare (Formula) → Aggregate → Output |
| `gold_absenteeism_by_department` | Source ×2 → Join → Aggregate → Output |
| `gold_absence_reasons_by_department` | Source → Filter → Unique → **AI Function (`ai_classify`)** → Join → Aggregate → Output |

Fichier attendu dans le Git folder : **`src/02-Visual-Data-Prep/hr_gold_kpis.designer.ipynb`**.
Il est orchestré après le pipeline par le job DAB `hr_360_job`.

## Prérequis

- Lakeflow Designer disponible : **+ New → Visual data prep**. Si l'option n'apparaît pas, un admin
  l'active dans les **Previews** du workspace.
- Le pipeline SDP a tourné : `silver_absences` (avec la colonne `comment`) et `gold_employees_current`
  existent dans `alp_demo_catalog.acme_hr`.
- Compute **serverless** et droit d'utiliser les AI Functions (endpoints Foundation Model).

## Construction pas à pas

Créer le fichier : dans le Git folder, dossier `src/02-Visual-Data-Prep/`, **+ New → Visual data prep**,
puis le nommer `hr_gold_kpis`. Créer ensuite un **Group** par sortie (A, B, C) pour garder le canvas lisible.

### A. Effectifs par département → `gold_headcount_by_department`

1. **Source** : table `alp_demo_catalog.acme_hr.gold_employees_current`.
2. **Filter** : `status = 'Active'` (garder la sortie *Included*).
3. **Prepare → Formula**, deux colonnes :
   - `is_cdi` = `CASE WHEN contract_type = 'CDI' THEN 1 ELSE 0 END`
   - `is_female` = `CASE WHEN gender = 'F' THEN 1 ELSE 0 END`
4. **Aggregate** : group by `department_id`, `department_name`, `business_unit`, `region`.
   - `COUNT(employee_id)` → `headcount`
   - `AVG(age)` → `avg_age`
   - `AVG(seniority_years)` → `avg_seniority_years`
   - `SUM(is_cdi)` → `cdi_count`
   - `SUM(is_female)` → `female_count`
5. **Prepare → Formula** : `female_pct` = `ROUND(100.0 * female_count / headcount, 1)`
   (et `ROUND(avg_age, 1)`, `ROUND(avg_seniority_years, 1)` si souhaité).
6. **Output** : **Materialized view**, catalog `alp_demo_catalog`, schéma `acme_hr`,
   nom `gold_headcount_by_department`.

Résultat attendu : **12 lignes**, une par département.

### B. Absentéisme par département / mois / type → `gold_absenteeism_by_department`

1. **Source** : `alp_demo_catalog.acme_hr.silver_absences`.
2. **Source** : `alp_demo_catalog.acme_hr.gold_employees_current` (on peut réutiliser celle de A).
3. **Join** *Inner* sur `employee_id`. Garder côté absences : `employee_id`, `absence_month`,
   `absence_type`, `days` ; côté employés : `department_id`, `department_name`, `business_unit`.
4. **Aggregate** : group by `department_id`, `department_name`, `business_unit`, `absence_month`,
   `absence_type`.
   - `COUNT DISTINCT(employee_id)` → `employees_absent`
   - `COUNT(*)` → `absence_events`
   - `SUM(days)` → `total_absence_days`
   - `AVG(days)` → `avg_days_per_absence`
5. **Output** : **Materialized view** `alp_demo_catalog.acme_hr.gold_absenteeism_by_department`.

### C. Motifs d'absence classés par l'IA → `gold_absence_reasons_by_department`

Environ 30 % des absences portent un motif libre (`comment`), par exemple « Chute dans l'escalier du
bâtiment B pendant le service » ou « Garde des enfants, école fermée ». L'IA les range dans des
catégories exploitables par les RH.

1. **Source** : `silver_absences`.
2. **Filter** : `comment IS NOT NULL`.
3. **Unique** sur la colonne `comment`, puis **Select** pour ne garder que `comment`. On classe
   chaque motif **une seule fois** (quelques dizaines d'appels IA au lieu de dizaines de milliers).
4. **AI Function** : fonction **`ai_classify`**, colonne d'entrée `comment`, labels :
   `santé`, `famille`, `formation`, `lié au travail`, `personnel`. Nommer la sortie `reason_category`.
   Vérifier dans l'aperçu que les motifs ambigus (mal de dos sur chantier, stress lié à la charge
   de travail…) partent bien en `lié au travail`.
5. **Join** *Inner* : le résultat de l'étape 4 avec la sortie de l'étape 2 sur `comment`, puis avec
   `gold_employees_current` sur `employee_id`.
6. **Aggregate** : group by `department_name`, `business_unit`, `reason_category`.
   - `COUNT(*)` → `absence_events`
   - `SUM(days)` → `total_absence_days`
7. **Output** : **Materialized view** `alp_demo_catalog.acme_hr.gold_absence_reasons_by_department`.

### (Option) Contrôle qualité no-code

Avant l'Output de C, ajouter un opérateur **Guardrails** : `reason_category` *Not null* et
*Accepted values* (les 5 labels). C'est l'équivalent no-code des **Expectations** du pipeline SDP.

## Raccourci : générer le canvas avec Genie Code

Ouvrir **Genie Code** dans Designer et coller :

```text
Dans le catalog alp_demo_catalog, schéma acme_hr, construis trois flux, chacun dans son propre groupe :

A) À partir de gold_employees_current filtré sur status = 'Active', calcule par department_id,
department_name, business_unit, region : headcount (nombre d'employés), avg_age, avg_seniority_years
(arrondis à 1 décimale), cdi_count (contract_type = 'CDI'), female_count (gender = 'F') et female_pct
(pourcentage de femmes, 1 décimale). Publie en materialized view gold_headcount_by_department.

B) Joins silver_absences et gold_employees_current sur employee_id (inner). Par department_id,
department_name, business_unit, absence_month, absence_type : employees_absent (employee_id
distincts), absence_events, total_absence_days (somme de days), avg_days_per_absence (moyenne,
1 décimale). Publie en materialized view gold_absenteeism_by_department.

C) Depuis silver_absences, garde les lignes où comment n'est pas nul, déduplique les valeurs de
comment, puis classe chaque comment avec ai_classify parmi les labels : santé, famille, formation,
lié au travail, personnel (colonne reason_category). Rejoins le résultat aux absences sur comment,
puis à gold_employees_current sur employee_id. Par department_name, business_unit, reason_category :
absence_events et total_absence_days. Publie en materialized view gold_absence_reasons_by_department.
```

Relire le canvas proposé (noms de colonnes, type de jointure) avant de l'exécuter.

## Exécuter, versionner, orchestrer

1. **Run** interactif : vérifier l'aperçu de chaque Output, puis contrôler les 3 MV dans le Catalog Explorer.
2. Sauvegarder : le fichier apparaît en `hr_gold_kpis.designer.ipynb` dans le Git folder.
   **Commit & push** depuis le dialogue Git.
3. Le job DAB `hr_360_job` (`resources/hr_360.job.yml`) enchaîne
   **pipeline SDP → Visual Data Prep**. Dans un bundle, un Visual Data Prep se déclare comme une
   `notebook_task` même si l'UI Jobs l'affiche comme « Visual data prep ».

> Les anciennes MV de même nom étaient gérées par le pipeline SDP. Elles disparaissent à la full
> refresh qui suit leur retrait du code. Si un nom est encore pris, supprimer l'objet
> (`DROP MATERIALIZED VIEW …`) avant le premier run du Visual Data Prep.
