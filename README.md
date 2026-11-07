# NFT Market Collector — analyse du marché des NFT Solana

Projet final du module **Architectures BD et collecte de données** (ESEO, 2026-2027) — Yann Monkam Nono.

Chaîne de données batch complète et incrémentale : **API publique → zone raw horodatée → validation et
rejets → transformation → base SQLite curated → data marts → rapport HTML**.

---

## 1. Problème, utilisateur et question

| | |
|---|---|
| **Utilisateur cible** | Collectionneur·se ou analyste d'un petit fonds Web3 qui suit quelques collections NFT Solana |
| **Problème** | Le « floor price » affiché par les plateformes est instantané et peut être influencé par quelques acteurs. On ne sait pas si l'activité vient d'une vraie demande ou de quelques robots / gros acheteurs. |
| **Décision rendue possible** | Acheter, attendre ou vendre ; se méfier d'un marché porté par peu d'acteurs ou par des offres automatisées. |
| **Question centrale** | *Pour quatre collections NFT Solana de référence sur Magic Eden, comment évoluent les prix et le volume des ventes au fil des collectes quotidiennes, et la demande (ventes et offres d'achat) est-elle portée par une large base d'acheteurs ou par quelques acteurs, souvent automatisés ?* |
| **Périmètre** | Inclus : ventes (`buyNow`) et offres d'achat (`bid`) de 4 collections ; prix en SOL et USD. Exclus : annonces, retraits, mises à jour de pools, autres blockchains, métadonnées des NFT. |
| **Fréquence utile** | Quotidienne (collecte incrémentale) ; l'historique s'allonge à chaque run. |

### Cas d'usage et KPI

| En tant qu'analyste, je veux… | KPI | Données | Fréquence |
|---|---|---|---|
| suivre le prix afin de repérer une tendance | **Prix médian des ventes** et **tendance** (médiane 2de moitié vs 1re moitié de la période) ; **offre médiane / meilleure offre** et **ratio offre / vente** | ventes, offres | quotidienne |
| mesurer l'activité afin de juger la liquidité | **Nombre de ventes, volume SOL et USD**, nombre d'offres | ventes, cours SOL/USD | quotidienne |
| savoir qui porte la demande afin d'évaluer le risque | **Part des offres du 1er enchérisseur**, **part des offres automatisées (pools AMM)**, **part du volume des 10 % d'acheteurs les plus dépensiers**, **taux d'acheteurs récurrents** | acheteurs / enchérisseurs pseudonymisés | quotidienne |

### Ce que le profilage a changé (séance 1 → projet final)

Le premier run réel (07/11/2026) a collecté **60 000 activités** : 38 % de mises à jour de pools, 34,3 % d'offres,
27,6 % d'annonces et **0,1 % de ventes (52)**. Avec la pagination plafonnée à 15 000 activités par collection,
un run ne couvre que **12 à 42 heures** d'historique. Deux décisions en découlent (voir l'ADR) :
1. **ajouter les offres d'achat** (20 603 lignes exploitables) pour mesurer le comportement des acheteurs ;
2. **collecter de façon incrémentale** (filigrane par collection) et construire l'historique par
   accumulation des runs quotidiens, au lieu de viser 30 jours en un seul appel.

Rapport complet : `reports/profiling_report.md` (`python scripts/profile_raw.py`).

## 2. Sources

| Source | Rôle | Accès | Licence / conditions | Date d'accès |
|---|---|---|---|---|
| **Magic Eden API v2** — `GET /v2/collections/{symbol}/activities` | Source principale : activités (ventes, offres) par collection | HTTP public, sans clé ; limite 120 requêtes/min | Gratuite, Terms of Service Magic Eden et **attribution demandée** (faite dans le rapport) | 1er accès 07/11/2026 ; chaque fichier raw porte `_meta.collected_at` |
| **CoinGecko API** — `GET /api/v3/coins/solana/market_chart` | Enrichissement : cours SOL/USD journalier | HTTP public | API publique gratuite, attribution CoinGecko | idem |

Évaluation détaillée (dont OpenSea et Kaggle, écartés) : [`docs/source_assessment.md`](docs/source_assessment.md).
Pas de scraping : uniquement des API documentées. Aucun secret dans le dépôt.

**Données personnelles.** Les adresses de wallet sont publiques sur la blockchain mais restent des
identifiants indirects : elles sont **pseudonymisées par SHA-256 salé** dès la transformation (y compris
dans les rejets). Le sel passe par la variable `WALLET_HASH_SALT`. Les adresses en clair n'existent que dans
la zone raw (non versionnée, rétention 90 jours). L'extrait raw versionné (`data/raw/sample_*.json`) a ses
wallets masqués.

## 3. Architecture et circulation des données

```
Magic Eden API ─┐                                       ┌─> data/rejected/rejected_<run>.csv
                ├─> collect.py ─> data/raw/*.json ─> validate.py ─┤
CoinGecko API ──┘  (incrémental,   (horodaté,        (R0–R10,      └─> transform.py ─> publish.py
                    filigrane)      non modifié)      ventes+offres)                       │
        SQLite data/curated/nft_market.db : fact_sale, fact_bid, dim_collection,           │
        collection_watermark, run_log + vues mart_*                                        │
                                                                                          ▼
     data/marts/*.csv · data/curated/*_curated.csv · reports/market_report.html · reports/run_report.json
```

- Architecture V1 : [`docs/architecture_v1.png`](docs/architecture_v1.png) — V2 (cible) : [`docs/architecture_v2.png`](docs/architecture_v2.png)
- Décision d'architecture : [`docs/ADR_001.md`](docs/ADR_001.md)
- DAG, observabilité, lineage, gouvernance, rétention, risques : [`docs/governance.md`](docs/governance.md)

| Table | Grain | Clé métier |
|---|---|---|
| `fact_sale` | une vente d'un NFT d'une collection suivie | `sale_id` = signature Solana |
| `fact_bid` | une offre d'achat sur un NFT ou sur la collection | `bid_id` = signature + `:` + mint (ou `collection`) |

## 4. Installation

Prérequis : Python 3.11+ et Git.

```bash
git clone <url-du-depot> && cd nft-market-collector
python -m venv .venv
source .venv/bin/activate            # Linux / macOS
.venv\Scripts\Activate.ps1           # Windows PowerShell
pip install -r requirements.txt
```

Sel de pseudonymisation (toujours le même d'un run à l'autre ; sinon un sel de démonstration est utilisé et signalé) :

```bash
export WALLET_HASH_SALT="une-longue-chaine-aleatoire"        # Linux / macOS
$env:WALLET_HASH_SALT = "une-longue-chaine-aleatoire"        # Windows PowerShell
```

## 5. Exécution

| Commande | Effet |
|---|---|
| `python src/pipeline.py` | **Collecte réelle incrémentale** (1er run ≈ 3 min, suivants plus courts) puis validation, transformation, publication |
| `python src/pipeline.py --replay` | Retraite le dernier lot raw sans rappeler l'API (reprise après panne) |
| `./run_demo.sh` | Démo **hors ligne** sur les données synthétiques de `data/samples/` (sorties dans `demo_output/`) |
| `python scripts/profile_raw.py` | Profilage du dernier lot raw → `reports/profiling_report.md/json` |
| `python scripts/incident_demo.py` | Rejoue 5 incidents dans un espace isolé → `reports/incident_report.json` |
| `python -m pytest -q` | 27 tests (règles, offres, idempotence, filigrane, incidents, collecteur) |

Démo hors ligne sous Windows :

```powershell
python src/pipeline.py --raw-file data/samples/magiceden_mad_lads_SYNTHETIC.json --raw-file data/samples/magiceden_okay_bears_SYNTHETIC.json --raw-file data/samples/magiceden_claynosaurz_SYNTHETIC.json --raw-file data/samples/magiceden_famous_fox_federation_SYNTHETIC.json --fx-file data/samples/coingecko_sol_usd_SYNTHETIC.json --output-root demo_output
```

Les paramètres (collections, profondeur, recouvrement, seuils qualité) sont dans [`config/config.yaml`](config/config.yaml).
Code retour : `0` si `PASS*`, `1` sinon (utilisable par un planificateur : cron, Planificateur de tâches Windows).

## 6. Contrat, règles de qualité et rejets

Contrat versionné : [`config/data_contract.yaml`](config/data_contract.yaml) (v1.2.0) — dictionnaire : [`docs/data_dictionary.md`](docs/data_dictionary.md).

| Règle | Dimension | Condition | Action |
|---|---|---|---|
| R0_SCHEMA | Validité | les champs du contrat existent dans la source | **bloque** le run |
| R1_VOLUME | Complétude | au moins une vente ou une offre reçue | **bloque** le run |
| R2_REQUIRED | Complétude | champs obligatoires non vides (liste propre aux ventes / aux offres) | rejet |
| R3_TIMESTAMP | Validité | `blockTime` entier, ≥ 2021-03-01 et ≤ maintenant + 10 min | rejet |
| R4_PRICE_RANGE | Validité | 0 < prix ≤ 100 000 SOL | rejet |
| R5_SELF_TRADE | Cohérence | ventes : acheteur ≠ vendeur | rejet |
| R6_COLLECTION | Cohérence | collection renvoyée = collection demandée | rejet |
| R7_UNIQUE | Unicité | clé métier unique dans le lot | déduplication comptée |
| R8_FRESHNESS | Fraîcheur | observation la plus récente < 72 h | alerte |
| R9_REJECT_RATE | Fiabilité | taux de rejet ≤ 20 % (par jeu) | **bloque la publication** |
| R10_COVERAGE | Complétude | si le plafond de pages est atteint avant le filigrane, le trou est daté | alerte |

Chaque ligne rejetée est conservée dans `data/rejected/rejected_<run_id>.csv` avec son jeu (`sales`/`bids`),
**toutes** ses causes (`reject_reasons`, ex. `R4_PRICE_RANGE;R5_SELF_TRADE`), le fichier source et le run.
Les activités hors périmètre sont comptées par type (`out_of_scope_by_type`) et restent dans le raw.

**Idempotence** (3 niveaux) : déduplication par clé métier dans le lot ; clés primaires + `INSERT … ON CONFLICT`
en base dans une transaction unique ; exports régénérés depuis la base (écriture atomique). Le recouvrement
volontaire d'une heure entre deux collectes incrémentales est absorbé par ce mécanisme.

## 7. Résultat et consultation

- `reports/market_report.html` : synthèse par collection, prix des ventes et niveau des offres, volume,
  concentration et automatisation de la demande, segments d'acheteurs.
- `data/marts/mart_collection_summary.csv` : le tableau qui répond à la question.
- `data/marts/mart_daily_collection.csv`, `mart_buyer_activity.csv`, `mart_bidder_activity.csv`, et la base :
  `sqlite3 data/curated/nft_market.db "SELECT * FROM mart_daily_bids;"`
- `reports/run_report.json` : rapport du dernier run (historique : `reports/runs/` et table `run_log`).

## 8. Test d'incident et reprise

`python scripts/incident_demo.py` rejoue 5 incidents (preuves dans `reports/incident_report.json` et `reports/incidents/`) :

| Incident | Détection | Statut | Publication |
|---|---|---|---|
| Source indisponible | 3 tentatives avec backoff, puis erreur | `FAILED_SOURCE_UNAVAILABLE` | non |
| Changement de schéma (`price` → `priceSol`) | R0 | `FAILED_SCHEMA_CHANGE` | non |
| Source vide | R1 | `FAILED_EMPTY_SOURCE` | non |
| Doublons massifs (pages servies 3 fois) | R7 | `PASS_WITH_WARNINGS` | oui, sans doublon |
| Valeurs impossibles (40 % de prix à 0) | R4 + R9 | `FAILED_QUALITY` | non |

Preuves sur données réelles (`reports/runs/`) :
- run `20261107T210929Z` (1re collecte) : 60 000 activités, 52 ventes + 20 603 offres insérées, **5 offres dupliquées par l'API retirées** (R7), 202 s ;
- run `20261107T211854Z` (collecte incrémentale) : **arrêt au filigrane** sur les 4 collections, 2 500 activités en 7,4 s, 42 nouvelles offres, 576 lignes du recouvrement ignorées ;
- run `20261107T211915Z` (`--replay`) : **0 ligne insérée**, 618 déjà présentes (idempotence) ;
- run `20261107T210927Z` (`--replay` sans fichier raw) : `FAILED_SOURCE_UNAVAILABLE` propre, sans effet sur la base.

**Reprise** : un run en échec ne modifie jamais la base (validation avant publication, transaction
tout-ou-rien) et laisse un rapport d'exécution. Le filigrane n'avance qu'après une publication réussie :
après une panne, le run suivant recollecte depuis le dernier point connu. `--replay` retraite le dernier
lot raw sans rappeler l'API. Une collection en erreur n'empêche pas la collecte des autres.

## 9. Limites, choix écartés, perspectives

- **Limites** : l'historique se construit run après run (pas de profondeur de 30 jours au 1er run) ; si un
  run est sauté sur une collection très active, un trou est possible (détecté et daté par R10) ; Magic Eden ne
  couvre qu'une partie du marché Solana (Tensor absent) ; une personne peut utiliser plusieurs wallets ;
  R5 ne détecte que l'auto-vente directe ; les ventes sont rares (52 en ~1 jour), donc les KPI de prix
  restent fragiles tant que l'historique est court ; cours SOL/USD journalier.
- **Écartés** : OpenSea (clé API sur demande), Kaggle (non reproductible), streaming/Kafka (aucun besoin
  < 1 jour), MongoDB (requêtes d'agrégation relationnelles), Data Warehouse séparé. Voir l'ADR.
- **Perspectives** : planification (cron / GitHub Actions / Planificateur Windows) toutes les 6 h pour les
  collections très actives ; seconde marketplace (Tensor) ; détection de wash trading par graphe ;
  export Parquet si l'historique dépasse quelques millions de lignes.

## 10. Arborescence

```
config/        config.yaml, data_contract.yaml
data/raw/      réponses API brutes horodatées (sample_* versionnés, wallets masqués)
data/samples/  données SYNTHÉTIQUES pour tests/démo + fichiers d'incident
data/rejected/ data/curated/ data/marts/
docs/          sources, dictionnaire, architectures V1/V2, ADR, gouvernance, Synthese_projet_NFT_Monkam_Nono.pdf
reports/       run_report.json, market_report.html, graphiques, profiling_report, incident_report
src/           collect.py validate.py transform.py publish.py pipeline.py common.py
scripts/       profile_raw.py incident_demo.py make_synthetic_data.py
tests/         test_quality.py test_collect.py
```

## 11. Usage d'outils d'IA

- **Outil utilisé** : Claude (Anthropic).
- **Tâches assistées** : proposition d'architecture, première version du code, des tests, du générateur de
  données synthétiques et de la documentation ; analyse du profilage du premier run réel.
- **Vérifications réalisées** : exécution des 27 tests, collecte réelle (07/11/2026), rejeu et seconde
  collecte pour vérifier l'idempotence et le filigrane, rejeu des 5 incidents, relecture du contrat et des règles.
- **Parties retravaillées personnellement** : *à compléter (choix des collections, seuils, interprétation des résultats…)*.
