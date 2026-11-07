# Gouvernance, observabilité, lineage et risques

## Diagnostic V1 → V2 (séance 3)

| Élément | État V1 (séances 1-2) | Limite observée | Évolution V2 |
|---|---|---|---|
| Source | 1 collection, 1 appel | pas de pagination, prix seulement en SOL ; 0,1 % de ventes (profilage) | 4 collections paginées, ventes **et offres**, + CoinGecko (USD) |
| Collecteur | script sans reprise | une erreur réseau arrête tout ; 1 run = 12 à 42 h d'historique | retry + backoff, collecte partielle tracée, écriture atomique, **filigrane incrémental** |
| Zone raw | JSON horodaté | pas de métadonnées de collecte | enveloppe `_meta` (url, run_id, statuts HTTP, stop_reason) |
| Validation | 5 règles | pas de contrôle de schéma ni de seuil global | R0-R9, dont blocage schéma / volume / taux de rejet |
| Zone curated | CSV écrasé | doublons possibles au rejeu, pas de contraintes | SQLite : PK, CHECK, FK, UPSERT transactionnel |
| Application | tableau CSV | pas de vue d'usage | marts + rapport HTML + graphiques |

## Classification Lake / Warehouse / Mart

| Couche | Contenu du projet | Transformation | Consommateur |
|---|---|---|---|
| Lake (raw) | `data/raw/*.json` | aucune (copie brute + `_meta`) | data engineer (rejeu, audit) |
| Warehouse léger (curated) | `fact_sale`, `fact_bid`, `dim_collection`, `collection_watermark`, `run_log` | typage, UTC, séparation ventes/offres, dédup, pseudonymisation, USD | analyste (SQL) |
| Mart (gold) | `mart_daily_sales`, `mart_daily_bids`, `mart_buyer_activity`, `mart_bidder_activity`, `mart_collection_summary` | agrégations par jour / acheteur / enchérisseur / collection | utilisateur final (rapport) |

## DAG

```
read_watermarks (SQLite) ─> collect_magiceden (4 collections, jusqu'au filigrane) ─┐
                            collect_coingecko ─────────────────────────────────────┴─> validate (ventes, offres)
     ─> transform ─> publish (transaction : faits + filigranes) ─> marts CSV + HTML ─> run_report
            validate ─> rejected/*.csv
```

| Tâche | Entrée | Sortie | Retry | Échec bloquant |
|---|---|---|---|---|
| read_watermarks | `collection_watermark` | filigrane par collection | — | non (1er run : fenêtre lookback) |
| collect_magiceden | API | `raw/magiceden_*.json` | 3 essais, backoff 2/4/8 s par page | oui si **toutes** les collections échouent |
| collect_coingecko | API | `raw/coingecko_*.json` | 3 essais | non (price_usd vide + warning) |
| validate | raw | acceptées / rejets | — | oui si R0, R1 ou R9 |
| transform | acceptées + cours | curated (mémoire) | — | oui |
| publish | curated | SQLite (faits + filigranes) + marts + HTML | transaction rollback | oui ; le filigrane n'avance pas |
| run_report | état du run | `reports/run_report.json`, `run_log` | — | toujours exécuté (`finally`) |

## Observabilité

| Signal | Mesure (champ du run report) | Seuil / objectif | Action |
|---|---|---|---|
| Fraîcheur | `R8_FRESHNESS.details.age_hours` | < 72 h | warning |
| Complétude | `R10_COVERAGE.details` (heures couvertes, trou daté) | aucun trou | warning ; augmenter la fréquence |
| Volume | `input_rows`, `raw_activities` | > 0 | blocage `FAILED_EMPTY_SOURCE` |
| Qualité | `rejected_rows / input_rows` | ≤ 2 % OK, ≤ 20 % warning, > 20 % blocage | `FAILED_QUALITY` |
| Doublons | `duplicates_removed`, `already_loaded_rows` | informatif | — |
| Fiabilité | `status` dans `run_log` | ≥ 95 % de runs `PASS*` sur 30 j | analyser les `error` |
| Performance | `duration_seconds` | < 5 min | réexamen ADR si > 15 min |

**Objectif de service** : chaque jour avant 08 h, les ventes et offres de J-1 des 4 collections sont publiées
sans trou de collecte, avec moins de 2 % de lignes rejetées et un rapport d'exécution archivé.

## Lineage

```
API Magic Eden ─> raw/magiceden_<coll>_<run>.json ─> validate (R0-R10) ─> fact_sale / fact_bid (run_id, source_file)
API CoinGecko  ─> raw/coingecko_sol_usd_<run>.json ─────────────────────┘          │
                                                     rejected/rejected_<run>.csv   └─> mart_* ─> market_report.html
```
Chaque ligne de `fact_sale` porte `run_id`, `source_file` et `contract_version` : on retrouve le fichier raw,
donc la réponse API exacte, à l'origine de chaque vente.

## Gouvernance

| Objet | Réponse |
|---|---|
| Owner | Yann Monkam Nono — définition du contrat, qualité, exécution |
| Catalogue | `README.md`, `docs/data_dictionary.md`, `config/data_contract.yaml` |
| Accès | Écriture : le pipeline uniquement. Lecture : analystes. Raw (wallets en clair) : owner seulement, non versionné |
| Rétention | raw 90 j ; rejets 90 j ; curated et run reports 13 mois glissants |
| Évolution | contrat versionné (semver) avec `changelog` ; `contract_version` stocké par ligne |
| Secrets | `WALLET_HASH_SALT`, `MAGICEDEN_API_KEY` par variables d'environnement ; `.env` ignoré par Git |

## Minimisation (RGPD)

| Champ | Nécessaire ? | Traitement | Conservation |
|---|---|---|---|
| Wallet acheteur / vendeur / enchérisseur | oui (KPI de concentration et de récurrence) | pseudonymisé SHA-256 salé en curated et rejets | clair : raw 90 j ; pseudonyme : 13 mois |
| Referrals, image, slot | non | non repris en curated | raw 90 j |
| Montants | oui | — | 13 mois |

Aucune donnée sensible (au sens art. 9 RGPD) n'est collectée. Les KPI publiés sont agrégés ; la liste par
acheteur ne contient que des pseudonymes.

## Revue de risques

| Risque | Probabilité | Impact | Mesure |
|---|---|---|---|
| Source indisponible / limite de débit | moyenne | run manqué | retry + backoff, rejeu, filigrane (reprise au dernier point) |
| Historique incomplet (plafond de pages) | moyenne | KPI biaisé | R10 date le trou ; fréquence augmentable |
| Schéma modifié | faible | KPI faux | R0 bloquant, contrat versionné |
| Donnée personnelle exposée | faible | élevé | pseudonymisation, raw non versionné, sel hors dépôt |
| KPI erroné (wash trading, bug prix) | moyenne | décision faussée | R4, R5, R9 ; médiane plutôt que moyenne |
| Couverture partielle du marché | élevée | volume sous-estimé | limite documentée ; seconde marketplace en perspective |
