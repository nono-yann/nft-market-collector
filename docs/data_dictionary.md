# Dictionnaire de données

Contrat de référence : `config/data_contract.yaml` (version 1.2.0).

## Table `fact_sale` (curated, SQLite) — export `data/curated/sales_curated.csv`

**Grain** : une ligne = une vente (`buyNow`) d'un NFT d'une collection Solana suivie, sur Magic Eden.
**Clé métier** : `sale_id`.

| Champ | Type | Obligatoire | Description | Exemple |
|---|---|---|---|---|
| `sale_id` | TEXT (PK) | Oui | Signature de la transaction Solana | `Dhb7H5…u52i` (88 car. base58) |
| `collection_symbol` | TEXT (FK) | Oui | Symbole Magic Eden de la collection | `okay_bears` |
| `token_mint` | TEXT | Oui | Adresse mint du NFT vendu | `4qUtgaQM…2ykB` |
| `sold_at` | TEXT ISO 8601 | Oui | Horodatage UTC de la vente | `2026-11-07T15:39:20Z` |
| `sale_date` | TEXT | Oui | Date UTC | `2026-11-07` |
| `price_sol` | REAL > 0 | Oui | Prix payé en SOL | `3.0` |
| `sol_usd_rate` | REAL | Non | Cours SOL/USD du jour (CoinGecko) | `89.14` |
| `price_usd` | REAL | Non | `price_sol × sol_usd_rate` | `267.42` |
| `buyer_id` | TEXT(16) | Oui | Pseudonyme acheteur : SHA-256(sel + wallet), 16 hex | `05c965fd146cc105` |
| `seller_id` | TEXT(16) | Oui | Pseudonyme vendeur (≠ `buyer_id`) | `8907da5ce5d77d00` |
| `marketplace_source` | TEXT | Non | Programme de marché (`magiceden_v2`, `mmm` = pool AMM) | `magiceden_v2` |
| `run_id` | TEXT | Oui | Run de première ingestion (lineage) | `20261107T204611Z` |
| `source_file` | TEXT | Oui | Fichier raw d'origine | `magiceden_okay_bears_20261107T204611Z.json` |
| `contract_version` | TEXT | Oui | Version du contrat appliquée | `1.2.0` |
| `loaded_at` | TEXT | Oui | Horodatage d'insertion | `2026-11-07T21:02:58Z` |

Contraintes : `PRIMARY KEY(sale_id)`, `CHECK(price_sol > 0)`, `CHECK(buyer_id <> seller_id)`, FK vers
`dim_collection`, index `(collection_symbol, sale_date)` et `(buyer_id)`.

## Table `fact_bid` — export `data/curated/bids_curated.csv`

**Grain** : une ligne = une offre d'achat (`bid`) posée sur un NFT précis ou sur toute la collection.
**Clé métier** : `bid_id` = signature + `:` + mint visé (ou `collection`). Une même transaction peut poser
plusieurs offres sur des NFT différents (constaté au profilage).

| Champ | Type | Obligatoire | Description | Exemple |
|---|---|---|---|---|
| `bid_id` | TEXT (PK) | Oui | `signature:tokenMint` ou `signature:collection` | `2Thmu…Qijw:5GUw…uHp` |
| `collection_symbol` | TEXT (FK) | Oui | Collection | `okay_bears` |
| `token_mint` | TEXT | Non | NFT visé ; vide = offre de collection | |
| `bid_scope` | TEXT | Oui | `token` ou `collection` (CHECK) | `collection` |
| `bid_channel` | TEXT | Oui | `pool_amm` (source `mmm`, automatisée) ou `manuelle` (CHECK) | `pool_amm` |
| `bid_at` | TEXT ISO 8601 | Oui | Horodatage UTC | `2026-11-07T19:45:25Z` |
| `bid_date` | TEXT | Oui | Date UTC | `2026-11-07` |
| `price_sol` | REAL > 0 | Oui | Montant offert | `1.8771` |
| `sol_usd_rate`, `price_usd` | REAL | Non | Conversion USD | |
| `bidder_id` | TEXT(16) | Oui | Pseudonyme de l'enchérisseur | `a1b2c3d4e5f60718` |
| `run_id`, `source_file`, `contract_version`, `loaded_at` | TEXT | Oui | Traçabilité | |

## Tables techniques

| Table | Rôle | Colonnes |
|---|---|---|
| `dim_collection` | collections suivies | `collection_symbol` (PK), `first_seen_run` |
| `collection_watermark` | filigrane de collecte incrémentale | `collection_symbol` (PK), `last_activity_ts` (dernier `blockTime` brut vu), `updated_by_run` |
| `run_log` | observabilité | `run_id`, `executed_at`, `mode`, `status`, `input_rows`, `accepted_rows`, `rejected_rows`, `duplicates_removed`, `inserted_rows`, `duration_seconds` |

## Data marts

| Mart | Forme | Grain | Contenu |
|---|---|---|---|
| `mart_daily_sales` | vue SQL | collection × jour | `n_sales`, `volume_sol`, `volume_usd`, `min/max_sale_sol`, `unique_buyers` |
| `mart_daily_bids` | vue SQL | collection × jour | `n_bids`, `unique_bidders`, `best_bid_sol`, `pool_bid_share_pct` |
| `mart_daily_collection` | CSV | collection × jour | jointure des deux vues + `median_sale_sol` |
| `mart_buyer_activity` | vue + CSV | acheteur | `n_purchases`, `n_collections`, `total_spent_sol`, dates, `segment` (ponctuel / récurrent / intensif) |
| `mart_bidder_activity` | vue + CSV | enchérisseur | `n_bids`, `n_collections`, `pool_bid_share_pct`, `best_bid_sol`, dates |
| `mart_collection_summary` | CSV | collection (période observée) | **produit final**, voir ci-dessous |

### `mart_collection_summary`

| Champ | KPI |
|---|---|
| `period_start`, `period_end`, `hours_observed` | période couverte par les données en base |
| `n_sales`, `volume_sol`, `volume_usd` | volume |
| `median_sale_sol`, `sale_price_trend_pct` | prix ; tendance = médiane 2de moitié / 1re moitié − 1 (≥ 3 ventes par moitié) |
| `unique_buyers`, `repeat_buyer_rate_pct`, `top10pct_buyers_volume_share_pct` | comportement des acheteurs (ventes) |
| `n_bids`, `unique_bidders`, `top_bidder_share_pct`, `pool_bid_share_pct` | concentration et automatisation de la demande |
| `median_bid_sol`, `best_bid_sol`, `bid_to_sale_pct` | niveau de la demande ; offre médiane / vente médiane |

## Zone raw

`data/raw/magiceden_<collection>_<run_id>.json` : enveloppe `_meta` (url, collection, run_id, collected_at,
pages_fetched, records, http_statuses, stop_reason, lookback_days, watermark_ts, synthetic) + `pages[]`
contenant la réponse de l'API **sans modification**. `data/raw/coingecko_sol_usd_<run_id>.json` : `_meta` + `response`.

Champs d'une activité Magic Eden : `signature`, `type`, `source`, `tokenMint`, `collection`, `collectionSymbol`,
`slot`, `blockTime`, `buyer`, `buyerReferral`, `seller`, `sellerReferral`, `price`, `priceInfo`, `image`.

## Zone rejected

`data/rejected/rejected_<run_id>.csv` : `run_id`, `dataset`, `reject_reasons` (liste `;`), `_source_file`,
`_query_collection`, champs source (wallets pseudonymisés).
