# Évaluation des sources

Quatre sources candidates ont été comparées. Critères : réponse directe à la question, accès stable et
autorisé, volume compatible, sens clair des champs, reproductibilité par un autre étudiant.

| Critère | **A. Magic Eden API v2** (retenue) | **B. CoinGecko API** (retenue, enrichissement) | C. OpenSea API v2 | D. Dataset Kaggle NFT |
|---|---|---|---|---|
| Producteur / URL | Magic Eden — `api-mainnet.magiceden.dev/v2/collections/{symbol}/activities` | CoinGecko — `api.coingecko.com/api/v3/coins/solana/market_chart` | OpenSea — `api.opensea.io/api/v2/events/collection/{slug}` | Contributeur Kaggle (fichier CSV) |
| Mode d'accès | HTTP GET public, pagination `offset`/`limit` | HTTP GET public | HTTP GET avec **clé API obligatoire** (demande à OpenSea) | Téléchargement manuel avec compte |
| Format | JSON (liste d'activités) | JSON (`prices`, `market_caps`, `total_volumes`) | JSON | CSV |
| Fréquence de mise à jour | Temps réel (chaque transaction) | Quotidienne (interval=daily) | Temps réel | Figée à la date de publication |
| Clé / identifiant | `signature` (transaction Solana) | horodatage (ms) | `transaction` + `order_hash` | variable selon le dataset |
| Licence / conditions | Gratuite, Terms of Service, attribution demandée | Gratuite (API publique), attribution | Terms of Service, clé personnelle | Licence propre au dataset (souvent CC) |
| Limite technique | 120 requêtes/min, pagination plafonnée | ~ quelques dizaines d'appels/min sans clé | quotas par clé | aucune, mais pas de mise à jour |
| Risque technique | Changement de schéma, indisponibilité, couverture partielle (une marketplace) | Indisponibilité, limite de débit | Délai d'obtention de la clé, secret à gérer | Non reproductible, données anciennes |
| Données personnelles | Adresses de wallet (pseudonymes publics) → **pseudonymisation** | Aucune | Adresses de wallet | Adresses de wallet |
| Réponse à la question | **Oui** : prix, date, collection, acheteur, vendeur par vente | Partielle : conversion USD | Oui | Partiellement, sur une période passée |

## Décision

- **Source principale : Magic Eden.** C'est la seule source qui donne, sans clé et de façon
  reproductible, le grain transactionnel nécessaire aux trois KPI (prix, volume **et** acheteurs).
- **Source d'enrichissement : CoinGecko** pour convertir les prix en USD. Son absence dégrade le
  résultat (`price_usd` vide, warning) mais ne bloque pas le pipeline.
- **OpenSea écarté** : clé obligatoire (délai et secret à gérer) et marché Ethereum surtout.
- **Kaggle écarté** : la collecte ne serait pas reproductible ni actualisable, ce qui contredit l'objectif.

## Champs utilisés et minimisation

| Champ source | Utilisé | Traitement |
|---|---|---|
| `signature` | oui | clé métier `sale_id` |
| `type` | oui | `buyNow` → ventes, `bid` → offres ; autres types comptés hors périmètre |
| `collectionSymbol` | oui | `collection_symbol`, contrôle R6 |
| `tokenMint` | oui | `token_mint` |
| `blockTime` | oui | `sold_at`, `sale_date` (UTC) |
| `price` | oui | `price_sol` |
| `buyer`, `seller` | oui | **pseudonymisés** → `buyer_id`, `seller_id`, `bidder_id` |
| `source` | oui | `marketplace_source` ; pour les offres → `bid_channel` (`mmm` = pool AMM automatisé) |
| `buyerReferral`, `sellerReferral`, `image`, `slot`, `priceInfo`, `collection` | non | conservés uniquement dans le raw |

## Profilage du premier run réel (07/11/2026)

| Constat | Conséquence |
|---|---|
| 60 000 activités : `poolUpdate` 38 %, `bid` 34,3 %, `list` 27,6 %, `buyNow` 0,1 % (52) | ajout des offres comme second jeu |
| 15 000 activités (plafond de 30 pages) = 12,0 h (okay_bears) à 41,9 h (famous_fox_federation) | collecte incrémentale par filigrane, règle R10 |
| `buyer` absent pour 65,5 % des activités, `tokenMint` pour 62,4 %, `seller` pour 34,4 % | champs obligatoires définis par jeu (R2), retirés du contrôle de schéma global |
| 19 069 signatures partagées par plusieurs activités ; 8 lignes strictement identiques | clé des offres = signature + mint ; R7 a retiré 5 offres dupliquées |
| Un enchérisseur réalise 75 % des offres sur okay_bears | KPI de concentration de la demande |

Détail : `reports/profiling_report.md`.
