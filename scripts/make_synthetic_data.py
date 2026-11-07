"""Génère des données SYNTHÉTIQUES au format exact de la source, pour les tests et la démo hors ligne.

Les fichiers produits portent "synthetic": true dans _meta et le suffixe SYNTHETIC : ils ne
doivent jamais être présentés comme des données réelles du marché.

Anomalies injectées volontairement (pour démontrer les règles de qualité) :
  doublons exacts, acheteur manquant, prix négatif / nul / impossible, date future,
  auto-vente (buyer = seller), collection incohérente.

    python scripts/make_synthetic_data.py
"""
from __future__ import annotations

import copy
import json
import random
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "samples"
B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
SEED = 42

COLLECTIONS = {  # symbole : (prix de départ en SOL, dérive quotidienne, ventes/jour)
    "mad_lads": (60.0, 0.006, 14),
    "okay_bears": (12.0, -0.008, 10),
    "claynosaurz": (8.0, 0.002, 12),
    "famous_fox_federation": (5.0, -0.003, 8),
}


def b58(rng: random.Random, n: int) -> str:
    return "".join(rng.choice(B58) for _ in range(n))


def make_activity(rng, symbol, ts, price, buyer, seller, kind="buyNow"):
    return {
        "signature": b58(rng, 88), "type": kind, "source": "magiceden_v2",
        "tokenMint": b58(rng, 44), "collection": symbol, "collectionSymbol": symbol,
        "slot": 250_000_000 + int(ts) % 10_000_000, "blockTime": int(ts),
        "buyer": buyer, "buyerReferral": "", "seller": seller, "sellerReferral": "",
        "price": round(price, 4),
        "priceInfo": {"solPrice": {"rawAmount": str(int(price * 1e9)),
                                   "address": "So11111111111111111111111111111111111111112",
                                   "decimals": 9}},
        "image": f"https://example.invalid/{symbol}.png",
    }


def envelope(symbol, activities, run_id, page_size=500):
    pages = [{"offset": i, "limit": page_size, "response": activities[i:i + page_size]}
             for i in range(0, len(activities), page_size)]
    return {"_meta": {"source": "magiceden", "collection": symbol, "run_id": run_id,
                      "url": f"https://api-mainnet.magiceden.dev/v2/collections/{symbol}/activities",
                      "collected_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
                      "pages_fetched": len(pages), "records": len(activities),
                      "stop_reason": "synthetic", "synthetic": True,
                      "note": "Données synthétiques générées par scripts/make_synthetic_data.py"},
            "pages": pages}


def main() -> None:
    rng = random.Random(SEED)
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "incidents").mkdir(exist_ok=True)
    now = int(time.time()) - 3600
    days = 30
    run_id = "SYNTHETIC"
    # Pool d'acheteurs à distribution inégale : quelques wallets très actifs
    wallets = [b58(rng, 44) for _ in range(400)]
    weights = [1 / (i + 1) ** 0.9 for i in range(len(wallets))]
    bot = b58(rng, 44)  # teneur de marché automatisé (pool AMM)

    for symbol, (p0, drift, per_day) in COLLECTIONS.items():
        acts = []
        for d in range(days, 0, -1):
            level = p0 * (1 + drift) ** (days - d)
            for _ in range(rng.randint(per_day // 2, per_day * 3 // 2)):
                ts = now - d * 86400 + rng.randint(0, 86399)
                price = max(0.05, rng.lognormvariate(0, 0.18) * level)
                buyer = rng.choices(wallets, weights)[0]
                seller = rng.choice(wallets)
                while seller == buyer:
                    seller = rng.choice(wallets)
                acts.append(make_activity(rng, symbol, ts, price, buyer, seller))
                if rng.random() < 0.8:  # activités hors périmètre (annonces, retraits, pools)
                    acts.append(make_activity(rng, symbol, ts - 600, price * 1.1, "", seller,
                                              kind=rng.choice(["list", "delist", "poolUpdate"])))
                for _ in range(rng.randint(3, 8)):  # offres d'achat : beaucoup venant d'un bot de pool
                    pool = rng.random() < 0.7
                    bidder = bot if pool else rng.choices(wallets, weights)[0]
                    bid = make_activity(rng, symbol, ts - rng.randint(0, 3600), price * rng.uniform(0.7, 0.98),
                                        bidder, "", kind="bid")
                    bid["seller"] = None
                    if pool:
                        bid["source"] = "mmm"
                        del bid["tokenMint"]
                    acts.append(bid)
        sales = [a for a in acts if a["type"] == "buyNow"]
        bids = [a for a in acts if a["type"] == "bid"]
        acts += [copy.deepcopy(rng.choice(bids)) for _ in range(2)]                  # R7 doublons d'offres
        # --- anomalies injectées
        acts += [copy.deepcopy(rng.choice(sales)) for _ in range(3)]                 # R7 doublons
        bad = copy.deepcopy(rng.choice(sales)); bad["signature"] = b58(rng, 88); bad["buyer"] = None
        acts.append(bad)                                                              # R2
        bad = copy.deepcopy(rng.choice(sales)); bad["signature"] = b58(rng, 88); bad["price"] = -3.5
        acts.append(bad)                                                              # R4
        bad = copy.deepcopy(rng.choice(sales)); bad["signature"] = b58(rng, 88); bad["blockTime"] = now + 86400 * 5
        acts.append(bad)                                                              # R3
        bad = copy.deepcopy(rng.choice(sales)); bad["signature"] = b58(rng, 88); bad["seller"] = bad["buyer"]
        acts.append(bad)                                                              # R5
        if symbol == "okay_bears":
            bad = copy.deepcopy(rng.choice(sales)); bad["signature"] = b58(rng, 88); bad["price"] = 2_500_000
            acts.append(bad)                                                          # R4 valeur impossible
            bad = copy.deepcopy(rng.choice(sales)); bad["signature"] = b58(rng, 88)
            bad["collectionSymbol"] = "unknown_collection"
            acts.append(bad)                                                          # R6
        acts.sort(key=lambda a: a["blockTime"], reverse=True)  # l'API renvoie du plus récent au plus ancien
        path = OUT / f"magiceden_{symbol}_SYNTHETIC.json"
        path.write_text(json.dumps(envelope(symbol, acts, run_id), indent=1), encoding="utf-8")
        print(f"{path.relative_to(ROOT)} : {len(acts)} activités")

    # Cours SOL/USD synthétique au format CoinGecko market_chart
    prices, level = [], 150.0
    for d in range(90, -1, -1):
        level *= 1 + rng.uniform(-0.03, 0.03)
        prices.append([(now - d * 86400) * 1000, round(level, 2)])
    fx = {"_meta": {"source": "coingecko", "synthetic": True, "run_id": run_id},
          "response": {"prices": prices, "market_caps": [], "total_volumes": []}}
    (OUT / "coingecko_sol_usd_SYNTHETIC.json").write_text(json.dumps(fx), encoding="utf-8")

    # ---- fichiers d'incident
    base = json.loads((OUT / "magiceden_okay_bears_SYNTHETIC.json").read_text())
    schema = copy.deepcopy(base)                       # le champ price devient priceSol
    for page in schema["pages"]:
        for a in page["response"]:
            a["priceSol"] = a.pop("price")
    (OUT / "incidents" / "incident_schema_change.json").write_text(json.dumps(schema))
    empty = copy.deepcopy(base); empty["pages"] = [{"offset": 0, "limit": 500, "response": []}]
    (OUT / "incidents" / "incident_empty_source.json").write_text(json.dumps(empty))
    dup = copy.deepcopy(base)                          # chaque page servie 3 fois
    dup["pages"] = dup["pages"] * 3
    (OUT / "incidents" / "incident_massive_duplicates.json").write_text(json.dumps(dup))
    impossible = copy.deepcopy(base)                   # 40 % des prix à zéro : bug source
    for page in impossible["pages"]:
        for i, a in enumerate(page["response"]):
            if a["type"] == "buyNow" and i % 5 < 2:
                a["price"] = 0
    (OUT / "incidents" / "incident_impossible_values.json").write_text(json.dumps(impossible))
    print("Fichiers d'incident générés dans data/samples/incidents/")


if __name__ == "__main__":
    main()
