"""Étape 3 - Transformation : typage, normalisation UTC, enrichissement USD, pseudonymisation.

Sortie alignée sur config/data_contract.yaml (curated_fields).
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from common import get_logger, pseudonymize

log = get_logger("transform")

CURATED_COLUMNS = ["sale_id", "collection_symbol", "token_mint", "sold_at", "sale_date",
                   "price_sol", "sol_usd_rate", "price_usd", "buyer_id", "seller_id",
                   "marketplace_source", "run_id", "source_file", "contract_version"]


BID_COLUMNS = ["bid_id", "collection_symbol", "token_mint", "bid_scope", "bid_channel", "bid_at",
               "bid_date", "price_sol", "sol_usd_rate", "price_usd", "bidder_id", "run_id",
               "source_file", "contract_version"]


def _attach_fx(df: pd.DataFrame, ts_col: str, fx: pd.DataFrame) -> pd.DataFrame:
    """Ajoute sol_usd_rate : cours du jour, ou dernier cours connu (2 jours max) avant l'événement."""
    df = df.copy()
    df["_day"] = df[ts_col].dt.normalize().astype("datetime64[ns, UTC]")
    if fx.empty:
        df["sol_usd_rate"] = None
    else:
        fx_sorted = fx.sort_values("rate_date").copy()
        fx_sorted["rate_date"] = fx_sorted["rate_date"].astype("datetime64[ns, UTC]")
        df = pd.merge_asof(df.sort_values("_day"), fx_sorted, left_on="_day", right_on="rate_date",
                           direction="backward", tolerance=pd.Timedelta(days=2)).drop(columns=["rate_date"])
    df["sol_usd_rate"] = pd.to_numeric(df["sol_usd_rate"], errors="coerce").round(4)
    df["price_usd"] = (df["price_sol"] * df["sol_usd_rate"]).round(2)
    return df.drop(columns=["_day"])


def transform_bids(accepted: pd.DataFrame, fx: pd.DataFrame, salt: str, run_id: str,
                   contract_version: str) -> pd.DataFrame:
    """Offres d'achat : une ligne = une offre posée sur un NFT (token) ou sur toute la collection."""
    if accepted.empty:
        return pd.DataFrame(columns=BID_COLUMNS)
    a = accepted.reset_index(drop=True)
    mint = a["tokenMint"].where(a["tokenMint"].notna() & (a["tokenMint"].astype(str).str.strip() != ""), None)
    df = pd.DataFrame({
        "bid_id": a["signature"].astype(str).str.strip() + ":" + mint.fillna("collection").astype(str),
        "collection_symbol": a["collectionSymbol"].astype(str).str.strip().str.lower(),
        "token_mint": mint,
        # offre sur un NFT précis, ou offre "collection" (pool) valable pour n'importe quel NFT
        "bid_scope": mint.notna().map({True: "token", False: "collection"}),
        # mmm = pools AMM Magic Eden (offres automatisées) ; magiceden_v2 = offre manuelle
        "bid_channel": a["source"].map(lambda v: "pool_amm" if v == "mmm" else "manuelle")
                       if "source" in a.columns else "inconnu",
        "ts": pd.to_datetime(pd.to_numeric(a["blockTime"]), unit="s", utc=True),
        "price_sol": pd.to_numeric(a["price"]).round(6),
        "bidder_id": a["buyer"].apply(lambda v: pseudonymize(v, salt)),
        "source_file": a["_source_file"].values,
    })
    df = _attach_fx(df, "ts", fx)
    df["bid_at"] = df["ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["bid_date"] = df["ts"].dt.strftime("%Y-%m-%d")
    df["run_id"] = run_id
    df["contract_version"] = contract_version
    out = df.sort_values(["bid_at", "bid_id"])[BID_COLUMNS].reset_index(drop=True)
    log.info("%d offres transformées", len(out))
    return out


def load_fx(fx_file: Path | None) -> pd.DataFrame:
    """Cours SOL/USD journalier depuis le raw CoinGecko : colonnes rate_date, sol_usd_rate."""
    if fx_file is None or not Path(fx_file).exists():
        return pd.DataFrame(columns=["rate_date", "sol_usd_rate"])
    payload = json.loads(Path(fx_file).read_text(encoding="utf-8"))
    prices = payload.get("response", {}).get("prices", [])
    fx = pd.DataFrame(prices, columns=["ts_ms", "sol_usd_rate"])
    if fx.empty:
        return pd.DataFrame(columns=["rate_date", "sol_usd_rate"])
    fx["rate_date"] = pd.to_datetime(fx["ts_ms"], unit="ms", utc=True).dt.normalize()
    # plusieurs points le même jour (point "maintenant") : on garde le dernier
    fx = fx.sort_values("ts_ms").groupby("rate_date", as_index=False)["sol_usd_rate"].last()
    return fx[["rate_date", "sol_usd_rate"]]


def transform(accepted: pd.DataFrame, fx: pd.DataFrame, salt: str, run_id: str,
              contract_version: str) -> pd.DataFrame:
    if accepted.empty:
        return pd.DataFrame(columns=CURATED_COLUMNS)
    df = pd.DataFrame()
    df["sale_id"] = accepted["signature"].astype(str).str.strip()
    df["collection_symbol"] = accepted["collectionSymbol"].astype(str).str.strip().str.lower()
    df["token_mint"] = accepted["tokenMint"].astype(str).str.strip()
    sold = pd.to_datetime(pd.to_numeric(accepted["blockTime"]), unit="s", utc=True)
    df["sold_at_ts"] = sold
    df["price_sol"] = pd.to_numeric(accepted["price"]).round(6)
    df["buyer_id"] = accepted["buyer"].apply(lambda v: pseudonymize(v, salt))
    df["seller_id"] = accepted["seller"].apply(lambda v: pseudonymize(v, salt))
    df["marketplace_source"] = accepted["source"] if "source" in accepted.columns else None
    df["source_file"] = accepted["_source_file"].values
    df = df.reset_index(drop=True)

    # Enrichissement : cours du jour, ou dernier cours connu avant la vente (merge_asof)
    df["sale_day"] = df["sold_at_ts"].dt.normalize()
    if not fx.empty:
        fx_sorted = fx.sort_values("rate_date").copy()
        fx_sorted["rate_date"] = fx_sorted["rate_date"].astype("datetime64[ns, UTC]")
        df["sale_day"] = df["sale_day"].astype("datetime64[ns, UTC]")
        df = df.sort_values("sale_day")
        df = pd.merge_asof(df, fx_sorted, left_on="sale_day", right_on="rate_date",
                           direction="backward", tolerance=pd.Timedelta(days=2))
        df = df.drop(columns=["rate_date"])
    else:
        df["sol_usd_rate"] = None
    df["sol_usd_rate"] = pd.to_numeric(df["sol_usd_rate"], errors="coerce").round(4)
    df["price_usd"] = (df["price_sol"] * df["sol_usd_rate"]).round(2)

    df["sold_at"] = df["sold_at_ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["sale_date"] = df["sold_at_ts"].dt.strftime("%Y-%m-%d")
    df["run_id"] = run_id
    df["contract_version"] = contract_version
    out = df.sort_values(["sold_at", "sale_id"])[CURATED_COLUMNS].reset_index(drop=True)
    missing_usd = int(out["price_usd"].isna().sum())
    if missing_usd:
        log.warning("%d vente(s) sans cours SOL/USD (price_usd vide)", missing_usd)
    log.info("%d ventes transformées", len(out))
    return out
