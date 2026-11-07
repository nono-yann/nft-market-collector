"""Étape 2 - Validation : lecture du raw, contrôle de schéma, règles de qualité, rejets.

Chaque règle a un identifiant, une dimension, une condition, un compteur d'échecs et une action.
Les lignes rejetées ne sont jamais supprimées silencieusement : elles sont écrites dans
data/rejected/ avec la (ou les) cause(s) de rejet.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from common import get_logger, pseudonymize, utc_now

log = get_logger("validate")


class SchemaError(RuntimeError):
    """Le schéma de la source ne respecte plus le contrat (incident bloquant)."""


class EmptySourceError(RuntimeError):
    """La source n'a fourni aucune vente exploitable (incident bloquant)."""


@dataclass
class RuleResult:
    rule_id: str
    dimension: str
    condition: str
    action: str
    failures: int = 0
    status: str = "PASS"          # PASS / WARN / FAIL
    details: dict = field(default_factory=dict)


# --------------------------------------------------------------------------- lecture du raw

def load_raw_sales(paths: list[Path]) -> pd.DataFrame:
    """Aplatit les pages brutes Magic Eden en DataFrame (1 ligne = 1 activité brute)."""
    rows = []
    for path in paths:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        meta = payload.get("_meta", {})
        for page in payload.get("pages", []):
            data = page.get("response")
            if not isinstance(data, list):
                continue
            for act in data:
                if isinstance(act, dict):
                    row = dict(act)
                    row["_source_file"] = Path(path).name
                    row["_query_collection"] = meta.get("collection")
                    row["_stop_reason"] = meta.get("stop_reason")
                    row["_watermark_ts"] = meta.get("watermark_ts")
                    rows.append(row)
    df = pd.DataFrame(rows)
    log.info("%d activités brutes lues depuis %d fichier(s)", len(df), len(paths))
    return df


def check_schema(df: pd.DataFrame, required_fields: list[str]) -> list[str]:
    """R0 : les champs du contrat doivent exister. Lève SchemaError sinon."""
    if df.empty:
        return []
    missing = [f for f in required_fields if f not in df.columns]
    if missing:
        raise SchemaError(f"Champs attendus absents de la source : {missing}. "
                          f"Champs reçus : {sorted(c for c in df.columns if not c.startswith('_'))}")
    return missing


# Deux jeux de données issus du même flux d'activités :
#   - ventes (buyNow)   : grain = une vente,  clé métier = signature
#   - offres (bid)      : grain = une offre,  clé métier = signature + tokenMint (une transaction
#                         peut poser plusieurs offres sur des NFT différents)
SPECS = {
    "sales": {"required": ["signature", "collectionSymbol", "tokenMint", "blockTime", "buyer", "seller", "price"],
              "key": ["signature"], "self_trade": True},
    "bids": {"required": ["signature", "collectionSymbol", "blockTime", "buyer", "price"],
             "key": ["signature", "tokenMint"], "self_trade": False},
}


def filter_types(df: pd.DataFrame, types: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    return df.loc[df["type"].isin(types)].copy()


def split_activities(df: pd.DataFrame, sale_types: list[str], bid_types: list[str]
                     ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Sépare ventes et offres. Les autres activités (list, delist, poolUpdate, cancelBid...) sont
    hors périmètre : elles restent dans le raw, comptées par type, ni acceptées ni rejetées."""
    if df.empty:
        return df, df, {}
    in_scope = df["type"].isin(sale_types + bid_types)
    out_counts = df.loc[~in_scope, "type"].value_counts().to_dict()
    return filter_types(df, sale_types), filter_types(df, bid_types), {k: int(v) for k, v in out_counts.items()}


def filter_sales(df: pd.DataFrame, sale_types: list[str]) -> tuple[pd.DataFrame, int]:
    """Compatibilité : ventes seules + nombre d'activités hors ventes."""
    if df.empty:
        return df, 0
    sales = filter_types(df, sale_types)
    return sales, len(df) - len(sales)


# --------------------------------------------------------------------------- règles

def _is_blank(series: pd.Series) -> pd.Series:
    return series.isna() | series.astype(str).str.strip().isin(["", "None", "nan"])


def apply_rules(rows: pd.DataFrame, qcfg: dict, now: datetime | None = None, kind: str = "sales"
                ) -> tuple[pd.DataFrame, pd.DataFrame, list[RuleResult], int]:
    """Applique R2 à R7 au jeu `kind` (sales ou bids).
    Retourne (acceptées, rejetées, résultats des règles, doublons retirés)."""
    now = now or utc_now()
    spec = SPECS[kind]
    df = rows.copy().reset_index(drop=True)
    if "tokenMint" not in df.columns:
        df["tokenMint"] = None
    reasons = pd.Series([[] for _ in range(len(df))], index=df.index, dtype=object)
    results: list[RuleResult] = []

    def flag(mask: pd.Series, rule: RuleResult) -> None:
        rule.failures = int(mask.sum())
        # lignes fautives isolées dans les rejets : alerte, pas d'échec du run
        rule.status = "PASS" if rule.failures == 0 else "WARN"
        for i in df.index[mask]:
            reasons.at[i] = reasons.at[i] + [rule.rule_id]
        results.append(rule)

    # R2 complétude
    required = spec["required"]
    blank = pd.Series(False, index=df.index)
    per_field = {}
    for col in required:
        m = _is_blank(df[col]) if col in df.columns else pd.Series(True, index=df.index)
        per_field[col] = int(m.sum())
        blank |= m
    r2 = RuleResult("R2_REQUIRED", "complétude", "champs obligatoires non vides", "rejeter",
                    details={"par_champ": per_field})
    flag(blank, r2)

    # R3 validité temporelle
    bt = pd.to_numeric(df["blockTime"], errors="coerce")
    min_ts = pd.Timestamp(qcfg["min_block_time"], tz="UTC").timestamp()
    max_ts = now.timestamp() + qcfg["future_tolerance_minutes"] * 60
    bad_time = bt.isna() | (bt < min_ts) | (bt > max_ts) | (bt % 1 != 0)
    flag(bad_time & ~blank, RuleResult("R3_TIMESTAMP", "validité",
         f"blockTime entier entre {qcfg['min_block_time']} et maintenant+{qcfg['future_tolerance_minutes']}min",
         "rejeter"))

    # R4 domaine du prix
    price = pd.to_numeric(df["price"], errors="coerce")
    bad_price = price.isna() | (price <= 0) | (price > qcfg["max_price_sol"])
    flag(bad_price & ~blank, RuleResult("R4_PRICE_RANGE", "validité",
         f"0 < price <= {qcfg['max_price_sol']} SOL", "rejeter"))

    # R5 cohérence acheteur / vendeur (ventes uniquement : une offre n'a pas encore de vendeur)
    if spec["self_trade"]:
        self_trade = (df["buyer"].astype(str) == df["seller"].astype(str)) & ~blank
        flag(self_trade, RuleResult("R5_SELF_TRADE", "cohérence", "buyer != seller",
                                    "rejeter (auto-vente)"))

    # R6 cohérence collection retournée / collection demandée
    wrong_coll = (df["collectionSymbol"].astype(str) != df["_query_collection"].astype(str)) & ~blank
    flag(wrong_coll, RuleResult("R6_COLLECTION", "cohérence",
                                "collectionSymbol = collection interrogée", "rejeter"))

    df["reject_reasons"] = reasons.apply(lambda r: ";".join(r))
    rejected_mask = df["reject_reasons"] != ""
    rejected = df.loc[rejected_mask].copy()
    accepted = df.loc[~rejected_mask].drop(columns=["reject_reasons"]).copy()

    # R7 unicité : déduplication par clé métier dans le lot (première occurrence conservée)
    before = len(accepted)
    key = (accepted[spec["key"]].astype(object).where(accepted[spec["key"]].notna(), "")
           .astype(str).agg(":".join, axis=1))
    accepted = accepted.loc[~key.duplicated(keep="first")]
    dups = before - len(accepted)
    results.append(RuleResult("R7_UNIQUE", "unicité", f"clé métier {'+'.join(spec['key'])} unique dans le lot",
                              "dédupliquer et compter", failures=dups,
                              status="PASS" if dups == 0 else "WARN"))
    return accepted, rejected, results, dups


def freshness_rule(accepted: pd.DataFrame, max_hours: float, now: datetime | None = None) -> RuleResult:
    """R8 : âge de l'observation la plus récente (vente ou offre)."""
    now = now or utc_now()
    rule = RuleResult("R8_FRESHNESS", "fraîcheur", f"observation la plus récente < {max_hours} h", "alerte")
    if accepted.empty:
        rule.status, rule.failures = "WARN", 1
        return rule
    newest = datetime.fromtimestamp(float(pd.to_numeric(accepted["blockTime"]).max()), tz=timezone.utc)
    age_h = round((now - newest).total_seconds() / 3600, 2)
    rule.details = {"newest_at": newest.isoformat(), "age_hours": age_h}
    if age_h > max_hours:
        rule.status, rule.failures = "WARN", 1
    return rule


def coverage_rule(raw_df: pd.DataFrame) -> RuleResult:
    """R10 : complétude de la fenêtre collectée.

    La pagination est plafonnée (max_pages). Si le plafond est atteint AVANT d'avoir rejoint le
    filigrane (dernière activité déjà chargée), il existe un trou entre le filigrane et l'activité
    la plus ancienne reçue : on le signale au lieu de l'ignorer."""
    rule = RuleResult("R10_COVERAGE", "complétude", "fenêtre collectée jointive avec le run précédent",
                      "alerte (trou de collecte daté)")
    if raw_df.empty:
        return rule
    details, gaps = {}, 0
    for coll, g in raw_df.groupby("_query_collection"):
        bt = pd.to_numeric(g["blockTime"], errors="coerce")
        oldest, newest = bt.min(), bt.max()
        stop = g["_stop_reason"].iloc[0] if "_stop_reason" in g else None
        wm = g["_watermark_ts"].iloc[0] if "_watermark_ts" in g else None
        wm = None if wm is None or pd.isna(wm) else float(wm)
        to_iso = lambda t: (datetime.fromtimestamp(float(t), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                            if pd.notna(t) else None)
        info = {"oldest_activity": to_iso(oldest), "newest_activity": to_iso(newest),
                "hours_covered": round((newest - oldest) / 3600, 1) if pd.notna(oldest) else 0,
                "stop_reason": stop, "watermark": to_iso(wm) if wm else None}
        if stop == "max_pages" and wm is not None and oldest > wm:
            info["gap"] = f"{to_iso(wm)} -> {to_iso(oldest)}"
            gaps += 1
        details[coll] = info
    rule.details = details
    if gaps:
        rule.status, rule.failures = "WARN", gaps
    return rule


def reject_rate_rule(n_input: int, n_rejected: int, max_rate: float, warn_rate: float) -> RuleResult:
    """R9 : taux de rejet global. Au-delà de max_rate, la publication est bloquée."""
    rate = n_rejected / n_input if n_input else 0.0
    rule = RuleResult("R9_REJECT_RATE", "fiabilité", f"taux de rejet <= {max_rate:.0%}",
                      "bloquer la publication", details={"reject_rate": round(rate, 4)})
    if rate > max_rate:
        rule.status, rule.failures = "FAIL", 1
    elif rate > warn_rate:
        rule.status = "WARN"
    return rule


def write_rejected(rejected: pd.DataFrame, rejected_dir: Path, run_id: str, salt: str) -> Path | None:
    """`rejected` peut contenir ventes et offres (colonne dataset)."""
    if rejected.empty:
        return None
    rejected_dir.mkdir(parents=True, exist_ok=True)
    out = rejected_dir / f"rejected_{run_id}.csv"
    cols = ["dataset", "reject_reasons", "_source_file", "_query_collection", "signature", "type",
            "collectionSymbol", "tokenMint", "blockTime", "price", "buyer", "seller"]
    rej = rejected[[c for c in cols if c in rejected.columns]].copy()
    # Minimisation : les wallets sont pseudonymisés aussi dans la zone des rejets
    for col in ("buyer", "seller"):
        if col in rej.columns:
            rej[col] = rej[col].apply(lambda v: pseudonymize(v, salt))
    rej.insert(0, "run_id", run_id)
    rej.to_csv(out, index=False)
    log.warning("%d ligne(s) rejetée(s) -> %s", len(rej), out.name)
    return out


def results_to_dicts(results: list[RuleResult]) -> list[dict]:
    return [asdict(r) for r in results]
