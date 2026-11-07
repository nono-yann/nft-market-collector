"""Tests des règles de qualité, de l'idempotence et des incidents."""
import sqlite3
import time

import pandas as pd
import pytest

import pipeline
import validate
from conftest import SAMPLES, synthetic_files

QCFG = {"max_price_sol": 100000, "min_block_time": "2021-03-01",
        "future_tolerance_minutes": 10, "freshness_max_hours": 72}
NOW = int(time.time())


def sale(**over):
    row = {"signature": "sig1", "type": "buyNow", "collectionSymbol": "okay_bears",
           "_query_collection": "okay_bears", "tokenMint": "mint1", "blockTime": NOW - 3600,
           "buyer": "walletA", "seller": "walletB", "price": 12.5, "_source_file": "f.json"}
    row.update(over)
    return row


def run_rules(rows):
    acc, rej, rules, dups = validate.apply_rules(pd.DataFrame(rows), QCFG)
    return acc, rej, {r.rule_id: r for r in rules}, dups


# ------------------------------------------------------------------ règles ligne à ligne
def test_valid_row_is_accepted():
    acc, rej, rules, _ = run_rules([sale()])
    assert len(acc) == 1 and rej.empty
    assert all(r.status == "PASS" for r in rules.values())


def test_r2_missing_buyer_rejected():
    acc, rej, rules, _ = run_rules([sale(), sale(signature="s2", buyer=None)])
    assert len(acc) == 1 and rules["R2_REQUIRED"].failures == 1
    assert rej.iloc[0]["reject_reasons"] == "R2_REQUIRED"


def test_r3_future_and_too_old_timestamp_rejected():
    _, rej, rules, _ = run_rules([sale(signature="a", blockTime=NOW + 86400),
                                  sale(signature="b", blockTime=1_500_000_000)])
    assert rules["R3_TIMESTAMP"].failures == 2 and len(rej) == 2


@pytest.mark.parametrize("price", [0, -1, 2_000_000, "abc"])
def test_r4_price_out_of_range_rejected(price):
    acc, _, rules, _ = run_rules([sale(price=price)])
    assert acc.empty and rules["R4_PRICE_RANGE"].failures == 1


def test_r5_self_trade_rejected():
    acc, _, rules, _ = run_rules([sale(seller="walletA")])
    assert acc.empty and rules["R5_SELF_TRADE"].failures == 1


def test_r6_wrong_collection_rejected():
    acc, _, rules, _ = run_rules([sale(collectionSymbol="other")])
    assert acc.empty and rules["R6_COLLECTION"].failures == 1


def test_r7_duplicates_removed_and_counted():
    acc, rej, rules, dups = run_rules([sale(), sale(), sale(), sale(signature="s2")])
    assert len(acc) == 2 and dups == 2 and rej.empty
    assert rules["R7_UNIQUE"].status == "WARN"


def test_multiple_reasons_are_all_recorded():
    _, rej, _, _ = run_rules([sale(price=-5, seller="walletA")])
    assert set(rej.iloc[0]["reject_reasons"].split(";")) == {"R4_PRICE_RANGE", "R5_SELF_TRADE"}


def test_r8_freshness_warns_on_old_data():
    old = pd.DataFrame([sale(blockTime=NOW - 10 * 86400)])
    assert validate.freshness_rule(old, 72).status == "WARN"
    assert validate.freshness_rule(pd.DataFrame([sale()]), 72).status == "PASS"


def test_r9_reject_rate():
    assert validate.reject_rate_rule(100, 30, 0.2, 0.02).status == "FAIL"
    assert validate.reject_rate_rule(100, 5, 0.2, 0.02).status == "WARN"
    assert validate.reject_rate_rule(100, 1, 0.2, 0.02).status == "PASS"


def test_r0_schema_change_detected():
    df = pd.DataFrame([{k: v for k, v in sale().items() if k != "price"} | {"priceSol": 1}])
    with pytest.raises(validate.SchemaError):
        validate.check_schema(df, ["signature", "price"])


# ------------------------------------------------------------------ pipeline complet
def _run(cfg, files, fx=True):
    argv = ["--config", str(cfg)] + sum([["--raw-file", f] for f in files], [])
    if fx:
        argv += ["--fx-file", str(SAMPLES / "coingecko_sol_usd_SYNTHETIC.json")]
    return pipeline.run(pipeline.parse_args(argv))


def test_pipeline_end_to_end_and_idempotent(tmp_config, tmp_path):
    first = _run(tmp_config, synthetic_files())
    assert first["status"].startswith("PASS")
    assert first["inserted_rows"] == first["accepted_rows"] > 1000
    second = _run(tmp_config, synthetic_files())
    assert second["inserted_rows"] == 0                      # rejeu : aucune duplication
    assert second["already_loaded_rows"] == first["accepted_rows"]
    with sqlite3.connect(tmp_path / "test.db") as conn:
        n, n_distinct = conn.execute("SELECT COUNT(*), COUNT(DISTINCT sale_id) FROM fact_sale").fetchone()
        assert n == n_distinct == first["datasets"]["sales"]["accepted_rows"]
        nb, nb_distinct = conn.execute("SELECT COUNT(*), COUNT(DISTINCT bid_id) FROM fact_bid").fetchone()
        assert nb == nb_distinct == first["datasets"]["bids"]["accepted_rows"] > 1000
        # aucune adresse de wallet en clair dans la base (pseudonymes de 16 caractères)
        assert conn.execute("SELECT MAX(LENGTH(buyer_id)) FROM fact_sale").fetchone()[0] == 16
        assert conn.execute("SELECT MAX(LENGTH(bidder_id)) FROM fact_bid").fetchone()[0] == 16
        # le filigrane de collecte incrémentale est enregistré pour chaque collection
        assert conn.execute("SELECT COUNT(*) FROM collection_watermark").fetchone()[0] == 4
    assert (tmp_path / "rejected_dir").exists() and any((tmp_path / "rejected_dir").iterdir())


@pytest.mark.parametrize("incident, expected", [
    ("incident_schema_change.json", "FAILED_SCHEMA_CHANGE"),
    ("incident_empty_source.json", "FAILED_EMPTY_SOURCE"),
    ("incident_impossible_values.json", "FAILED_QUALITY"),
])
def test_incidents_are_detected_and_block_publication(tmp_config, tmp_path, incident, expected):
    report = _run(tmp_config, [str(SAMPLES / "incidents" / incident)])
    assert report["status"] == expected
    assert not (tmp_path / "curated_dir" / "sales_curated.csv").exists()   # rien de publié
    assert (tmp_path / "reports_dir" / "run_report.json").exists()         # mais preuve conservée


def test_massive_duplicates_are_neutralised(tmp_config):
    report = _run(tmp_config, [str(SAMPLES / "incidents" / "incident_massive_duplicates.json")])
    assert report["status"].startswith("PASS")
    assert report["duplicates_removed"] >= 2 * report["accepted_rows"] - 10


def test_source_unavailable_is_reported(tmp_config, tmp_path, monkeypatch):
    import yaml
    cfg = yaml.safe_load(tmp_config.read_text(encoding="utf-8"))
    cfg["magiceden"]["base_url"] = "http://127.0.0.1:9"     # port fermé : source injoignable
    cfg["magiceden"]["max_retries"] = 1
    cfg["coingecko"]["enabled"] = False
    tmp_config.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    monkeypatch.setattr("collect.time.sleep", lambda s: None)
    report = pipeline.run(pipeline.parse_args(["--config", str(tmp_config)]))
    assert report["status"] == "FAILED_SOURCE_UNAVAILABLE"


# ------------------------------------------------------------------ offres (bids)
def bid(**over):
    row = {"signature": "b1", "type": "bid", "collectionSymbol": "okay_bears",
           "_query_collection": "okay_bears", "tokenMint": None, "blockTime": NOW - 600,
           "buyer": "bidderA", "seller": None, "price": 1.9, "source": "mmm", "_source_file": "f.json"}
    row.update(over)
    return row


def test_bid_without_seller_or_mint_is_valid():
    acc, rej, rules, _ = validate.apply_rules(pd.DataFrame([bid()]), QCFG, kind="bids")
    assert len(acc) == 1 and rej.empty
    assert "R5_SELF_TRADE" not in {r.rule_id for r in rules}


def test_bid_key_is_signature_plus_mint():
    # une transaction peut poser des offres sur plusieurs NFT : même signature, mints différents
    rows = [bid(tokenMint="m1"), bid(tokenMint="m2"), bid(tokenMint="m2"), bid(signature="b2")]
    acc, _, _, dups = validate.apply_rules(pd.DataFrame(rows), QCFG, kind="bids")
    assert len(acc) == 3 and dups == 1


def test_bid_without_bidder_rejected():
    _, rej, rules, _ = validate.apply_rules(pd.DataFrame([bid(buyer="")]), QCFG, kind="bids")
    assert len(rej) == 1 and {r.rule_id: r for r in rules}["R2_REQUIRED"].failures == 1


def test_coverage_rule_flags_gap_when_page_cap_hit_before_watermark():
    df = pd.DataFrame([{"_query_collection": "c", "blockTime": NOW - 3600, "_stop_reason": "max_pages",
                        "_watermark_ts": NOW - 86400}])
    r = validate.coverage_rule(df)
    assert r.status == "WARN" and "gap" in r.details["c"]
    df["_stop_reason"] = "watermark_reached"
    assert validate.coverage_rule(df).status == "PASS"
