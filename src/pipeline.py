"""Orchestrateur du pipeline (DAG linéaire) :

    collect_magiceden ─┐
    collect_coingecko ─┴─> validate ─> transform ─> publish (SQLite + marts + rapport) ─> run_report

Exemples :
    python src/pipeline.py                         # collecte réelle puis traitement
    python src/pipeline.py --replay                # retraite le dernier lot raw (reprise après panne)
    python src/pipeline.py --raw-file data/samples/sample_source_SYNTHETIC.json \
                           --fx-file data/samples/sample_fx_SYNTHETIC.json   # démo hors ligne
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

import pandas as pd

import collect
import publish
import transform
import validate
from common import (get_salt, load_config, load_yaml, new_run_id, project_path, setup_logging,
                    utc_now)

EXIT_OK, EXIT_FAILED = 0, 1


def latest_raw_batch(raw_dir: Path) -> tuple[list[Path], Path | None]:
    """Fichiers raw du run le plus récent (reprise sans recollecte)."""
    files = sorted(raw_dir.glob("magiceden_*_*.json"))
    if not files:
        return [], None
    run_ids = sorted({re.search(r"_(\d{8}T\d{6}Z)\.json$", f.name).group(1)
                      for f in files if re.search(r"_(\d{8}T\d{6}Z)\.json$", f.name)})
    last = run_ids[-1]
    fx = sorted(raw_dir.glob(f"coingecko_sol_usd_{last}.json"))
    return [f for f in files if f.name.endswith(f"_{last}.json")], (fx[0] if fx else None)


def _rel(p: Path) -> str:
    try:
        return str(Path(p).relative_to(project_path(".")))
    except ValueError:
        return str(p)


def _is_synthetic(paths: list[Path]) -> bool:
    for p in paths:
        try:
            if json.loads(Path(p).read_text(encoding="utf-8")).get("_meta", {}).get("synthetic"):
                return True
        except (OSError, ValueError):
            pass
    return False


def run(args: argparse.Namespace) -> dict:
    cfg = load_config(args.config)
    log = setup_logging(cfg["pipeline"]["log_file"])
    contract = load_yaml(cfg["pipeline"]["contract_path"])
    paths = {k: project_path(v) for k, v in cfg["paths"].items()}
    if args.output_root:   # démo / tests : toutes les sorties dans un dossier séparé
        root = Path(args.output_root).resolve()
        paths = {k: root / Path(v).name for k, v in paths.items()}
        paths["raw_dir"] = project_path(cfg["paths"]["raw_dir"])
    if args.database:
        paths["database"] = Path(args.database).resolve()
    run_id = new_run_id()
    t0 = time.perf_counter()
    mode = "file" if args.raw_file else ("replay" if args.replay else "collect")
    report: dict = {
        "pipeline": cfg["pipeline"]["name"], "run_id": run_id,
        "executed_at": utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"), "mode": mode,
        "contract_version": contract["version"], "status": "RUNNING", "warnings": [],
    }
    salt, demo_salt = get_salt(cfg["privacy"]["salt_env_var"])
    if demo_salt:
        report["warnings"].append("WALLET_HASH_SALT absent : sel de démonstration utilisé")
    log.info("=== Run %s démarré (mode=%s, contrat v%s) ===", run_id, mode, contract["version"])
    conn = None
    try:
        # ---------------------------------------------------------------- 1. collecte
        if mode == "collect":
            conn = publish.connect(paths["database"])
            watermarks = publish.get_watermarks(conn)          # collecte incrémentale
            report["watermarks_used"] = {c: int(t) for c, t in watermarks.items()}
            res = collect.collect_all(cfg, run_id, watermarks)
            sales_files, fx_file = res["sales_files"], res["fx_file"]
            for coll, err in res["errors"].items():
                report["warnings"].append(f"collecte partielle : {coll} en échec ({err})")
        elif mode == "replay":
            sales_files, fx_file = latest_raw_batch(paths["raw_dir"])
            if not sales_files:
                raise collect.SourceUnavailableError("Aucun fichier raw à rejouer dans data/raw")
        else:
            sales_files = [Path(p).resolve() for p in args.raw_file]
            fx_file = Path(args.fx_file).resolve() if args.fx_file else None
        if args.no_fx:
            fx_file = None
        if cfg["coingecko"].get("enabled") and fx_file is None:
            report["warnings"].append("cours SOL/USD indisponible : price_usd vide")
        report["source_files"] = [Path(p).name for p in sales_files] + ([Path(fx_file).name] if fx_file else [])
        report["synthetic_input"] = _is_synthetic(sales_files)

        # ---------------------------------------------------------------- 2. validation
        raw_df = validate.load_raw_sales(sales_files)
        report["raw_activities"] = len(raw_df)
        validate.check_schema(raw_df, contract["raw_required_fields"])            # R0 bloquant
        sales, bids, out_counts = validate.split_activities(
            raw_df, cfg["magiceden"]["sale_types"], cfg["magiceden"].get("bid_types", []))
        report["out_of_scope_activities"] = sum(out_counts.values())
        report["out_of_scope_by_type"] = out_counts
        report["input_rows"] = len(sales) + len(bids)
        if report["input_rows"] == 0:                                             # R1 bloquant
            raise validate.EmptySourceError("Aucune vente ni offre dans les fichiers raw")
        if sales.empty:
            report["warnings"].append("aucune vente dans ce lot (seulement des offres)")

        qcfg = cfg["quality"]
        global_rules = [validate.RuleResult("R0_SCHEMA", "validité", "champs du contrat présents", "bloquer le run"),
                        validate.RuleResult("R1_VOLUME", "complétude", "au moins une vente ou offre", "bloquer le run",
                                            details={"sales": len(sales), "bids": len(bids)}),
                        validate.coverage_rule(raw_df)]
        all_rules, accepted, rejected_parts, datasets = [], {}, [], {}
        for kind, rows in (("sales", sales), ("bids", bids)):
            acc, rej, rules, dups = validate.apply_rules(rows, qcfg, kind=kind) if len(rows) else \
                (rows, rows, [], 0)
            if len(rows):
                rules.append(validate.freshness_rule(acc, qcfg["freshness_max_hours"]))
                rules.append(validate.reject_rate_rule(len(rows), len(rej), qcfg["max_reject_rate"],
                                                       qcfg["warn_reject_rate"]))
            accepted[kind] = acc
            if len(rej):
                rejected_parts.append(rej.assign(dataset=kind))
            datasets[kind] = {"input_rows": len(rows), "accepted_rows": len(acc),
                              "rejected_rows": len(rej), "duplicates_removed": dups}
            all_rules += [{**validate.results_to_dicts([r])[0], "dataset": kind} for r in rules]
        all_rules = [{**validate.results_to_dicts([r])[0], "dataset": "all"} for r in global_rules] + all_rules
        rejected = pd.concat(rejected_parts) if rejected_parts else pd.DataFrame()
        rej_file = validate.write_rejected(rejected, paths["rejected_dir"], run_id, salt)
        report.update({
            "accepted_rows": sum(d["accepted_rows"] for d in datasets.values()),
            "rejected_rows": sum(d["rejected_rows"] for d in datasets.values()),
            "duplicates_removed": sum(d["duplicates_removed"] for d in datasets.values()),
            "datasets": datasets,
            "rejected_file": rej_file.name if rej_file else None,
            "quality_rules": all_rules,
        })
        if any(r["rule_id"] == "R9_REJECT_RATE" and r["status"] == "FAIL" for r in all_rules):
            report["status"] = "FAILED_QUALITY"
            log.error("Taux de rejet trop élevé : publication bloquée")
            return report

        # ---------------------------------------------------------------- 3. transformation
        fx = transform.load_fx(fx_file)
        cur_sales = transform.transform(accepted["sales"], fx, salt, run_id, contract["version"])
        cur_bids = transform.transform_bids(accepted["bids"], fx, salt, run_id, contract["version"])
        report["rows_without_usd"] = int(cur_sales["price_usd"].isna().sum() + cur_bids["price_usd"].isna().sum())
        new_watermarks = (pd.to_numeric(raw_df["blockTime"], errors="coerce")
                          .groupby(raw_df["_query_collection"]).max().dropna().to_dict())

        # ---------------------------------------------------------------- 4. publication
        conn = conn or publish.connect(paths["database"])
        report.update(publish.publish_all(conn, cur_sales, cur_bids, new_watermarks, run_id))
        marts = publish.build_marts(conn)
        report["outputs"] = [_rel(p) for p in
                             publish.export_marts(marts, paths["curated_dir"], paths["marts_dir"])]
        charts = publish.make_charts(marts, paths["reports_dir"])
        html = publish.write_html_report(marts, charts, {**report, "status": "PASS"}, paths["reports_dir"])
        report["outputs"].append(_rel(html))
        report["database_totals"] = {"sales": int(len(marts["sales"])), "bids": int(len(marts["bids"]))}

        warn_rules = sorted({f"{r['rule_id']}({r['dataset']})" for r in all_rules if r["status"] in ("WARN", "FAIL")})
        if warn_rules:
            report["warnings"].append(f"règles en alerte : {warn_rules}")
        report["status"] = "PASS_WITH_WARNINGS" if report["warnings"] else "PASS"
        return report

    except collect.SourceUnavailableError as exc:
        report["status"], report["error"] = "FAILED_SOURCE_UNAVAILABLE", str(exc)
    except validate.SchemaError as exc:
        report["status"], report["error"] = "FAILED_SCHEMA_CHANGE", str(exc)
    except validate.EmptySourceError as exc:
        report["status"], report["error"] = "FAILED_EMPTY_SOURCE", str(exc)
    except Exception as exc:  # noqa: BLE001 - tout échec est tracé dans le rapport
        log.exception("Erreur inattendue")
        report["status"], report["error"] = "FAILED", f"{type(exc).__name__}: {exc}"
    finally:
        report["duration_seconds"] = round(time.perf_counter() - t0, 2)
        _write_report(report, paths["reports_dir"])
        try:
            conn = conn or publish.connect(paths["database"])
            publish.log_run(conn, report)
            conn.close()
        except Exception as exc:  # noqa: BLE001
            log.error("Impossible d'écrire run_log : %s", exc)
        lvl = log.info if report["status"].startswith("PASS") else log.error
        lvl("=== Run %s terminé : %s (%.2fs) %s ===", run_id, report["status"],
            report["duration_seconds"], report.get("error", ""))
    return report


def _write_report(report: dict, reports_dir: Path) -> None:
    runs_dir = reports_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    (runs_dir / f"run_report_{report['run_id']}.json").write_text(text, encoding="utf-8")
    (reports_dir / "run_report.json").write_text(text, encoding="utf-8")   # dernier run


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pipeline de collecte du marché NFT (Magic Eden)")
    p.add_argument("--config", default="config/config.yaml")
    p.add_argument("--replay", action="store_true", help="retraiter le dernier lot raw sans recollecter")
    p.add_argument("--raw-file", action="append", help="traiter un fichier raw donné (répétable)")
    p.add_argument("--fx-file", help="fichier raw CoinGecko à utiliser avec --raw-file")
    p.add_argument("--no-fx", action="store_true", help="ignorer l'enrichissement SOL/USD")
    p.add_argument("--database", help="chemin alternatif de la base SQLite (tests, incidents)")
    p.add_argument("--output-root", help="écrire base, curated, marts, rejets et rapports dans ce dossier (démo)")
    return p.parse_args(argv)


if __name__ == "__main__":
    result = run(parse_args())
    sys.exit(EXIT_OK if result["status"].startswith("PASS") else EXIT_FAILED)
