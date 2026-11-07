"""Test d'incident : rejoue chaque scénario dans un espace isolé et produit reports/incident_report.json.

Scénarios : source indisponible, changement de schéma, source vide, doublons massifs,
valeurs impossibles. La base et les sorties principales du projet ne sont pas touchées.

    python scripts/incident_demo.py
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
import pipeline  # noqa: E402

SAMPLES = ROOT / "data" / "samples"
WORK = ROOT / "reports" / "incidents"

SCENARIOS = {
    "source_indisponible": {"base_url": "http://127.0.0.1:9", "files": None},
    "changement_schema": {"files": ["incident_schema_change.json"]},
    "source_vide": {"files": ["incident_empty_source.json"]},
    "doublons_massifs": {"files": ["incident_massive_duplicates.json"]},
    "valeurs_impossibles": {"files": ["incident_impossible_values.json"]},
}


def main() -> None:
    shutil.rmtree(WORK, ignore_errors=True)
    base_cfg = yaml.safe_load((ROOT / "config" / "config.yaml").read_text(encoding="utf-8"))
    summary = []
    for name, sc in SCENARIOS.items():
        d = WORK / name
        cfg = json.loads(json.dumps(base_cfg))
        cfg["paths"] = {k: str(d / Path(v).name) for k, v in cfg["paths"].items()}
        cfg["pipeline"]["log_file"] = str(d / "pipeline.log")
        cfg["pipeline"]["contract_path"] = str(ROOT / "config" / "data_contract.yaml")
        if sc.get("base_url"):
            cfg["magiceden"]["base_url"] = sc["base_url"]
            cfg["magiceden"]["max_retries"] = 2
            cfg["coingecko"]["enabled"] = False
        d.mkdir(parents=True, exist_ok=True)
        (d / "config.yaml").write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
        argv = ["--config", str(d / "config.yaml")]
        for f in sc["files"] or []:
            argv += ["--raw-file", str(SAMPLES / "incidents" / f)]
        print(f"\n##### Scénario : {name}")
        rep = pipeline.run(pipeline.parse_args(argv))
        summary.append({"scenario": name, "status": rep["status"], "error": rep.get("error"),
                        "input_rows": rep.get("input_rows"), "accepted_rows": rep.get("accepted_rows"),
                        "rejected_rows": rep.get("rejected_rows"),
                        "duplicates_removed": rep.get("duplicates_removed"),
                        "published": bool(rep.get("outputs")),
                        "evidence": f"reports/incidents/{name}/"})
    out = ROOT / "reports" / "incident_report.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\nRésumé :")
    for s in summary:
        print(f"  {s['scenario']:<22} -> {s['status']:<28} publié={s['published']}")
    print(f"\n-> {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
