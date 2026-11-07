"""Profilage du raw (avant nettoyage) -> reports/profiling_report.json et .md

    python scripts/profile_raw.py                  # dernier lot de data/raw
    python scripts/profile_raw.py fichier1.json …  # fichiers donnés
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
from pipeline import latest_raw_batch  # noqa: E402
from validate import load_raw_sales  # noqa: E402


def profile(files: list[Path]) -> dict:
    df = load_raw_sales(files)
    ts = pd.to_datetime(pd.to_numeric(df["blockTime"], errors="coerce"), unit="s", utc=True)
    fields = [c for c in df.columns if not c.startswith("_")]
    out = {
        "files": [Path(f).name for f in files],
        "rows": len(df), "columns": len(fields),
        "dtypes": {c: str(df[c].dtype) for c in fields},
        "missing_rate": {c: round(float(df[c].isna().mean()), 4) for c in fields},
        "duplicated_signatures": int(df["signature"].duplicated().sum()),
        "exact_duplicate_rows": int(df.drop(columns=["priceInfo"], errors="ignore").astype(str).duplicated().sum()),
        "type_counts": df["type"].value_counts().to_dict(),
        "source_counts": df["source"].value_counts().to_dict() if "source" in df else {},
        "by_collection": {},
    }
    for coll, g in df.groupby("_query_collection"):
        t = ts.loc[g.index]
        out["by_collection"][coll] = {
            "rows": len(g), "first": t.min().isoformat(), "last": t.max().isoformat(),
            "hours_covered": round((t.max() - t.min()).total_seconds() / 3600, 1),
            "type_counts": g["type"].value_counts().to_dict(),
            "price_by_type": {k: {"min": float(v.min()), "median": float(v.median()), "max": float(v.max())}
                              for k, v in pd.to_numeric(g["price"], errors="coerce").groupby(g["type"])},
            "distinct_buyers": int(g["buyer"].nunique()) if "buyer" in g else 0,
        }
    return out


def to_markdown(p: dict) -> str:
    lines = ["# Rapport de profilage du raw", "", f"Fichiers : {', '.join(p['files'])}", "",
             f"- {p['rows']} activités, {p['columns']} champs",
             f"- signatures dupliquées : {p['duplicated_signatures']} ; lignes strictement identiques : {p['exact_duplicate_rows']}",
             "", "## Types d'activité", "", "| type | nombre | part |", "|---|---|---|"]
    lines += [f"| {k} | {v} | {v / p['rows']:.1%} |" for k, v in p["type_counts"].items()]
    lines += ["", "## Taux de valeurs manquantes", "", "| champ | manquant |", "|---|---|"]
    lines += [f"| {k} | {v:.1%} |" for k, v in sorted(p["missing_rate"].items(), key=lambda x: -x[1])]
    lines += ["", "## Couverture par collection", "", "| collection | activités | début | fin | heures | ventes | offres |",
              "|---|---|---|---|---|---|---|"]
    for c, d in p["by_collection"].items():
        lines.append(f"| {c} | {d['rows']} | {d['first'][:16]} | {d['last'][:16]} | {d['hours_covered']} | "
                     f"{d['type_counts'].get('buyNow', 0)} | {d['type_counts'].get('bid', 0)} |")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    files = [Path(a) for a in sys.argv[1:]] or latest_raw_batch(ROOT / "data" / "raw")[0]
    if not files:
        sys.exit("Aucun fichier raw trouvé")
    prof = profile(files)
    (ROOT / "reports" / "profiling_report.json").write_text(json.dumps(prof, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    md = to_markdown(prof)
    (ROOT / "reports" / "profiling_report.md").write_text(md, encoding="utf-8")
    print(md)
