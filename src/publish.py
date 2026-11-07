"""Étape 4 - Publication : base SQLite (curated), data marts, export CSV et rapport HTML.

Idempotence : les clés primaires (sale_id, bid_id) + INSERT ... ON CONFLICT garantissent qu'un
rejeu du même raw n'ajoute aucune ligne. Ventes, offres et filigranes sont chargés dans UNE
transaction (tout ou rien). Les exports sont régénérés intégralement depuis la base.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from common import get_logger, utc_now

log = get_logger("publish")

# Palette catégorielle validée (ordre fixe, la couleur suit la collection)
SERIES_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e8e7e2", "#fcfcfb"

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS dim_collection (
    collection_symbol TEXT PRIMARY KEY,
    first_seen_run    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sale (
    sale_id            TEXT PRIMARY KEY,                -- clé métier : signature Solana
    collection_symbol  TEXT NOT NULL REFERENCES dim_collection(collection_symbol) ON DELETE RESTRICT,
    token_mint         TEXT NOT NULL,
    sold_at            TEXT NOT NULL,                   -- ISO 8601 UTC
    sale_date          TEXT NOT NULL,
    price_sol          REAL NOT NULL CHECK (price_sol > 0),
    sol_usd_rate       REAL CHECK (sol_usd_rate IS NULL OR sol_usd_rate > 0),
    price_usd          REAL,
    buyer_id           TEXT NOT NULL,
    seller_id          TEXT NOT NULL,
    marketplace_source TEXT,
    run_id             TEXT NOT NULL,                   -- run de première ingestion (lineage)
    source_file        TEXT NOT NULL,
    contract_version   TEXT NOT NULL,
    loaded_at          TEXT NOT NULL,
    CHECK (buyer_id <> seller_id)
);
CREATE INDEX IF NOT EXISTS idx_sale_collection_date ON fact_sale(collection_symbol, sale_date);
CREATE INDEX IF NOT EXISTS idx_sale_buyer ON fact_sale(buyer_id);

CREATE TABLE IF NOT EXISTS fact_bid (
    bid_id             TEXT PRIMARY KEY,                -- signature + ':' + tokenMint (ou 'collection')
    collection_symbol  TEXT NOT NULL REFERENCES dim_collection(collection_symbol) ON DELETE RESTRICT,
    token_mint         TEXT,                            -- vide = offre sur toute la collection
    bid_scope          TEXT NOT NULL CHECK (bid_scope IN ('token', 'collection')),
    bid_channel        TEXT NOT NULL CHECK (bid_channel IN ('pool_amm', 'manuelle', 'inconnu')),
    bid_at             TEXT NOT NULL,
    bid_date           TEXT NOT NULL,
    price_sol          REAL NOT NULL CHECK (price_sol > 0),
    sol_usd_rate       REAL CHECK (sol_usd_rate IS NULL OR sol_usd_rate > 0),
    price_usd          REAL,
    bidder_id          TEXT NOT NULL,
    run_id             TEXT NOT NULL,
    source_file        TEXT NOT NULL,
    contract_version   TEXT NOT NULL,
    loaded_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bid_collection_date ON fact_bid(collection_symbol, bid_date);
CREATE INDEX IF NOT EXISTS idx_bid_bidder ON fact_bid(bidder_id);

-- Filigrane de collecte incrémentale : dernière activité brute vue par collection
CREATE TABLE IF NOT EXISTS collection_watermark (
    collection_symbol  TEXT PRIMARY KEY,
    last_activity_ts   INTEGER NOT NULL,
    updated_by_run     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id             TEXT PRIMARY KEY,
    executed_at        TEXT NOT NULL,
    mode               TEXT NOT NULL,
    status             TEXT NOT NULL,
    input_rows         INTEGER,
    accepted_rows      INTEGER,
    rejected_rows      INTEGER,
    duplicates_removed INTEGER,
    inserted_rows      INTEGER,
    duration_seconds   REAL
);

-- Les vues sont recréées à chaque connexion : leur définition suit la version du code
DROP VIEW IF EXISTS mart_daily_collection;
DROP VIEW IF EXISTS mart_daily_sales;
DROP VIEW IF EXISTS mart_daily_bids;
DROP VIEW IF EXISTS mart_buyer_activity;
DROP VIEW IF EXISTS mart_bidder_activity;

CREATE VIEW mart_daily_sales AS
SELECT sale_date AS day, collection_symbol,
       COUNT(*) AS n_sales, ROUND(SUM(price_sol), 4) AS volume_sol, ROUND(SUM(price_usd), 2) AS volume_usd,
       ROUND(MIN(price_sol), 4) AS min_sale_sol, ROUND(MAX(price_sol), 4) AS max_sale_sol,
       COUNT(DISTINCT buyer_id) AS unique_buyers
FROM fact_sale GROUP BY sale_date, collection_symbol;

CREATE VIEW mart_daily_bids AS
SELECT bid_date AS day, collection_symbol,
       COUNT(*) AS n_bids, COUNT(DISTINCT bidder_id) AS unique_bidders,
       ROUND(MAX(price_sol), 4) AS best_bid_sol,
       ROUND(100.0 * SUM(bid_channel = 'pool_amm') / COUNT(*), 1) AS pool_bid_share_pct
FROM fact_bid GROUP BY bid_date, collection_symbol;

CREATE VIEW mart_buyer_activity AS
SELECT buyer_id, COUNT(*) AS n_purchases, COUNT(DISTINCT collection_symbol) AS n_collections,
       ROUND(SUM(price_sol), 4) AS total_spent_sol, MIN(sold_at) AS first_purchase_at,
       MAX(sold_at) AS last_purchase_at
FROM fact_sale GROUP BY buyer_id;

CREATE VIEW mart_bidder_activity AS
SELECT bidder_id, COUNT(*) AS n_bids, COUNT(DISTINCT collection_symbol) AS n_collections,
       ROUND(100.0 * SUM(bid_channel = 'pool_amm') / COUNT(*), 1) AS pool_bid_share_pct,
       ROUND(MAX(price_sol), 4) AS best_bid_sol, MIN(bid_at) AS first_bid_at, MAX(bid_at) AS last_bid_at
FROM fact_bid GROUP BY bidder_id;
"""

SALE_COLS = ["sale_id", "collection_symbol", "token_mint", "sold_at", "sale_date", "price_sol",
             "sol_usd_rate", "price_usd", "buyer_id", "seller_id", "marketplace_source", "run_id",
             "source_file", "contract_version", "loaded_at"]
BID_COLS = ["bid_id", "collection_symbol", "token_mint", "bid_scope", "bid_channel", "bid_at", "bid_date",
            "price_sol", "sol_usd_rate", "price_usd", "bidder_id", "run_id", "source_file",
            "contract_version", "loaded_at"]


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.executescript(SCHEMA_SQL)
    return conn


def get_watermarks(conn: sqlite3.Connection) -> dict[str, float]:
    return {c: float(t) for c, t in
            conn.execute("SELECT collection_symbol, last_activity_ts FROM collection_watermark")}


def _upsert(conn, table: str, key: str, cols: list[str], df: pd.DataFrame, loaded_at: str) -> int:
    if df.empty:
        return 0
    records = df.astype(object).where(pd.notna(df), None).to_dict("records")
    before = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    placeholders = ", ".join(f":{c}" for c in cols)
    conn.executemany(
        f"""INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})
            ON CONFLICT({key}) DO UPDATE SET
              -- seul enrichissement autorisé au rejeu : compléter un cours USD manquant
              sol_usd_rate = COALESCE({table}.sol_usd_rate, excluded.sol_usd_rate),
              price_usd    = COALESCE({table}.price_usd, excluded.price_usd)""",
        [{**r, "loaded_at": loaded_at} for r in records])
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] - before


def publish_all(conn: sqlite3.Connection, sales: pd.DataFrame, bids: pd.DataFrame,
                watermarks: dict[str, int], run_id: str) -> dict:
    """Charge ventes, offres et filigranes dans une transaction unique (tout ou rien)."""
    loaded_at = utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")
    colls = sorted(set(sales.get("collection_symbol", pd.Series(dtype=str)))
                   | set(bids.get("collection_symbol", pd.Series(dtype=str))) | set(watermarks))
    with conn:
        conn.executemany("INSERT OR IGNORE INTO dim_collection(collection_symbol, first_seen_run) VALUES (?, ?)",
                         [(c, run_id) for c in colls])
        ins_s = _upsert(conn, "fact_sale", "sale_id", SALE_COLS[:-1] + ["loaded_at"], sales, loaded_at)
        ins_b = _upsert(conn, "fact_bid", "bid_id", BID_COLS[:-1] + ["loaded_at"], bids, loaded_at)
        conn.executemany(
            """INSERT INTO collection_watermark VALUES (?, ?, ?)
               ON CONFLICT(collection_symbol) DO UPDATE SET
                 last_activity_ts = MAX(last_activity_ts, excluded.last_activity_ts),
                 updated_by_run = excluded.updated_by_run""",
            [(c, int(t), run_id) for c, t in watermarks.items()])
    res = {"inserted_sales": ins_s, "already_loaded_sales": len(sales) - ins_s,
           "inserted_bids": ins_b, "already_loaded_bids": len(bids) - ins_b}
    res["inserted_rows"] = ins_s + ins_b
    res["already_loaded_rows"] = res["already_loaded_sales"] + res["already_loaded_bids"]
    log.info("Base : ventes %d insérée(s) / %d déjà présente(s) ; offres %d / %d (idempotence)",
             ins_s, res["already_loaded_sales"], ins_b, res["already_loaded_bids"])
    return res


def log_run(conn: sqlite3.Connection, report: dict) -> None:
    with conn:
        conn.execute(
            """INSERT OR REPLACE INTO run_log VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (report["run_id"], report["executed_at"], report["mode"], report["status"],
             report.get("input_rows"), report.get("accepted_rows"), report.get("rejected_rows"),
             report.get("duplicates_removed"), report.get("inserted_rows"),
             report.get("duration_seconds")))


# --------------------------------------------------------------------------- marts & KPI

def _half_trend(g: pd.DataFrame, ts_col: str) -> float | None:
    """Variation du prix médian entre la 1re et la 2de moitié de la période observée
    (au moins 3 observations par moitié, sinon non calculée)."""
    t = pd.to_datetime(g[ts_col])
    mid = t.min() + (t.max() - t.min()) / 2
    first, second = g.loc[t <= mid, "price_sol"], g.loc[t > mid, "price_sol"]
    if len(first) < 3 or len(second) < 3:
        return None
    return round((second.median() / first.median() - 1) * 100, 1)


def build_marts(conn: sqlite3.Connection) -> dict[str, pd.DataFrame]:
    sales = pd.read_sql("SELECT * FROM fact_sale ORDER BY sold_at", conn)
    bids = pd.read_sql("SELECT * FROM fact_bid ORDER BY bid_at", conn)
    ds = pd.read_sql("SELECT * FROM mart_daily_sales", conn)
    db = pd.read_sql("SELECT * FROM mart_daily_bids", conn)
    buyers = pd.read_sql("SELECT * FROM mart_buyer_activity ORDER BY total_spent_sol DESC", conn)
    bidders = pd.read_sql("SELECT * FROM mart_bidder_activity ORDER BY n_bids DESC", conn)

    daily = ds.merge(db, on=["day", "collection_symbol"], how="outer").sort_values(["day", "collection_symbol"])
    if not sales.empty:  # SQLite n'a pas de MEDIAN : calcul pandas
        med = sales.groupby(["sale_date", "collection_symbol"])["price_sol"].median().round(4)
        med = med.rename("median_sale_sol").reset_index().rename(columns={"sale_date": "day"})
        daily = daily.merge(med, on=["day", "collection_symbol"], how="left")
    if not buyers.empty:
        buyers["segment"] = pd.cut(buyers["n_purchases"], bins=[0, 1, 4, float("inf")],
                                   labels=["ponctuel (1 achat)", "récurrent (2-4)", "intensif (5+)"])

    rows = []
    for coll in sorted(set(sales.get("collection_symbol", [])) | set(bids.get("collection_symbol", []))):
        s = sales[sales["collection_symbol"] == coll]
        b = bids[bids["collection_symbol"] == coll]
        times = pd.concat([pd.to_datetime(s["sold_at"]), pd.to_datetime(b["bid_at"])])
        row = {"collection_symbol": coll,
               "period_start": times.min().strftime("%Y-%m-%dT%H:%MZ") if len(times) else None,
               "period_end": times.max().strftime("%Y-%m-%dT%H:%MZ") if len(times) else None,
               "hours_observed": round((times.max() - times.min()).total_seconds() / 3600, 1) if len(times) else 0,
               "n_sales": len(s), "volume_sol": round(s["price_sol"].sum(), 2),
               "volume_usd": round(s["price_usd"].sum(), 2) if s["price_usd"].notna().any() else None,
               "median_sale_sol": round(s["price_sol"].median(), 4) if len(s) else None,
               "sale_price_trend_pct": _half_trend(s, "sold_at") if len(s) else None}
        if len(s):
            pb = s.groupby("buyer_id")["price_sol"].agg(["count", "sum"]).sort_values("sum", ascending=False)
            top_n = max(1, int(round(len(pb) * 0.10)))
            row.update({"unique_buyers": len(pb),
                        "repeat_buyer_rate_pct": round((pb["count"] >= 2).mean() * 100, 1),
                        "top10pct_buyers_volume_share_pct": round(pb["sum"].head(top_n).sum() / pb["sum"].sum() * 100, 1)})
        if len(b):
            per_bidder = b["bidder_id"].value_counts()
            med_bid = b["price_sol"].median()
            row.update({"n_bids": len(b), "unique_bidders": int(per_bidder.size),
                        "top_bidder_share_pct": round(per_bidder.iloc[0] / len(b) * 100, 1),
                        "pool_bid_share_pct": round((b["bid_channel"] == "pool_amm").mean() * 100, 1),
                        "median_bid_sol": round(med_bid, 4), "best_bid_sol": round(b["price_sol"].max(), 4),
                        "bid_to_sale_pct": round(med_bid / row["median_sale_sol"] * 100, 1)
                        if row["median_sale_sol"] else None})
        rows.append(row)
    summary = pd.DataFrame(rows)
    return {"sales": sales, "bids": bids, "daily": daily, "buyers": buyers, "bidders": bidders,
            "summary": summary}


def export_marts(marts: dict, curated_dir: Path, marts_dir: Path) -> list[Path]:
    """Régénère intégralement les exports depuis la base (remplacement contrôlé, idempotent)."""
    curated_dir.mkdir(parents=True, exist_ok=True)
    marts_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        curated_dir / "sales_curated.csv": marts["sales"].drop(columns=["loaded_at"], errors="ignore"),
        curated_dir / "bids_curated.csv": marts["bids"].drop(columns=["loaded_at"], errors="ignore"),
        marts_dir / "mart_daily_collection.csv": marts["daily"],
        marts_dir / "mart_buyer_activity.csv": marts["buyers"],
        marts_dir / "mart_bidder_activity.csv": marts["bidders"],
        marts_dir / "mart_collection_summary.csv": marts["summary"],
    }
    for path, df in outputs.items():
        tmp = path.with_suffix(".tmp")
        df.to_csv(tmp, index=False)
        tmp.replace(path)
    return list(outputs)


# --------------------------------------------------------------------------- restitution

def _style_axes(ax, grid_axis="y"):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("#c3c2b7")
    ax.tick_params(colors=INK2, labelsize=8)
    ax.grid(axis=grid_axis, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_facecolor(SURFACE)


def make_charts(marts: dict, reports_dir: Path) -> dict[str, str]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    summary, sales, bids = marts["summary"], marts["sales"], marts["bids"]
    if summary.empty:
        return {}
    reports_dir.mkdir(parents=True, exist_ok=True)
    for old in reports_dir.glob("chart_*.png"):
        old.unlink()
    colls = list(summary["collection_symbol"])          # ordre alphabétique : couleur stable
    color = {c: SERIES_COLORS[i % len(SERIES_COLORS)] for i, c in enumerate(colls)}
    charts = {}

    # 1. Petits multiples : ventes (points) et prix médian horaire des offres (ligne)
    n = len(colls)
    ncols = 2 if n > 1 else 1
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(9, 2.6 * nrows), dpi=150, squeeze=False)
    fig.patch.set_facecolor(SURFACE)
    for ax, c in zip(axes.flat, colls):
        s = sales[sales["collection_symbol"] == c]
        b = bids[bids["collection_symbol"] == c]
        if len(b):
            bh = (b.assign(t=pd.to_datetime(b["bid_at"])).set_index("t")["price_sol"]
                  .resample("1h").median().dropna())
            ax.plot(bh.index, bh.values, color="#9b9a94", linewidth=1.5, label="offre médiane (1 h)")
        if len(s):
            ax.scatter(pd.to_datetime(s["sold_at"]), s["price_sol"], s=22, color=color[c],
                       edgecolor=SURFACE, linewidth=1, zorder=3, label="vente")
        ax.set_title(f"{c}  ·  {len(s)} ventes, {len(b)} offres", loc="left", fontsize=9, color=INK)
        ax.set_ylabel("SOL", fontsize=8, color=INK2)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m %Hh"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=5))
        _style_axes(ax)
    for ax in list(axes.flat)[n:]:
        ax.axis("off")
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=2, frameon=False, fontsize=8)
    fig.suptitle("Prix des ventes et niveau des offres d'achat", x=0.01, ha="left", fontsize=11, color=INK)
    fig.tight_layout(rect=(0, 0.05, 1, 0.97))
    fig.savefig(reports_dir / "chart_prices.png"); plt.close(fig)
    charts["prix"] = "chart_prices.png"

    # 2. Volume des ventes par collection
    fig, ax = plt.subplots(figsize=(9, 0.55 * n + 1.2), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    vals = summary["volume_sol"].fillna(0)
    bars = ax.barh(colls, vals, color=[color[c] for c in colls], height=0.55)
    for bar, v, k in zip(bars, vals, summary["n_sales"]):
        ax.text(bar.get_width(), bar.get_y() + bar.get_height() / 2, f"  {v:.1f} SOL · {k} ventes",
                va="center", fontsize=8, color=INK)
    ax.invert_yaxis(); ax.set_xlim(0, max(vals.max() * 1.35, 1))
    ax.set_title("Volume des ventes sur la période observée", loc="left", fontsize=11, color=INK)
    ax.set_xlabel("SOL", fontsize=8, color=INK2)
    _style_axes(ax, "x")
    fig.tight_layout(); fig.savefig(reports_dir / "chart_volume.png"); plt.close(fig)
    charts["volume"] = "chart_volume.png"

    # 3. Comportement des acheteurs : trois parts (même unité, un seul axe)
    metrics = [("top_bidder_share_pct", "offres du 1er enchérisseur", SERIES_COLORS[0]),
               ("pool_bid_share_pct", "offres automatisées (pools AMM)", SERIES_COLORS[1]),
               ("top10pct_buyers_volume_share_pct", "volume des 10 % d'acheteurs les + dépensiers", SERIES_COLORS[2])]
    fig, ax = plt.subplots(figsize=(9, 0.8 * n + 1.6), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    h = 0.24
    for i, (col, label, col_hex) in enumerate(metrics):
        v = summary.get(col, pd.Series([None] * n)).astype(float).fillna(0)
        ys = [j + (i - 1) * (h + 0.03) for j in range(n)]
        bars = ax.barh(ys, v, height=h, color=col_hex, label=label)
        for bar, x in zip(bars, v):
            ax.text(bar.get_width() + 1, bar.get_y() + h / 2, f"{x:.0f} %", va="center", fontsize=7, color=INK)
    ax.set_yticks(range(n)); ax.set_yticklabels(colls); ax.invert_yaxis(); ax.set_xlim(0, 110)
    ax.set_title("Concentration et automatisation de la demande", loc="left", fontsize=11, color=INK)
    ax.set_xlabel("%", fontsize=8, color=INK2)
    ax.legend(frameon=False, fontsize=7.5, loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=3)
    _style_axes(ax, "x")
    fig.tight_layout(); fig.savefig(reports_dir / "chart_buyer_behaviour.png"); plt.close(fig)
    charts["acheteurs"] = "chart_buyer_behaviour.png"
    return charts


def write_html_report(marts: dict, charts: dict, run_report: dict, reports_dir: Path) -> Path:
    summary, buyers = marts["summary"], marts["buyers"]
    seg_html = ""
    if not buyers.empty:
        seg = (buyers.groupby("segment", observed=False)
               .agg(acheteurs=("buyer_id", "count"), volume_sol=("total_spent_sol", "sum")).reset_index())
        seg["part_volume_pct"] = (seg["volume_sol"] / seg["volume_sol"].sum() * 100).round(1)
        seg["volume_sol"] = seg["volume_sol"].round(2)
        seg_html = seg.to_html(index=False)
    synthetic = run_report.get("synthetic_input", False)
    banner = ("<p class='warn'>⚠ Données d'entrée SYNTHÉTIQUES (démonstration hors ligne) : "
              "les chiffres ne décrivent pas le marché réel.</p>") if synthetic else ""
    imgs = "".join(f"<img src='{f}' alt='{k}'>" for k, f in charts.items())
    html = f"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<title>NFT Market Report</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:1100px;margin:24px auto;padding:0 16px;color:#0b0b0b;background:#fcfcfb}}
table{{border-collapse:collapse;font-size:12px;margin:8px 0 20px}} td,th{{border-bottom:1px solid #e8e7e2;padding:4px 6px;text-align:right}}
th{{color:#52514e}} td:first-child,th:first-child{{text-align:left}} img{{width:100%;margin:8px 0}}
.warn{{background:#fff4d6;border-left:4px solid #fab219;padding:8px}} .meta{{color:#52514e;font-size:13px}}
.scroll{{overflow-x:auto}}
</style></head><body>
<h1>Marché NFT Solana — ventes et offres sur Magic Eden</h1>
<p class="meta">Run {run_report['run_id']} · statut {run_report['status']} · généré le {run_report['executed_at']}</p>
{banner}
<h2>Synthèse par collection</h2><div class="scroll">{summary.to_html(index=False, na_rep='-')}</div>
<h2>Graphiques</h2>{imgs}
<h2>Segments d'acheteurs (ventes, wallets pseudonymisés)</h2>{seg_html}
<p class="meta">Source : API publique Magic Eden (attribution demandée par Magic Eden) ; cours SOL/USD : CoinGecko.
Les identifiants d'acheteurs et d'enchérisseurs sont des pseudonymes (SHA-256 salé), aucune adresse de wallet n'est publiée.</p>
</body></html>"""
    out = reports_dir / "market_report.html"
    out.write_text(html, encoding="utf-8")
    return out
