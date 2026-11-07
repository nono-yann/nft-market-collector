"""Étape 1 - Collecte reproductible.

Interroge l'API publique Magic Eden (activités d'une collection) et CoinGecko (cours SOL/USD),
puis écrit les réponses NON MODIFIÉES dans la zone raw, avec une enveloppe de métadonnées
(_meta) qui assure la traçabilité : URL, paramètres, horodatage, run_id, statuts HTTP.

Utilisation autonome :
    python src/collect.py
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import requests

from common import get_logger, load_config, new_run_id, project_path, setup_logging, utc_now

log = get_logger("collect")

USER_AGENT = "nft-market-collector/1.0 (projet etudiant ESEO)"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class SourceUnavailableError(RuntimeError):
    """Aucune donnée n'a pu être récupérée depuis la source."""


def _headers() -> dict:
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    api_key = os.getenv("MAGICEDEN_API_KEY")  # optionnelle, jamais dans le dépôt
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def http_get_json(url: str, params: dict, timeout: int, max_retries: int,
                  session: requests.Session | None = None) -> tuple[int, object]:
    """GET avec reprise (backoff exponentiel) sur erreurs réseau, 429 et 5xx.

    Retourne (status_code, json). Lève requests.HTTPError pour les erreurs non récupérables.
    """
    sess = session or requests.Session()
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = sess.get(url, params=params, headers=_headers(), timeout=timeout)
            if resp.status_code in RETRYABLE_STATUS:
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            return resp.status_code, resp.json()
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError, ValueError) as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status is not None and status not in RETRYABLE_STATUS:
                raise  # 400, 401, 404... : inutile de réessayer
            last_error = exc
            wait = 2 ** attempt
            log.warning("Échec tentative %d/%d sur %s (%s) - nouvel essai dans %ss",
                        attempt, max_retries, url, exc, wait)
            time.sleep(wait)
    raise SourceUnavailableError(f"{url} indisponible après {max_retries} tentatives : {last_error}")


def _write_raw_atomic(path: Path, payload: dict) -> None:
    """Écriture atomique : fichier temporaire puis renommage (pas de raw à moitié écrit)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)


def collect_collection(symbol: str, cfg: dict, run_id: str, raw_dir: Path,
                       session: requests.Session | None = None, since_ts: float | None = None) -> Path:
    """Collecte incrémentale : on pagine du plus récent au plus ancien et on s'arrête dès que l'on
    rejoint le filigrane `since_ts` (dernière activité déjà chargée, moins une marge de recouvrement)
    ou, au premier run, la fenêtre lookback_days. max_pages reste un garde-fou."""
    me = cfg["magiceden"]
    url = me["base_url"].rstrip("/") + me["endpoint"].format(symbol=symbol)
    lookback_cutoff = utc_now().timestamp() - me["lookback_days"] * 86400
    overlap = me.get("overlap_minutes", 60) * 60
    cutoff_ts = max(lookback_cutoff, since_ts - overlap) if since_ts else lookback_cutoff
    cutoff_reason = "watermark_reached" if since_ts and since_ts - overlap > lookback_cutoff else "lookback_reached"
    pages, statuses = [], []
    stop_reason = "max_pages"

    for page in range(me["max_pages"]):
        params = {"offset": page * me["page_size"], "limit": me["page_size"]}
        try:
            status, data = http_get_json(url, params, me["timeout_seconds"], me["max_retries"], session)
        except (requests.HTTPError, SourceUnavailableError) as exc:
            resp = getattr(exc, "response", None)
            status = resp.status_code if resp is not None else "unavailable"
            if page == 0:
                raise SourceUnavailableError(f"{symbol} : {status} dès la 1re page ({exc})") from exc
            # Pages déjà reçues conservées : collecte partielle tracée dans stop_reason
            stop_reason = f"error_{status}_at_offset_{params['offset']}"
            log.warning("%s : arrêt de pagination (%s)", symbol, stop_reason)
            break

        statuses.append(status)
        pages.append({"offset": params["offset"], "limit": params["limit"], "response": data})

        if not isinstance(data, list) or len(data) == 0:
            stop_reason = "empty_page"
            break
        times = [a.get("blockTime") for a in data if isinstance(a, dict) and a.get("blockTime")]
        if times and min(times) < cutoff_ts:
            stop_reason = cutoff_reason
            break
        if len(data) < me["page_size"]:
            stop_reason = "last_page"
            break
        time.sleep(me["sleep_seconds"])

    n_records = sum(len(p["response"]) for p in pages if isinstance(p["response"], list))
    payload = {
        "_meta": {
            "source": "magiceden",
            "url": url,
            "collection": symbol,
            "run_id": run_id,
            "collected_at": utc_now().isoformat(),
            "pages_fetched": len(pages),
            "records": n_records,
            "http_statuses": statuses,
            "stop_reason": stop_reason,
            "lookback_days": me["lookback_days"],
            "watermark_ts": since_ts,
            "synthetic": False,
        },
        "pages": pages,
    }
    out = raw_dir / f"magiceden_{symbol}_{run_id}.json"
    _write_raw_atomic(out, payload)
    log.info("%s : %d activités brutes, %d pages (%s) -> %s",
             symbol, n_records, len(pages), stop_reason, out.name)
    return out


def collect_sol_usd(cfg: dict, run_id: str, raw_dir: Path,
                    session: requests.Session | None = None) -> Path | None:
    cg = cfg["coingecko"]
    if not cg.get("enabled", False):
        return None
    try:
        status, data = http_get_json(cg["url"], cg["params"], cg["timeout_seconds"],
                                     cg["max_retries"], session)
    except (SourceUnavailableError, requests.HTTPError) as exc:
        # Source d'enrichissement : son absence dégrade le résultat mais ne bloque pas le run
        log.warning("CoinGecko indisponible, price_usd sera vide : %s", exc)
        return None
    payload = {
        "_meta": {"source": "coingecko", "url": cg["url"], "params": cg["params"],
                  "run_id": run_id, "collected_at": utc_now().isoformat(),
                  "http_status": status, "synthetic": False},
        "response": data,
    }
    out = raw_dir / f"coingecko_sol_usd_{run_id}.json"
    _write_raw_atomic(out, payload)
    log.info("CoinGecko : %d cours journaliers -> %s", len(data.get("prices", [])), out.name)
    return out


def collect_all(cfg: dict, run_id: str, watermarks: dict | None = None) -> dict:
    """Collecte toutes les sources. `watermarks` = {collection: dernier blockTime chargé}."""
    watermarks = watermarks or {}
    raw_dir = project_path(cfg["paths"]["raw_dir"])
    session = requests.Session()
    files, errors = [], {}
    for symbol in cfg["magiceden"]["collections"]:
        try:
            files.append(collect_collection(symbol, cfg, run_id, raw_dir, session,
                                            since_ts=watermarks.get(symbol)))
        except (SourceUnavailableError, requests.RequestException) as exc:
            errors[symbol] = str(exc)
            log.error("Collecte %s en échec : %s", symbol, exc)
    if not files:
        raise SourceUnavailableError(f"Aucune collection collectée : {errors}")
    fx_file = collect_sol_usd(cfg, run_id, raw_dir, session)
    return {"sales_files": files, "fx_file": fx_file, "errors": errors}


if __name__ == "__main__":
    config = load_config()
    setup_logging(config["pipeline"]["log_file"])
    result = collect_all(config, new_run_id())
    print(json.dumps({k: [str(p) for p in v] if isinstance(v, list) else str(v)
                      for k, v in result.items()}, indent=2, ensure_ascii=False))
