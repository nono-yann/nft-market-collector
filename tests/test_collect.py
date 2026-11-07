"""Tests du collecteur avec une fausse session HTTP (aucun accès réseau)."""
import json
import time

import requests

import collect
from conftest import ROOT

CFG = {"magiceden": {"base_url": "https://api.test/v2", "endpoint": "/collections/{symbol}/activities",
                     "collections": ["okay_bears", "bad_symbol"], "page_size": 2, "max_pages": 10,
                     "lookback_days": 30, "sleep_seconds": 0, "timeout_seconds": 1, "max_retries": 2,
                     "sale_types": ["buyNow"]},
       "coingecko": {"enabled": False},
       "paths": {"raw_dir": "unused"}}


class FakeResp:
    def __init__(self, status, data):
        self.status_code, self._data = status, data

    def json(self):
        return self._data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)


class FakeSession:
    """Simule l'API : 5 activités récentes pour okay_bears, 400 pour un symbole inconnu,
    et un 429 au premier appel pour vérifier la reprise."""
    def __init__(self):
        now = int(time.time())
        self.items = [{"signature": f"s{i}", "blockTime": now - i * 60} for i in range(5)]
        self.calls = 0

    def get(self, url, params, headers, timeout):
        self.calls += 1
        if self.calls == 1:
            return FakeResp(429, None)
        if "bad_symbol" in url:
            return FakeResp(400, {"msg": "invalid symbol"})
        o, l = params["offset"], params["limit"]
        return FakeResp(200, self.items[o:o + l])


def test_pagination_retry_and_raw_envelope(tmp_path, monkeypatch):
    monkeypatch.setattr(collect.time, "sleep", lambda s: None)
    out = collect.collect_collection("okay_bears", CFG, "RUN1", tmp_path, FakeSession())
    payload = json.loads(out.read_text())
    assert payload["_meta"]["records"] == 5                 # 3 pages : 2 + 2 + 1
    assert payload["_meta"]["stop_reason"] == "last_page"
    assert out.name == "magiceden_okay_bears_RUN1.json"
    # la réponse brute est conservée telle quelle
    assert payload["pages"][0]["response"][0] == {"signature": "s0", "blockTime": payload["pages"][0]["response"][0]["blockTime"]}


def test_invalid_collection_does_not_stop_others(tmp_path, monkeypatch):
    monkeypatch.setattr(collect.time, "sleep", lambda s: None)
    monkeypatch.setattr(collect, "project_path", lambda p: tmp_path)
    monkeypatch.setattr(collect.requests, "Session", FakeSession)
    res = collect.collect_all(CFG, "RUN2")
    assert len(res["sales_files"]) == 1 and "bad_symbol" in res["errors"]


def test_incremental_collection_stops_at_watermark(tmp_path, monkeypatch):
    monkeypatch.setattr(collect.time, "sleep", lambda s: None)
    sess = FakeSession()
    sess.calls = 1                                   # pas de 429 simulé ici
    now = int(time.time())
    sess.items = [{"signature": f"s{i}", "blockTime": now - i * 1800} for i in range(20)]  # 1 activité / 30 min
    cfg = json.loads(json.dumps(CFG))
    cfg["magiceden"]["overlap_minutes"] = 60
    out = collect.collect_collection("okay_bears", cfg, "RUN3", tmp_path, sess, since_ts=now - 3 * 3600)
    meta = json.loads(out.read_text())["_meta"]
    assert meta["stop_reason"] == "watermark_reached"
    assert meta["records"] < 20 and meta["watermark_ts"] == now - 3 * 3600
