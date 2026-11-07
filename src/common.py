"""Utilitaires partagés : configuration, chemins, logs, identifiant d'exécution."""
from __future__ import annotations

import hashlib
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

# Racine du projet = dossier parent de src/
ROOT = Path(__file__).resolve().parent.parent


def load_yaml(path: str | Path) -> dict:
    path = Path(path)
    if not path.is_absolute():
        path = ROOT / path
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def load_config(path: str | Path = "config/config.yaml") -> dict:
    return load_yaml(path)


def project_path(relative: str | Path) -> Path:
    """Convertit un chemin relatif de la config en chemin absolu sous la racine du projet."""
    p = Path(relative)
    return p if p.is_absolute() else ROOT / p


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_run_id() -> str:
    """Identifiant d'exécution lisible et triable, ex. 20261107T203000Z."""
    return utc_now().strftime("%Y%m%dT%H%M%SZ")


def setup_logging(log_file: str | Path, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger("nft")
    for h in list(logger.handlers):  # reconfiguration (tests, incidents dans le même process)
        logger.removeHandler(h)
        h.close()
    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                            datefmt="%Y-%m-%dT%H:%M:%S")
    log_path = project_path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"nft.{name}")


DEMO_SALT = "demo-salt-change-me"


def get_salt(env_var: str = "WALLET_HASH_SALT") -> tuple[str, bool]:
    """Retourne (sel, is_demo). Le sel réel ne doit jamais être versionné."""
    salt = os.getenv(env_var)
    return (salt, False) if salt else (DEMO_SALT, True)


def pseudonymize(value, salt: str) -> str | None:
    """Pseudonymisation d'un wallet : SHA-256(sel + adresse), tronqué à 16 caractères hexadécimaux."""
    if value is None or (isinstance(value, float) and value != value) or str(value).strip() == "":
        return None
    return hashlib.sha256((salt + str(value)).encode("utf-8")).hexdigest()[:16]
