import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
SAMPLES = ROOT / "data" / "samples"


@pytest.fixture
def tmp_config(tmp_path):
    """Copie de la config dont toutes les sorties pointent vers un dossier temporaire :
    les tests ne modifient jamais data/ ni reports/ du projet."""
    cfg = yaml.safe_load((ROOT / "config" / "config.yaml").read_text(encoding="utf-8"))
    for key in cfg["paths"]:
        cfg["paths"][key] = str(tmp_path / key)
    cfg["paths"]["database"] = str(tmp_path / "test.db")
    cfg["pipeline"]["log_file"] = str(tmp_path / "pipeline.log")
    cfg["pipeline"]["contract_path"] = str(ROOT / "config" / "data_contract.yaml")
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    return path


def synthetic_files():
    return sorted(str(p) for p in SAMPLES.glob("magiceden_*_SYNTHETIC.json"))
