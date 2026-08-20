import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union


ConfigPath = Optional[Union[str, os.PathLike]]


DEFAULT_CONFIG_PATH = Path(
    os.environ.get(
        "QMT_ADAPTER_CONFIG",
        r"C:\pazq_qmt_simulate\userdata\qmt_adapter\bridge_config.json",
    )
)


def load_config(path: ConfigPath = None) -> Dict[str, Any]:
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    with config_path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    config["_config_path"] = str(config_path)
    return config
