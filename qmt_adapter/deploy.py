"""Deploy the QMT-side bridge to a stable local directory."""

import json
import os
from pathlib import Path
import pkgutil
import secrets
import tempfile
from typing import Any, Dict, Iterable, Optional, Union

from .protocol import MAX_MESSAGE_SIZE
from .version import __version__


PathValue = Union[str, os.PathLike]

DEFAULT_INSTALL_ROOT = Path(r"C:\QMTAdapter")
RUNTIME_RELATIVE_PATH = Path("runtime") / "qmt_adapter_qmt.py"
CONFIG_RELATIVE_PATH = Path("config") / "bridge_config.json"
DATABASE_RELATIVE_PATH = Path("data") / "bridge.db"
LOADER_RELATIVE_PATH = Path("qmt_adapter_loader.py")
DEFAULT_PIPE_NAME = r"\\.\pipe\qmt_adapter"
LEGACY_DEFAULT_PIPE_NAME = r"\\.\pipe\qmt_adapter_v1"
LEGACY_MAX_MESSAGE_SIZE = 1024 * 1024


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=str(path.parent), prefix=path.name + ".", delete=False
        ) as handle:
            temporary_name = handle.name
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, str(path))
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _bridge_source() -> bytes:
    source = pkgutil.get_data("qmt_side", "qmt_adapter_qmt.py")
    if source is None:
        raise RuntimeError("packaged QMT bridge source is missing")
    return source


def _loader_source(bridge_path: Path, config_path: Path) -> bytes:
    bridge_text = str(bridge_path)
    config_text = str(config_path)
    try:
        bridge_text.encode("ascii")
        config_text.encode("ascii")
    except UnicodeEncodeError:
        raise ValueError("deployment path must contain ASCII characters only")

    source = """# coding: gbk
# This file is copied once into a QMT Python strategy.
# The deployed bridge replaces these two placeholder functions at load time.

BRIDGE_PATH = {bridge_path!r}
QMT_ADAPTER_CONFIG_PATH = {config_path!r}


def init(ContextInfo):
    pass


def handlebar(ContextInfo):
    pass


with open(BRIDGE_PATH, "rb") as bridge_file:
    bridge_code = compile(bridge_file.read(), BRIDGE_PATH, "exec")

exec(bridge_code, globals(), globals())
""".format(bridge_path=bridge_text, config_path=config_text)
    return source.encode("ascii")


def _initial_config(account_ids: Iterable[str], db_path: Path) -> Dict[str, Any]:
    normalized_accounts = []
    seen = set()
    for account_id in account_ids:
        normalized = str(account_id).strip()
        if normalized and normalized not in seen:
            normalized_accounts.append(
                {"account_id": normalized, "account_type": "STOCK"}
            )
            seen.add(normalized)
    if not normalized_accounts:
        raise ValueError(
            "--account-id is required when bridge_config.json does not exist"
        )
    return {
        "version": __version__,
        "pipe_name": DEFAULT_PIPE_NAME,
        "auth_token": secrets.token_hex(32),
        "db_path": str(db_path),
        "accounts": normalized_accounts,
        "timer_period": "10nMilliSecond",
        "reconcile_interval_seconds": 5.0,
        "max_commands_per_tick": 20,
        "max_pending_commands": 1000,
        "max_clients": 8,
        "max_message_size": MAX_MESSAGE_SIZE,
        "qmt_remark_max_bytes": 64,
    }


def _migrate_existing_config(config_path: Path) -> None:
    """更新部署版本并迁移旧默认值，保留用户明确设置的配置。"""
    config = json.loads(config_path.read_text(encoding="ascii"))
    changed = False
    if config.get("version") != __version__:
        config["version"] = __version__
        changed = True
    if "environment" in config:
        del config["environment"]
        changed = True
    if config.get("pipe_name") == LEGACY_DEFAULT_PIPE_NAME:
        config["pipe_name"] = DEFAULT_PIPE_NAME
        changed = True
    if config.get("max_message_size") in (None, LEGACY_MAX_MESSAGE_SIZE):
        config["max_message_size"] = MAX_MESSAGE_SIZE
        changed = True
    if "max_clients" not in config:
        config["max_clients"] = 8
        changed = True
    if not changed:
        return
    encoded_config = (json.dumps(config, ensure_ascii=True, indent=2) + "\n").encode(
        "ascii"
    )
    _atomic_write(config_path, encoded_config)


def deploy(
    root: Optional[PathValue] = None,
    account_ids: Iterable[str] = (),
) -> Dict[str, Any]:
    """Deploy or upgrade the QMT-side runtime.

    The bridge and loader are replaced atomically on every call. Existing
    account, authentication, database settings and SQLite data are preserved.
    The configuration's ``version`` is updated to the deployed package version.
    Legacy default pipe and message-size settings are migrated; existing
    configurations receive the default multi-client limit when absent, while
    custom values are preserved. The obsolete ``environment`` setting is
    removed.

    Args:
        root: Absolute deployment directory. Defaults to ``C:\\QMTAdapter``.
        account_ids: Stock account IDs used only when creating the initial
            configuration. At least one is required on first deployment.

    Returns:
        Paths written by the deployment and whether the configuration was
        newly created.
    """
    install_root = Path(root) if root is not None else DEFAULT_INSTALL_ROOT
    if not install_root.is_absolute():
        raise ValueError("deployment root must be an absolute path")

    bridge_path = install_root / RUNTIME_RELATIVE_PATH
    config_path = install_root / CONFIG_RELATIVE_PATH
    database_path = install_root / DATABASE_RELATIVE_PATH
    loader_path = install_root / LOADER_RELATIVE_PATH

    config = None
    if not config_path.exists():
        config = _initial_config(account_ids, database_path)

    bridge_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    _atomic_write(bridge_path, _bridge_source())
    _atomic_write(loader_path, _loader_source(bridge_path, config_path))

    config_created = False
    if config is not None:
        encoded_config = (
            json.dumps(config, ensure_ascii=True, indent=2) + "\n"
        ).encode("ascii")
        _atomic_write(config_path, encoded_config)
        config_created = True
    else:
        _migrate_existing_config(config_path)

    return {
        "root": install_root,
        "bridge_path": bridge_path,
        "loader_path": loader_path,
        "config_path": config_path,
        "database_path": database_path,
        "config_created": config_created,
    }
