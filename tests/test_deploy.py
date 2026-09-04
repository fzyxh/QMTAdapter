import json
from pathlib import Path
import runpy
import tempfile
import unittest

from qmt_adapter.deploy import (
    DEFAULT_PIPE_NAME,
    DEFAULT_QUOTE_EVENT_MAX_ITEMS,
    DEFAULT_QUOTE_PIPE_NAME,
    deploy,
)
from qmt_adapter.protocol import MAX_MESSAGE_SIZE
from qmt_adapter.version import __version__


class DeployTests(unittest.TestCase):
    def test_first_deploy_creates_runtime_loader_and_config(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "QMTAdapter"

            result = deploy(root=root, account_ids=["SIM001"])

            self.assertTrue(result["bridge_path"].is_file())
            self.assertTrue(result["loader_path"].is_file())
            self.assertTrue(result["config_path"].is_file())
            self.assertFalse(result["database_path"].exists())
            self.assertTrue(result["config_created"])

            loader = result["loader_path"].read_text(encoding="ascii")
            self.assertIn("exec(bridge_code, globals(), globals())", loader)
            loaded = runpy.run_path(str(result["loader_path"]))
            self.assertEqual(loaded["CONFIG_PATH"], str(result["config_path"]))
            self.assertEqual(
                loaded["init"].__code__.co_filename, str(result["bridge_path"])
            )
            self.assertEqual(
                loaded["handlebar"].__code__.co_filename,
                str(result["bridge_path"]),
            )

            config = json.loads(result["config_path"].read_text(encoding="ascii"))
            self.assertNotIn("environment", config)
            self.assertEqual(config["version"], __version__)
            self.assertEqual(config["accounts"][0]["account_id"], "SIM001")
            self.assertEqual(config["pipe_name"], DEFAULT_PIPE_NAME)
            self.assertEqual(
                config["quote_pipe_name"], DEFAULT_QUOTE_PIPE_NAME
            )
            self.assertEqual(config["max_clients"], 8)
            self.assertEqual(config["max_quote_clients"], 8)
            self.assertEqual(config["max_message_size"], MAX_MESSAGE_SIZE)
            self.assertEqual(
                config["quote_event_max_items"],
                DEFAULT_QUOTE_EVENT_MAX_ITEMS,
            )
            self.assertEqual(len(config["auth_token"]), 64)
            int(config["auth_token"], 16)

    def test_redeploy_updates_version_and_preserves_config_and_database(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "QMTAdapter"
            first = deploy(root=root, account_ids=["SIM001"])
            original_config = json.loads(
                first["config_path"].read_text(encoding="ascii")
            )
            original_config["version"] = "0.1.0"
            first["config_path"].write_text(
                json.dumps(original_config, ensure_ascii=True, indent=2) + "\n",
                encoding="ascii",
            )
            first["database_path"].write_bytes(b"database-sentinel")
            first["bridge_path"].write_bytes(b"old-bridge")

            second = deploy(root=root, account_ids=["IGNORED"])

            self.assertFalse(second["config_created"])
            updated_config = json.loads(
                second["config_path"].read_text(encoding="ascii")
            )
            self.assertEqual(updated_config["version"], __version__)
            self.assertEqual(
                {key: value for key, value in updated_config.items() if key != "version"},
                {key: value for key, value in original_config.items() if key != "version"},
            )
            self.assertEqual(
                second["database_path"].read_bytes(), b"database-sentinel"
            )
            self.assertNotEqual(second["bridge_path"].read_bytes(), b"old-bridge")

    def test_redeploy_migrates_legacy_defaults_and_removes_environment(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "QMTAdapter"
            first = deploy(root=root, account_ids=["SIM001"])
            config = json.loads(first["config_path"].read_text(encoding="ascii"))
            config["version"] = 1
            config["environment"] = "SIMULATION"
            config["pipe_name"] = r"\\.\pipe\qmt_adapter_v1"
            config["max_message_size"] = 1024 * 1024
            del config["max_clients"]
            for key in (
                "quote_pipe_name",
                "max_quote_clients",
                "quote_client_queue_size",
                "quote_event_queue_size",
            ):
                config.pop(key, None)
            config["quote_event_max_items"] = 500
            first["config_path"].write_text(
                json.dumps(config, ensure_ascii=True, indent=2) + "\n",
                encoding="ascii",
            )

            second = deploy(root=root)

            updated = json.loads(second["config_path"].read_text(encoding="ascii"))
            self.assertFalse(second["config_created"])
            self.assertEqual(updated["version"], __version__)
            self.assertNotIn("environment", updated)
            self.assertEqual(updated["pipe_name"], DEFAULT_PIPE_NAME)
            self.assertEqual(updated["max_message_size"], MAX_MESSAGE_SIZE)
            self.assertEqual(updated["max_clients"], 8)
            self.assertEqual(
                updated["quote_pipe_name"], DEFAULT_QUOTE_PIPE_NAME
            )
            self.assertEqual(updated["max_quote_clients"], 8)
            self.assertEqual(
                updated["quote_event_max_items"],
                DEFAULT_QUOTE_EVENT_MAX_ITEMS,
            )
            self.assertEqual(updated["accounts"], config["accounts"])
            self.assertEqual(updated["auth_token"], config["auth_token"])

    def test_redeploy_preserves_custom_pipe_and_message_size(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "QMTAdapter"
            first = deploy(root=root, account_ids=["SIM001"])
            config = json.loads(first["config_path"].read_text(encoding="ascii"))
            config["pipe_name"] = r"\\.\pipe\custom_qmt_adapter"
            config["max_clients"] = 3
            config["max_message_size"] = 2 * 1024 * 1024
            config["quote_event_max_items"] = 1234
            first["config_path"].write_text(
                json.dumps(config, ensure_ascii=True, indent=2) + "\n",
                encoding="ascii",
            )

            deploy(root=root)

            updated = json.loads(first["config_path"].read_text(encoding="ascii"))
            self.assertEqual(updated["pipe_name"], config["pipe_name"])
            self.assertEqual(updated["max_clients"], 3)
            self.assertEqual(
                updated["max_message_size"], config["max_message_size"]
            )
            self.assertEqual(updated["quote_event_max_items"], 1234)

    def test_first_deploy_requires_account_id(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(ValueError):
                deploy(root=Path(temp_dir) / "QMTAdapter")

    def test_relative_root_is_rejected(self):
        with self.assertRaises(ValueError):
            deploy(root="relative-path", account_ids=["SIM001"])


if __name__ == "__main__":
    unittest.main()
