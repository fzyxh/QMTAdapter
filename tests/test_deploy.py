import json
from pathlib import Path
import runpy
import tempfile
import unittest

from qmt_adapter.deploy import deploy


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
            self.assertEqual(config["environment"], "SIMULATION")
            self.assertEqual(config["accounts"][0]["account_id"], "SIM001")
            self.assertEqual(len(config["auth_token"]), 64)
            int(config["auth_token"], 16)

    def test_redeploy_preserves_config_and_database(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "QMTAdapter"
            first = deploy(root=root, account_ids=["SIM001"])
            original_config = first["config_path"].read_bytes()
            first["database_path"].write_bytes(b"database-sentinel")
            first["bridge_path"].write_bytes(b"old-bridge")

            second = deploy(root=root, account_ids=["IGNORED"])

            self.assertFalse(second["config_created"])
            self.assertEqual(second["config_path"].read_bytes(), original_config)
            self.assertEqual(
                second["database_path"].read_bytes(), b"database-sentinel"
            )
            self.assertNotEqual(second["bridge_path"].read_bytes(), b"old-bridge")

    def test_first_deploy_requires_account_id(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(ValueError):
                deploy(root=Path(temp_dir) / "QMTAdapter")

    def test_relative_root_is_rejected(self):
        with self.assertRaises(ValueError):
            deploy(root="relative-path", account_ids=["SIM001"])


if __name__ == "__main__":
    unittest.main()
