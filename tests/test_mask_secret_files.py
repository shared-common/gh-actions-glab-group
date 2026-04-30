import io
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / ".github" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import mask_secret_files  # noqa: E402


class MaskSecretFilesTests(unittest.TestCase):
    def test_escape_workflow_command_escapes_special_characters(self):
        self.assertEqual(
            mask_secret_files.escape_workflow_command("line1%\nline2\r"),
            "line1%25%0Aline2%0D",
        )

    def test_main_masks_each_secret_value_once(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            alpha_path = Path(temp_dir) / "ALPHA"
            beta_path = Path(temp_dir) / "BETA"
            alpha_path.write_text("secret-value\nsecond-line", encoding="utf-8")
            beta_path.write_text("secret-value\nsecond-line", encoding="utf-8")

            env = {
                "SECRET_NAMES": "ALPHA,BETA",
                "ALPHA_FILE": str(alpha_path),
                "BETA_FILE": str(beta_path),
            }

            with mock.patch.dict(os.environ, env, clear=False):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    self.assertEqual(mask_secret_files.main(), 0)

        output_lines = stdout.getvalue().splitlines()
        self.assertEqual(
            output_lines,
            [
                "::add-mask::secret-value%0Asecond-line",
                "::add-mask::secret-value",
                "::add-mask::second-line",
            ],
        )


if __name__ == "__main__":
    unittest.main()
