import os
import subprocess
import sys
import unittest
import platform


class BlackFormattingTest(unittest.TestCase):
    def test_black_formatting(self):
        root_dir = os.path.normpath(os.path.join(__file__, ".."))
        res = subprocess.run(
            [
                self.get_black_path(),
                root_dir,
                "--check",
                "--extend-exclude",
                "pytest-envs",
            ],
            capture_output=True,
        )
        if res.returncode != 0:
            print(res.stdout.decode())
            print(res.stderr.decode(), file=sys.stderr)
            self.fail(
                (
                    "Not all python files are correctly formatted. "
                    "Run 'python -m black <path_to_knime-python>' to autoformat the files.\n"
                    + res.stderr.decode()
                )
            )

    def get_black_path(self):
        folder_name = "Scripts" if platform.system() == "Windows" else "bin"
        return os.path.join(sys.prefix, folder_name, "black")
