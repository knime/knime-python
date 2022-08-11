import os
import subprocess
import sys
import unittest


class BlackFormattingTest(unittest.TestCase):
    def test_black_formatting(self):
        root_dir = os.path.normpath(os.path.join(__file__, ".."))
        black_path = os.path.join(sys.prefix, "bin", "black")
        res = subprocess.run([black_path, root_dir, "--check"], capture_output=True)
        if res.returncode != 0:
            print(res.stdout.decode())
            print(res.stderr.decode(), file=sys.stderr)
            self.fail(
                (
                    "Not all python files are correctly formatted. "
                    "Run 'python -m black <path_to_knime-python>' to autoformat the files."
                )
            )
