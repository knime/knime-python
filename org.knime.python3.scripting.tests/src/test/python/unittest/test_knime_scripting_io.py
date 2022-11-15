import unittest

import pandas as pd
import knime.scripting.io as knio
import sys


class DummyDataSink:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def write(self, data):
        pass

    def close(self):
        pass


class NewApiTest(unittest.TestCase):
    def setUp(self):
        import knime.api.table as ktn
        import knime._arrow._table as knat

        ktn._backend = knat._ArrowBackend(lambda: DummyDataSink())

    def test_new_api_available(self):
        df = pd.DataFrame()
        df["A"] = [1, 2, 3, 4]
        t = knio.Table.from_pandas(df)

    def test_new_batch_api_available(self):
        df = pd.DataFrame()
        df["A"] = [1, 2, 3, 4]
        t = knio.BatchOutputTable.create()
        t.append(knio.Table.from_pandas(df))
        t.append(knio.Table.from_pandas(df))
        self.assertEqual(2, t.num_batches)


class ApiDeprecationTest(unittest.TestCase):
    def setUp(self):
        if "knime_io" in sys.modules:
            del sys.modules["knime_io"]
        if "knime.scripting.io" in sys.modules:
            del sys.modules["knime.scripting.io"]

    def test_old_api_warns_about_deprecation(self):
        with self.assertWarns(DeprecationWarning):
            import knime_io

        del sys.modules["knime_io"]

    def test_cannot_import_knio_after_scripting_io(self):
        import knime.scripting.io

        with self.assertRaises(ImportError):
            with self.assertWarns(DeprecationWarning):
                import knime_io

    def test_cannot_import_scripting_io_after_knio(self):
        with self.assertWarns(DeprecationWarning):
            import knime_io

        with self.assertRaises(ImportError):
            import knime.scripting.io


class KnimeKernelTest(unittest.TestCase):
    def test_import_knime_kernel(self):
        import _kernel_launcher

        del sys.modules["_kernel_launcher"]


if __name__ == "__main__":
    unittest.main()
