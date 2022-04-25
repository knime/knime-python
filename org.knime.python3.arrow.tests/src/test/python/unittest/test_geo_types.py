from codecs import ignore_errors
from os import pardir
from typing import Type, Union
import unittest
import pandas as pd
import pyarrow as pa
import numpy as np

import pythonpath
import knime_arrow_pandas as kap
import knime_arrow_types as katy
import knime_arrow as knar
import knime_types as kt


class TestDataSource:
    def __init__(self, absolute_path):
        self.absolute_path = absolute_path

    def getAbsolutePath(self):
        return self.absolute_path

    def isFooterWritten(self):
        return True

    def hasColumnNames(self):
        return False


class GeoSpatialExtensionTypeTest(unittest.TestCase):
    """
    We currently put the testcase for the GeoSpatial types here because the test needs access 
    to some internals (moc an ArrowDataSource...) and also serves as test for the column conversion 
    extension mechanism.
    
    However, it only works if knime-geospatial is checked out right next to knime-python. 
    TODO: We should consider moving this test over to knime-geospatial as part of AP-18690.
    """

    geospatial_types_found = False

    @classmethod
    def setUpClass(cls):
        try:
            import geopandas
            import sys
            import os

            # we expect that the knime-geospatial git repository is located next to knime-python
            this_file_path = os.path.dirname(os.path.realpath(__file__))
            sys.path.append(
                os.path.join(
                    "..",
                    "..",
                    "..",
                    "..",
                    "..",
                    "..",
                    "knime-geospatial",
                    "org.knime.geospatial.python",
                    "src",
                    "main",
                    "python",
                )
            )

            kt.register_python_value_factory(
                "geospatial_types",
                "GeoValueFactory",
                '{"type": "struct", "inner_types": ["variable_width_binary", "string"]}',
                """
                {
                    "type": "struct", 
                    "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.geospatial.core.data.cell.GeoPointCell$ValueFactory\\"}" }, 
                    "inner_types": [
                        {"type": "simple", "traits": {}},
                        {"type": "simple", "traits": {}}
                    ]
                }
                """,
            )

            # to register the arrow<->pandas column converters
            import geospatial_types

            GeoSpatialExtensionTypeTest.geospatial_types_found = True
        except ImportError:
            # We simply skip the tests if no geospatial extension was found
            print(
                "Skipping GeoSpatial tests because knime-geospatial could not be found or 'geopandas' is not available"
            )
            pass

    def _generate_test_table(self):
        # returns a table with: RowKey, WKT (string) and GeoPoint columns
        knime_generated_table_path = "geospatial_table_3.zip"

        test_data_source = TestDataSource(knime_generated_table_path)
        pa_data_source = knar.ArrowDataSource(test_data_source)
        arrow = pa_data_source.to_arrow_table()
        arrow = katy.unwrap_primitive_arrays(arrow)

        return arrow

    def _to_pandas(self, arrow):
        return kap.arrow_data_to_pandas_df(arrow)

    def test_load_table(self):
        if not GeoSpatialExtensionTypeTest.geospatial_types_found:
            return

        t = self._generate_test_table()
        self.assertEqual(["<Row ID>", "column1", "geometry"], t.schema.names)
        self.assertEqual([pa.string(), pa.string()], t.schema.types[0:2])
        self.assertIsInstance(t.schema.types[2], pa.ExtensionType)

    def test_load_df(self):
        if not GeoSpatialExtensionTypeTest.geospatial_types_found:
            return

        t = self._generate_test_table()
        df = self._to_pandas(t)

        from shapely.geometry import Point
        import geopandas

        use_geodf = True
        if use_geodf:
            df = geopandas.GeoDataFrame(df)

        # Appending this way keeps the CRS
        df = df.append(
            geopandas.GeoDataFrame(
                [["testPoint", Point(12, 34)]], columns=["column1", "geometry"]
            ),
            ignore_index=True,
        )

        if use_geodf:
            # appending a Point directly only works if it's a GeoDataFrame,
            # but it drops the CRS and we get a deprecation warning from shapely
            df.loc[len(df)] = ["testPoint2", Point(654, 23)]
            df = pd.DataFrame(df)

        out_t = kap.pandas_df_to_arrow(df)
        self.assertEqual(Point(30, 10), out_t[2][0].as_py().to_shapely())
        self.assertEqual(Point(12, 34), out_t[2][1].as_py().to_shapely())
        if use_geodf:
            self.assertEqual(Point(654, 23), out_t[2][2].as_py().to_shapely())


if __name__ == "__main__":
    unittest.main()
