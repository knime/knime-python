import os
from codecs import ignore_errors
from os import pardir
from typing import Type, Union
import unittest
import pandas as pd
import pyarrow as pa
import numpy as np

import knime_arrow as ka
import knime_arrow_table as kat
import knime_node_arrow_table as knat

import knime_arrow_pandas as kap
import knime_arrow_types as katy
import knime_arrow as knar
import knime_types as kt


class DummyJavaDataSink:
    def __init__(self) -> None:
        import os

        self._path = os.path.join(os.curdir, "test_data_sink")

    def getAbsolutePath(self):
        return self._path

    def reportBatchWritten(self, offset):
        pass

    def setColumnarSchema(self, schema):
        pass

    def setFinalSize(self, size):
        import os

        os.remove(self._path)

    def write(self, data):
        pass


class DummyWriter:
    def write(self, data):
        pass

    def close(self):
        pass


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
                "knime.types.geospatial",
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
                "knime.types.geospatial.GeoValue",
            )

            # to register the arrow<->pandas column converters
            import knime.types.geospatial

            GeoSpatialExtensionTypeTest.geospatial_types_found = True
        except ImportError:
            # We simply skip the tests if no geospatial extension was found
            print(
                "Skipping GeoSpatial tests because knime-geospatial could not be found or 'geopandas' is not available"
            )
            pass

    def _generate_test_table(self, path="geospatial_table_3.zip"):
        # returns a table with: RowKey, WKT (string) and GeoPoint columns
        knime_generated_table_path = path
        test_data_source = TestDataSource(knime_generated_table_path)
        pa_data_source = knar.ArrowDataSource(test_data_source)
        arrow = pa_data_source.to_arrow_table()
        arrow = katy.unwrap_primitive_arrays(arrow)

        return arrow

    def _generate_backends(self):
        dummy_java_sink = DummyJavaDataSink()
        dummy_writer = DummyWriter()
        arrow_sink = ka.ArrowDataSink(dummy_java_sink)
        arrow_sink._writer = dummy_writer

        arrow_backend = kat.ArrowBackend(DummyJavaDataSink)
        node_arrow_backend = knat._ArrowBackend(DummyJavaDataSink)
        return arrow_backend, node_arrow_backend

    def _to_pandas(self, arrow):
        return kap.arrow_data_to_pandas_df(arrow)

    def _generate_test_data_frame(
        self,
        file_name="generatedTestData.zip",
        lists=True,
        sets=True,
        columns=None,
    ) -> pd.DataFrame:
        """
        Creates a Dataframe from a KNIME table on disk
        @param path: path for the KNIME Table
        @param lists: allow lists in output table (extension lists have difficulties)
        @param sets: allow sets in output table (extension sets have difficulties)
        @return: pandas dataframe containing data from KNIME GenerateTestTable node
        """
        knime_generated_table_path = os.path.normpath(
            os.path.join(__file__, "..", file_name)
        )
        test_data_source = TestDataSource(knime_generated_table_path)
        pa_data_source = knar.ArrowDataSource(test_data_source)
        arrow = pa_data_source.to_arrow_table()
        arrow = katy.unwrap_primitive_arrays(arrow)

        df = kap.arrow_data_to_pandas_df(arrow)
        if columns is not None:
            df.columns = columns

        return df

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
        df = pd.concat(
            [
                df,
                geopandas.GeoDataFrame(
                    [["testPoint", Point(12, 34)]], columns=["column1", "geometry"]
                ),
            ],
            ignore_index=True,
        )

        if use_geodf:
            # appending a Point directly only works if it's a GeoDataFrame,
            # but it drops the CRS and we get a deprecation warning from shapely
            df.loc[len(df)] = ["testPoint2", Point(654, 23)]

        out_t = kap.pandas_df_to_arrow(df)
        self.assertEqual(Point(30, 10), out_t[2][0].as_py().to_shapely())
        self.assertEqual(Point(12, 34), out_t[2][1].as_py().to_shapely())
        if use_geodf:
            self.assertEqual(Point(654, 23), out_t[2][2].as_py().to_shapely())

    def test_knime_node_table(self):
        if not GeoSpatialExtensionTypeTest.geospatial_types_found:
            return
        from shapely.geometry import Point
        import geopandas as gpd

        arrow_backend, node_arrow_backend = self._generate_backends()
        # load test table
        t = self._generate_test_table(path="geospatial_table_3.zip")
        df = self._to_pandas(t)
        df.columns = ["column1", "geometry"]

        gdf = gpd.GeoDataFrame(df)
        gdf = pd.concat(
            [
                gdf,
                gpd.GeoDataFrame(
                    [["testPoint", Point(12, 34)]], columns=["column1", "geometry"]
                ),
            ],
            ignore_index=True,
        )
        gdf["area"] = gdf.area
        original_df = gdf.reset_index(drop=True)

        # new backend
        gdf_copy = gdf.copy(deep=True)
        node_table = node_arrow_backend.create_table_from_pandas(
            gdf_copy, sentinel="min"
        )
        geodf2 = gpd.GeoDataFrame(node_table.to_pandas())
        new_df = geodf2.reset_index(drop=True)
        self.assertEqual(original_df.crs, new_df.crs)
        self.assertCountEqual(original_df.columns, new_df.columns)
        self.assertCountEqual(original_df, new_df)

        # old backend
        gdf_copy = gdf.copy(deep=True)
        # test if conversion to table alters the original df
        table = arrow_backend.write_table(gdf_copy)
        new_df = gdf_copy.reset_index(drop=True)
        self.assertEqual(original_df.crs, new_df.crs)
        self.assertCountEqual(original_df.columns, new_df.columns)
        self.assertCountEqual(original_df, new_df)

    def test_after_conversion_to_kntable(self):
        if not GeoSpatialExtensionTypeTest.geospatial_types_found:
            return
        import geopandas as gpd

        arrow_backend, node_arrow_backend = self._generate_backends()

        # load test table
        t = self._generate_test_table(path="geospatial_table_3.zip")
        df = self._to_pandas(t)

        df.columns = ["column1", "geometry"]

        gdf = gpd.GeoDataFrame(df)

        # test for new backend
        gdf_copy = gdf.copy(deep=True)
        node_table = node_arrow_backend.create_table_from_pandas(
            gdf_copy, sentinel="min"
        )
        pd.testing.assert_frame_equal(gdf, gdf_copy)

        # test for old backend
        gdf_copy = gdf.copy(deep=True)
        table = arrow_backend.write_table(gdf_copy)
        pd.testing.assert_frame_equal(gdf, gdf_copy)

    def test_dict_decoding_geospatials(self):
        if not GeoSpatialExtensionTypeTest.geospatial_types_found:
            return
        kt.register_python_value_factory(
            "geospatial_types",
            "GeoValueFactory",
            '{"type": "struct", "inner_types": ["variable_width_binary", "string"]}',
            """
            {
                "type": "struct", 
                "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.geospatial.core.data.cell.GeoCell$ValueFactory\\"}" }, 
                "inner_types": [
                    {"type": "simple", "traits": {}},
                    {"type": "simple", "traits": {}}
                ]
            }
            """,
        )

        df = self._generate_test_data_frame(
            "5kDictEncodedChunkedGeospatials.zip", columns=["Name", "geometry"]
        )
        import geopandas as gpd

        gdf = gpd.GeoDataFrame(df)

        self.assertEqual(str(gdf["geometry"].iloc[-1]), "POINT (50 10)")
        self.assertEqual(
            str(gdf["geometry"].iloc[0]), "LINESTRING (30 10, 10 30, 40 40)"
        )

        # test slicing over a chunked extension array
        sliced = gdf["geometry"].iloc[-21:]
        self.assertEqual(str(sliced[0]), "POINT (50 10)")
        self.assertEqual(str(sliced[3]), "LINESTRING (30 10, 10 30, 40 40)")
        self.assertEqual(str(sliced[5]), "POINT (30 10)")


if __name__ == "__main__":
    unittest.main()
