# ------------------------------------------------------------------------
#  Copyright by KNIME AG, Zurich, Switzerland
#  Website: http://www.knime.com; Email: contact@knime.com
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License, Version 3, as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, see <http://www.gnu.org/licenses>.
#
#  Additional permission under GNU GPL version 3 section 7:
#
#  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
#  Hence, KNIME and ECLIPSE are both independent programs and are not
#  derived from each other. Should, however, the interpretation of the
#  GNU GPL Version 3 ("License") under any applicable laws result in
#  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
#  you the additional permission to use and propagate KNIME together with
#  ECLIPSE with only the license terms in place for ECLIPSE applying to
#  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
#  license terms of ECLIPSE themselves allow for the respective use and
#  propagation of ECLIPSE together with KNIME.
#
#  Additional permission relating to nodes for KNIME that extend the Node
#  Extension (and in particular that are based on subclasses of NodeModel,
#  NodeDialog, and NodeView) and that only interoperate with KNIME through
#  standard APIs ("Nodes"):
#  Nodes are deemed to be separate and independent programs and to not be
#  covered works.  Notwithstanding anything to the contrary in the
#  License, the License does not apply to Nodes, you are not required to
#  license Nodes under the License, and you are granted a license to
#  prepare and propagate Nodes, in each case even if such Nodes are
#  propagated with or for interoperation with KNIME.  The owner of a Node
#  may freely choose the license terms applicable to such Node, including
#  when such Node is propagated with or for interoperation with KNIME.
# ------------------------------------------------------------------------

"""
@author Jonas Klotz, KNIME GmbH, Berlin, Germany
"""
import os
from codecs import ignore_errors
from os import pardir
from typing import Type, Union
import unittest
import pandas as pd
import pyarrow as pa
import numpy as np

import knime._arrow._backend as ka
import knime.scripting._deprecated._arrow_table as kat
import knime_node_arrow_table as knat

import knime._arrow._pandas as kap
import knime._arrow._types as katy
import knime._arrow._backend as knar
import knime_types as kt

from testing_utility import (
    ArrowTestBackends,
    _generate_test_data_frame,
    _generate_test_table,
    _register_extension_types,
)


def _register_geospatial_value_factories():
    geo_module = "knime.types.geospatial"
    geo_valfac = "GeoValueFactory"

    kt.register_python_value_factory(
        geo_module,
        geo_valfac,
        '{"type": "struct", "inner_types": ["variable_width_binary", "string"]}',
        """
        {
            "type": "struct", 
            "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.geospatial.core.data.cell.GeoCell$ValueFactory\\"}" }, 
            "inner": [
                {"type": "simple", "traits": {}},
                {"type": "simple", "traits": {}}
            ]
        }
        """,
        "knime.types.geospatial.GeoValue",
    )
    kt.register_python_value_factory(
        geo_module,
        geo_valfac,
        '{"type": "struct", "inner_types": ["variable_width_binary", "string"]}',
        """
        {
            "type": "struct", 
            "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.geospatial.core.data.cell.GeoPointCell$ValueFactory\\"}" }, 
            "inner": [
                {"type": "simple", "traits": {}},
                {"type": "simple", "traits": {}}
            ]
        }
        """,
        "shapely.geometry.point.Point",
    )


def _register_geospatial_col_converters():
    import knime.api.types as katy

    python_module = "knime.types.geospatial"
    # to pandas
    python_class_name = "ToGeoPandasColumnConverter"
    python_value_type_names = [
        "org.knime.geospatial.core.data.cell.GeoCell$ValueFactory",
        "org.knime.geospatial.core.data.cell.GeoPointCell$ValueFactory",
        "org.knime.geospatial.core.data.cell.GeoLineCell$ValueFactory",
        "org.knime.geospatial.core.data.cell.GeoPolygonCell$ValueFactory",
        "org.knime.geospatial.core.data.cell.GeoMultiPointCell$ValueFactory",
        "org.knime.geospatial.core.data.cell.GeoMultiLineCell$ValueFactory",
        "org.knime.geospatial.core.data.cell.GeoMultiPolygonCell$ValueFactory",
        "org.knime.geospatial.core.data.cell.GeoCollectionCell$ValueFactory",
    ]
    for python_value_type_name in python_value_type_names:
        katy._to_pandas_column_converters[python_value_type_name] = (
            python_module,
            python_class_name,
        )

    # From Pandas
    python_class_name = "FromGeoPandasColumnConverter"
    python_value_type_names.append("geopandas.array.GeometryDtype")
    for python_value_type_name in python_value_type_names:
        katy._from_pandas_column_converters[python_value_type_name] = (
            python_module,
            python_class_name,
        )


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
                    this_file_path,
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

            _register_geospatial_value_factories()
            _register_geospatial_col_converters()
            _register_extension_types()
            # to register the arrow<->pandas column converters
            import knime.types.geospatial

            GeoSpatialExtensionTypeTest.geospatial_types_found = True
        except ImportError as e:
            raise unittest.SkipTest(
                """
                Skipping GeoSpatial tests because knime-geospatial could 
                not be found or 'geopandas' is not available
                """,
                e,
            )

    def _to_pandas(self, arrow):
        return kap.arrow_data_to_pandas_df(arrow)

    def test_load_table(self):
        t = _generate_test_table("geospatial_table_3.zip")
        self.assertEqual(["<Row ID>", "column1", "geometry"], t.schema.names)
        self.assertEqual([pa.string(), pa.string()], t.schema.types[0:2])
        self.assertIsInstance(t.schema.types[2], pa.ExtensionType)

    def test_load_df(self):
        t = _generate_test_table("geospatial_table_3.zip")
        df = self._to_pandas(t)
        from shapely.geometry import Point
        import geopandas

        use_geodf = True
        if use_geodf:
            df = geopandas.GeoDataFrame(df)
            crs = df.crs

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
            df.crs = crs

        out_t = kap.pandas_df_to_arrow(df)
        self.assertEqual(Point(30, 10), out_t[2][0].as_py().to_shapely())
        self.assertEqual(Point(12, 34), out_t[2][1].as_py().to_shapely())
        if use_geodf:
            self.assertEqual(Point(654, 23), out_t[2][2].as_py().to_shapely())

    def test_knime_node_table(self):
        from shapely.geometry import Point
        import geopandas as gpd

        with ArrowTestBackends() as test_backends:
            # load test table
            t = _generate_test_table(path="geospatial_table_3.zip")
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
            node_table = test_backends.arrow_backend.create_table_from_pandas(
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
            test_backends.deprecated_arrow_backend.write_table(gdf_copy)
            new_df = gdf_copy.reset_index(drop=True)
            self.assertEqual(original_df.crs, new_df.crs)
            self.assertCountEqual(original_df.columns, new_df.columns)
            self.assertCountEqual(original_df, new_df)

    def test_after_conversion_to_kntable(self):
        import geopandas as gpd

        with ArrowTestBackends() as test_backends:
            # load test table
            df = _generate_test_data_frame(
                file_name="geospatial_table_3.zip", columns=["column1", "geometry"]
            )
            gdf = gpd.GeoDataFrame(df)

            # test for new backend
            gdf_copy = gdf.copy(deep=True)
            test_backends.arrow_backend.create_table_from_pandas(
                gdf_copy, sentinel="min"
            )
            self.assertTrue(gdf.equals(gdf_copy))
            # test for old backend
            gdf_copy = gdf.copy(deep=True)
            test_backends.deprecated_arrow_backend.write_table(gdf_copy)
            self.assertTrue(gdf.equals(gdf_copy))

    def test_dict_decoding_geospatials(self):
        df = _generate_test_data_frame(
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

    def test_missing_values(self):
        import geopandas as gpd

        with ArrowTestBackends() as test_backends:
            # load test table
            df = _generate_test_data_frame(
                file_name="geospatial_table_3.zip", columns=["column1", "geometry"]
            )
            gdf = gpd.GeoDataFrame(df)
            crs = gdf.crs
            gdf.loc[len(gdf)] = ["Point(10 10)", np.nan]
            gdf.loc[len(gdf)] = ["Point(10 10)", pd.NA]
            gdf = gdf.reset_index(drop=True)
            gdf.crs = crs

            arrow_table = test_backends.deprecated_arrow_backend.write_table(gdf)
            schema = (
                "<RowID>: string\n"
                "column1: string\n"
                "geometry: extension<knime.logical_type<LogicalTypeExtensionType>>"
            )
            self.assertEqual(
                schema, arrow_table._schema.to_string(show_schema_metadata=False)
            )

    def test_setting_crs_with_other_ext_types(self):
        _register_extension_types()
        _register_geospatial_value_factories()
        _register_extension_types()

        columns = [
            "latitude",
            "longitude",
            "geometry",
            "tz_datetime",
            "date",
            "time",
            "datetime",
        ]
        # test if copying a dataframe with extension types works
        df = _generate_test_data_frame("GeoWithDateTime.zip", columns=columns)

        import geopandas as gpd

        crs = "EPSG:4326"

        gdf = gpd.GeoDataFrame(df)
        gdf = gdf.set_geometry("geometry")

        gdf.to_crs(crs, inplace=True)

        gdf = gdf.to_crs(crs, inplace=False)

        self.assertEqual(gdf.crs, crs)


if __name__ == "__main__":
    unittest.main()
