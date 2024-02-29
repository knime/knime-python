# -*- coding: utf-8 -*-
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
@author Adrian Nembach, KNIME GmbH, Konstanz, Germany
"""
import knime._arrow._types as kat
import knime._arrow._pandas as kap
import knime._backend._gateway as kg
import knime_types as kt
import pandas as pd
import pyarrow as pa
import numpy as np


class EntryPoint(kg.EntryPoint):
    def registerPythonValueFactory(
        self,
        python_module,
        python_value_factory_name,
        data_spec,
        data_traits,
        python_value_type_name,
        is_default_python_representation,
    ):
        kt.register_python_value_factory(
            python_module,
            python_value_factory_name,
            data_spec,
            data_traits,
            python_value_type_name,
            is_default_python_representation,
        )

    def assertFsLocationEquals(self, data_source, category, specifier, path):
        with kg.data_source_mapper(data_source) as source:
            batch = source[0]
            array = batch.column(0)
            # assert type(array) == flv.FSLocationArray, 'Wrong array type: ' + str(type(array))
            # support for ExtensionType Scalars was only recently and is not fully available even now
            v = array[0].as_py()
            expected = {
                "fs_category": category,
                "fs_specifier": specifier,
                "path": path,
            }
            assert v.to_dict() == expected, (
                "Wrong location dictionary. Expected "
                + str(expected)
                + " got "
                + str(v.to_dict())
            )

    def assertIntListEquals(self, data_source, a, b, c, d, e):
        with kg.data_source_mapper(data_source) as source:
            batch = source[0]
            array = batch.column(0)
            pd_array = array.to_pandas()
            py_array = array.to_pylist()
            values = [[a, b, c, d, e]]

            assert (
                py_array == values
            ), f"Wrong list of ints, expected '{values}' got '{py_array}'"
            assert (
                pd_array.shape == (1,)
            ), f"Wrong shape returned from pandas, expected '(1,)', got '{pd_array.shape}'"
            assert (
                type(pd_array[0]) == np.ndarray
            ), f"Wrong type returned from pandas, expected 'numpy.ndarray' got '{type(pd_array[0])}'"
            assert (
                len(pd_array[0]) == 5
            ), f"Wrong length of list returned from pandas, expected '5', got '{len(pd_array[0])}'"
            assert np.all(
                pd_array[0] == values[0]
            ), f"Wrong list of ints returned from pandas, expected '{values}' got '{pd_array[0]}'"

    def writeFsLocationViaPandas(self, data_sink, category, specifier, path):
        with kg.data_sink_mapper(data_sink) as sink:
            import knime.types.builtin as et

            fs_location = et.FsLocationValue(category, specifier, path)
            df = pd.DataFrame()
            df["fs_location"] = [fs_location]
            table = kap.pandas_df_to_arrow(df)
            sink.write(table)

    def writeFsLocationViaPyList(self, data_sink, category, specifier, path):
        with kg.data_sink_mapper(data_sink) as sink:
            import knime.types.builtin as et

            fs_location = et.FsLocationValue(category, specifier, path)
            sink.write(pa.table([fs_location], ["fs_location"]))

    def launchPythonTests(self):
        test_primitive_in_df()
        test_primitive_list_in_df()

    def copy(self, data_source, data_sink):
        with kg.data_source_mapper(data_source) as source:
            with kg.data_sink_mapper(data_sink) as sink:
                table = source.to_arrow_table()
                sink.write(table)

    def copyThroughPandas(self, data_source, data_sink):
        with kg.data_source_mapper(data_source) as source:
            with kg.data_sink_mapper(data_sink) as sink:
                df = source.to_pandas()
                arrow_table = kap.pandas_df_to_arrow(df)
                sink.write(arrow_table)

    class Java:
        implements = [
            "org.knime.python3.arrow.type.KnimeArrowExtensionTypesTest.KnimeArrowExtensionTypeEntryPoint"
        ]


kg.connect_to_knime(EntryPoint())


def test_primitive_in_df():
    df = pd.DataFrame()
    df["column"] = [1]
    arrow_table = kap.pandas_df_to_arrow(df)


def test_primitive_list_in_df():
    df = pd.DataFrame()
    df["column"] = [[1]]
    arrow_table = kap.pandas_df_to_arrow(df)
    field = arrow_table.schema.field(1)
    assert isinstance(field.type, pa.ListType)
    assert field.type.value_type == pa.int64()
    assert field.name == "column"
