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
import knime_arrow_types as kat
import knime_gateway as kg
import knime_types as kt
import pandas as pd
import pyarrow as pa


class EntryPoint(kg.EntryPoint):

    def registerPythonValueFactory(self, python_module, python_value_factory_name, data_spec, java_value_factory,
                                   data_traits):
        kt.register_python_value_factory(python_module, python_value_factory_name, data_spec, java_value_factory,
                                         data_traits)

    def assertFsLocationEquals(self, data_source, category, specifier, path):
        with kg.data_source_mapper(data_source) as source:
            batch = source[0]
            array = batch.column(0)
            # assert type(array) == flv.FSLocationArray, 'Wrong array type: ' + str(type(array))
            # support for ExtensionType Scalars was only recently and is not fully available even now
            v = array[0].as_py()
            expected = {'fs_category': category, 'fs_specifier': specifier, 'path': path}
            assert v.to_dict() == expected, 'Wrong location dictionary. Expected ' + str(expected) + ' got ' + str(
                v.to_dict())

    def assertUtf8EncodedStringEquals(self, data_source, value):
        with kg.data_source_mapper(data_source) as source:
            batch = source[0]
            array = batch.column(0)
            pd_array = array.to_pandas()
            py_array = array.to_pylist()
            v = pd_array[0]
            assert v.value == value, "Wrong UTF8EncodedString: Expected '" + str(value) + "' got '" + str(v.value) + "'"
            py_value = py_array[0].value
            assert py_value == value, "Wrong UTF8EncodedString returned by to_pylist. Expected '" + str(
                value) + "' got '" + str(py_value) + "'"

    def writeUtf8EncodedStringViaPandas(self, data_sink, value):
        with kg.data_sink_mapper(data_sink) as sink:
            import utf8_string
            utf8_string = utf8_string.Utf8EncodedString(value)
            df = pd.DataFrame()
            df['utf8_encoded_string'] = [utf8_string]
            table = kat.pandas_df_to_arrow_table(df)
            sink.write(table)

    def writeUtf8EncodedStringViaPyList(self, data_sink, value):
        with kg.data_sink_mapper(data_sink) as sink:
            import utf8_string
            extension_array = kat.knime_extension_array([utf8_string.Utf8EncodedString(value)])
            sink.write(pa.table([extension_array], ['utf8_encoded_string']))

    def writeFsLocationViaPandas(self, data_sink, category, specifier, path):
        with kg.data_sink_mapper(data_sink) as sink:
            import extension_types as et
            fs_location = et.FsLocationValue(category, specifier, path)
            df = pd.DataFrame()
            df['fs_location'] = [fs_location]
            table = kat.pandas_df_to_arrow_table(df)
            sink.write(table)

    def writeFsLocationViaPyList(self, data_sink, category, specifier, path):
        with kg.data_sink_mapper(data_sink) as sink:
            import extension_types as et
            fs_location = et.FsLocationValue(category, specifier, path)
            extension_array = kat.knime_extension_array([fs_location])
            sink.write(pa.table([extension_array], ['fs_location']))

    def launchPythonTests(self):
        test_primitive_in_df()
        test_primitive_list_in_df()
        test_list_of_ext_type_in_df()
        test_ext_type_in_df()

    def copy(self, data_source, data_sink):
        with kg.data_source_mapper(data_source) as source:
            with kg.data_sink_mapper(data_sink) as sink:
                sink.write(source.to_arrow_table())

    def copyThroughPandas(self, data_source, data_sink):
        with kg.data_source_mapper(data_source) as source:
            with kg.data_sink_mapper(data_sink) as sink:
                df = source.to_pandas()
                arrow_table = kat.pandas_df_to_arrow_table(df)
                sink.write(arrow_table)

    class Java:
        implements = ["org.knime.python3.arrow.type.KnimeArrowExtensionTypesTest.KnimeArrowExtensionTypeEntryPoint"]


kg.connect_to_knime(EntryPoint())


def test_primitive_in_df():
    df = pd.DataFrame()
    df['column'] = [1]
    arrow_table = kat.pandas_df_to_arrow_table(df)


def test_primitive_list_in_df():
    df = pd.DataFrame()
    df['column'] = [[1]]
    arrow_table = kat.pandas_df_to_arrow_table(df)
    field = arrow_table.schema.field(0)
    assert field.type == pa.list_(pa.int32())
    assert field.name == 'column'


def test_list_of_ext_type_in_df():
    import utf8_string
    df = pd.DataFrame()
    df['column'] = [[utf8_string.Utf8EncodedString('foobar')]]
    arrow_table = kat.pandas_df_to_arrow_table(df)
    field = arrow_table.schema.field(0)
    assert isinstance(field.type.value_type, kat.ValueFactoryExtensionType)
    assert field.name == 'column'
    pylist = arrow_table[0].to_pylist()
    assert pylist[0][0].value == 'foobar'


def test_ext_type_in_df():
    import utf8_string
    df = pd.DataFrame()
    df['column'] = [utf8_string.Utf8EncodedString('barfoo')]
    arrow_table = kat.pandas_df_to_arrow_table(df)
    field = arrow_table.schema.field(0)
    assert isinstance(field.type, kat.ValueFactoryExtensionType)
    assert arrow_table[0].to_pylist()[0].value == 'barfoo'
