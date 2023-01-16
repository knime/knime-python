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

import pandas as pd
import pyarrow as pa

import knime._arrow._backend as ka
import knime._arrow._backend as knar
import knime._arrow._pandas as kap
import knime.scripting._deprecated._arrow_table as kat
import knime._arrow._types as katy
import knime_node_arrow_table as knat
import knime.api.types as kt


def _register_extension_types():
    ext_types = "knime.types.builtin"
    kt.register_python_value_factory(
        ext_types,
        "LocalTimeValueFactory",
        '"long"',
        """
                    {
                        "type": "simple",
                        "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.LocalTimeValueFactory\\"}" }
                    }
                    """,
        "datetime.time",
    )

    kt.register_python_value_factory(
        ext_types,
        "LocalDateValueFactory",
        '"long"',
        """
                    {
                        "type": "simple",
                        "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.LocalDateValueFactory\\"}" }
                    }
                    """,
        "datetime.date",
    )
    kt.register_python_value_factory(
        ext_types,
        "LocalDateTimeValueFactory",
        '{"type": "struct", "inner_types": ["long", "long"]}',
        """
                    {
                        "type": "struct",
                        "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.LocalDateTimeValueFactory\\"}" },
                        "inner": [
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}}
                        ]
                    }
                    """,
        "datetime.datetime",
    )
    kt.register_python_value_factory(
        ext_types,
        "ZonedDateTimeValueFactory2",
        '{"type": "struct", "inner_types": ["long", "long", "int", "string"]}',
        """
                    {
                        "type": "struct",
                        "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.ZonedDateTimeValueFactory2\\"}" },
                        "inner": [
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}}
                        ]
                    }
                    """,
        "datetime.datetime",
    )
    kt.register_python_value_factory(
        ext_types,
        "ZonedDateTimeValueFactory2",
        '{"type": "struct", "inner_types": ["long", "long", "int", "string"]}',
        """
                    {
                        "type": "struct",
                        "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.ZonedDateTimeValueFactory2\\"}" },
                        "inner": [
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}}
                        ]
                    }
                    """,
        "pandas._libs.tslibs.timestamps.Timestamp",
        is_default_python_representation=False,
    )
    kt.register_python_value_factory(
        ext_types,
        "DurationValueFactory",
        '{"type": "struct", "inner_types": ["long", "int"]}',
        """
                    {
                        "type": "struct",
                        "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.DurationValueFactory\\"}" },
                        "inner": [
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}}
                        ]
                    }
                    """,
        "datetime.timedelta",
    )
    kt.register_python_value_factory(
        ext_types,
        "DurationValueFactory",
        '{"type": "struct", "inner_types": ["long", "int"]}',
        """
                    {
                        "type": "struct",
                        "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.DurationValueFactory\\"}" },
                        "inner": [
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}}
                        ]
                    }
                    """,
        "pandas._libs.tslibs.timedeltas.Timedelta",
        is_default_python_representation=False,
    )
    kt.register_python_value_factory(
        ext_types,
        "FSLocationValueFactory",
        '{"type": "struct", "inner_types": ["string", "string", "string"]}',
        """
                    {
                        "type": "struct",
                        "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.filehandling.core.data.location.FSLocationValueFactory\\"}" },
                        "inner": [
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}}
                        ]
                    }
                    """,
        "knime.types.builtin.FSLocationValue",
    )

    kt.register_python_value_factory(
        "knime.types.builtin",
        "DenseByteVectorValueFactory",
        '"variable_width_binary"',
        """
                {
                    "type": "simple",
                    "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.value.DenseByteVectorValueFactory\\"}" }
                }
                """,
        "knime.types.builtin.DenseByteVectorValue",
    )
    kt.register_python_value_factory(
        "knime.types.builtin",
        "DenseBitVectorValueFactory",
        '"variable_width_binary"',
        """
                {
                    "type": "simple",
                    "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.value.DenseBitVectorValueFactory\\"}" }
                }
                """,
        "knime.types.builtin.DenseBitVectorValue",
    )


class TestDataSource:
    def __init__(self, absolute_path):
        self.absolute_path = absolute_path

    def getAbsolutePath(self):
        return self.absolute_path

    def isFooterWritten(self):
        return True

    def hasColumnNames(self):
        return False


class DummyJavaDataSink:
    def __init__(self) -> None:
        import os

        self._size = 0
        self._path = os.path.join(os.curdir, "test_data_sink")

    def getAbsolutePath(self):
        return self._path

    def reportBatchWritten(self, offset):
        pass

    def setColumnarSchema(self, schema):
        pass

    def setFinalSize(self, size):
        pass

    def write(self, data):
        pass


class DummyWriter:
    def write(self, data):
        pass

    def close(self):
        pass


class DummyConverter:
    def needs_conversion(self):
        return False

    def encode(self, storage):
        return storage

    def decode(self, storage):
        return storage


class DummyJavaDataSinkFactory:
    def __init__(self) -> None:
        self._sinks = []

    def __enter__(self):
        return self.create_data_sink

    def __exit__(self, *args):
        for sink in self._sinks:
            os.remove(sink)

    def create_data_sink(self) -> ka.ArrowDataSink:
        dummy_java_sink = DummyJavaDataSink()
        dummy_writer = DummyWriter()
        arrow_sink = ka.ArrowDataSink(dummy_java_sink)
        arrow_sink._writer = dummy_writer
        self._sinks.append(dummy_java_sink._path)
        return arrow_sink


def _create_dummy_arrow_sink():
    dummy_java_sink = DummyJavaDataSink()
    dummy_writer = DummyWriter()
    arrow_sink = ka.ArrowDataSink(dummy_java_sink)
    arrow_sink._writer = dummy_writer
    return arrow_sink


def _generate_test_table(path):
    """generates test pa.Table from filepath"""
    knime_generated_table_path = os.path.join(os.path.dirname(__file__), path)
    test_data_source = TestDataSource(knime_generated_table_path)
    pa_data_source = knar.ArrowDataSource(test_data_source)
    arrow = pa_data_source.to_arrow_table()
    arrow = katy.unwrap_primitive_arrays(arrow)

    return arrow


class ArrowTestBackends:
    def __init__(self):
        self.deprecated_arrow_backend = None
        self.arrow_backend = None

    def __enter__(self):
        dummy_java_sink = DummyJavaDataSink()
        self._sink_file = dummy_java_sink._path
        dummy_writer = DummyWriter()
        arrow_sink = ka.ArrowDataSink(dummy_java_sink)
        arrow_sink._writer = dummy_writer

        self.deprecated_arrow_backend = kat.ArrowBackend(DummyJavaDataSink)
        self.arrow_backend = knat._ArrowBackend(DummyJavaDataSink)
        return self

    def __exit__(self, *args):
        os.remove(self._sink_file)


def _generate_test_data_frame(
    file_name,
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
    arrow = _generate_arrow_table(file_name)

    df = kap.arrow_data_to_pandas_df(arrow)
    if columns is not None:
        df.columns = columns

    df = df[
        df.columns.drop(list(df.filter(regex="DoubleSetCol")))
    ]  # this column is buggy (DoubleSetColumns)
    if not lists:
        df = df[df.columns.drop(list(df.filter(regex="List")))]
    if not sets:
        df = df[df.columns.drop(list(df.filter(regex="Set")))]

    return df


def _generate_arrow_table(path) -> pa.Table:
    """Creates an Arrow Table from a KNIME table on disk

    @param path: path for the KNIME Table
    """
    knime_generated_table_path = os.path.join(os.path.dirname(__file__), path)
    test_data_source = TestDataSource(knime_generated_table_path)
    pa_data_source = knar.ArrowDataSource(test_data_source)
    arrow = pa_data_source.to_arrow_table()
    arrow = katy.unwrap_primitive_arrays(arrow)
    return arrow


def _apply_to_array(array, func):
    if isinstance(array, pa.ChunkedArray):
        return pa.chunked_array([func(chunk) for chunk in array.chunks])
    else:
        return func(array)
