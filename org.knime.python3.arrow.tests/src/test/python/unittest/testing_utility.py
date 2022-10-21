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

"""
@author Jonas Klotz, KNIME GmbH, Berlin, Germany
"""
import os

import pandas as pd
import pyarrow as pa

import knime_arrow as ka
import knime_arrow as knar
import knime_arrow_pandas as kap
import knime_arrow_table as kat
import knime_arrow_types as katy
import knime_node_arrow_table as knat


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
        import os

        os.remove(self._path)

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
            try:
                os.remove(sink)
            except FileNotFoundError:
                pass

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
    # returns a table with: RowKey, WKT (string) and GeoPoint columns
    knime_generated_table_path = path
    test_data_source = TestDataSource(knime_generated_table_path)
    pa_data_source = knar.ArrowDataSource(test_data_source)
    arrow = pa_data_source.to_arrow_table()
    arrow = katy.unwrap_primitive_arrays(arrow)

    return arrow


def _generate_backends():
    dummy_java_sink = DummyJavaDataSink()
    dummy_writer = DummyWriter()
    arrow_sink = ka.ArrowDataSink(dummy_java_sink)
    arrow_sink._writer = dummy_writer

    arrow_backend = kat.ArrowBackend(DummyJavaDataSink)
    node_arrow_backend = knat._ArrowBackend(DummyJavaDataSink)
    return arrow_backend, node_arrow_backend


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

    return df


def _generate_arrow_table(file_name) -> pa.Table:
    """Creates an Arrow Table from a KNIME table on disk

    @param path: path for the KNIME Table
    """
    knime_generated_table_path = os.path.normpath(
        os.path.join(__file__, "..", file_name)
    )
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