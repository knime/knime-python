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
Provides the implementation of the KNIME Table using the Apache Arrow backend

@author Carsten Haubold, KNIME GmbH, Konstanz, Germany
"""

from types import LambdaType
from typing import Iterator, List, Optional, Tuple, Union
import knime_table as kta
import knime_arrow as ka
import knime_arrow_types as kat
import pyarrow as pa


class ArrowBatch(kta.Batch):
    def __init__(
        self,
        data: Union[pa.RecordBatch, "pandas.DataFrame"],
        sentinel: Optional[Union[str, int]] = None,
    ):
        """
        Create an arrow batch from the given data either in pyarrow.RecordBatch or
        pandas.DataFrame format.

        Arguments:
            data: 
                A pyarrow.RecordBatch or a pandas.DataFrame
            sentinel:
                None, "min", "max" or an int. If not None, values in integral columns that match the sentinel 
                will be interpreted as missing values.
        """

        if isinstance(data, pa.RecordBatch):
            self._batch = data
        else:
            import pandas as pd

            if isinstance(data, pd.DataFrame):
                import knime_arrow_pandas as kap

                self._batch = kap.pandas_df_to_arrow(data, to_batch=True)
            else:
                raise ValueError("Can only create an ArrowBatch with data")

        if sentinel is not None:
            self._batch = kat.sentinel_to_missing_value(self._batch, sentinel=sentinel)

        self._batch = kat.wrap_primitive_arrays(self._batch)

    def to_pandas(
        self,
        sentinel: Optional[Union[str, int]] = None,
        rows: Optional[slice] = None,
        columns: Optional[Union[List[int], slice, List[str]]] = None,
    ) -> "pandas.DataFrame":
        import knime_arrow_pandas as kap

        return kap.arrow_data_to_pandas_df(self.to_pyarrow(sentinel, rows, columns))

    def to_pyarrow(
        self,
        sentinel: Optional[Union[str, int]] = None,
        rows: Optional[slice] = None,
        columns: Optional[Union[List[int], slice, List[str]]] = None,
    ) -> pa.RecordBatch:
        batch = self._batch

        if columns is not None:
            batch = _select_columns(batch, columns)

        if rows is not None:
            batch = _select_rows(batch, rows)

        batch = kat.unwrap_primitive_arrays(batch)

        if sentinel is not None:
            batch = kat.insert_sentinel_for_missing_values(batch, sentinel)

        return batch

    @property
    def num_rows(self) -> int:
        return self._batch.num_rows

    @property
    def num_columns(self) -> int:
        return self._batch.num_columns

    @property
    def column_names(self) -> List[str]:
        return self._batch.schema.names

    def __str__(self) -> str:
        return str(self.to_arrow())

    def __repr__(self) -> str:
        return f"{__class__}(shape={self.shape}, schema={self._batch.schema}, data={str(self)})"


class ArrowReadTable(kta.ReadTable):
    def __init__(self, source: ka.ArrowDataSource):
        self._source = source

    def to_pandas(
        self,
        sentinel: Optional[Union[str, int]] = None,
        rows: Optional[slice] = None,
        columns: Optional[Union[List[int], slice, List[str]]] = None,
    ) -> "pandas.DataFrame":
        import knime_arrow_pandas as kap

        return kap.arrow_data_to_pandas_df(self.to_pyarrow(sentinel, rows, columns))

    def to_pyarrow(
        self,
        sentinel: Optional[Union[str, int]] = None,
        rows: Optional[slice] = None,
        columns: Optional[Union[List[int], slice, List[str]]] = None,
    ) -> pa.Table:
        table = self._source.to_arrow_table()

        if columns is not None:
            table = _select_columns(table, columns)

        if rows is not None:
            table = _select_rows(table, rows)

        table = kat.unwrap_primitive_arrays(table)

        if sentinel is not None:
            table = kat.insert_sentinel_for_missing_values(table, sentinel)

        return table

    @property
    def num_rows(self) -> int:
        return self._source.num_rows

    @property
    def num_columns(self) -> int:
        return len(self._source.schema)

    @property
    def num_batches(self) -> int:
        return len(self._source)

    @property
    def column_names(self) -> List[str]:
        return self._source.schema.names

    def batches(self) -> Iterator[ArrowBatch]:
        batch_idx = 0
        while batch_idx < len(self._source):
            yield ArrowBatch(self._source[batch_idx])
            batch_idx += 1

    def __str__(self) -> str:
        return str(self._source.to_arrow_table())

    def __repr__(self) -> str:
        return f"{__class__}(schema={self._source.schema}, data={str(self)})"


class _ArrowWriteTableImpl(kta.WriteTable):

    _MAX_NUM_BYTES_PER_BATCH = (
        1 << 64
    )  # same target batch size as in org.knime.core.columnar.cursor.ColumnarWriteCursor

    def __init__(self, sink):
        """
        Remember to close the sink, the ArrowWriteTable does not do this for you.
        """
        self._sink = sink
        self._last_batch = None
        self._num_batches = 0

    def _put_table(
        self,
        data: Union[pa.Table, "pandas.DataFrame"],
        sentinel: Optional[Union[str, int]] = None,
    ):
        if not isinstance(data, pa.Table):
            import knime_arrow_pandas as kap
            import pandas as pd

            if not isinstance(data, pd.DataFrame):
                raise ValueError(
                    "Can only fill WriteTable from pandas.DataFrame or pyarrow.Table"
                )
            data = kap.pandas_df_to_arrow(data)

        batches = self._split_table(data)
        self._num_batches += len(batches)
        for b in batches:
            if sentinel is not None:
                b = kat.sentinel_to_missing_value(b, sentinel)
            b = kat.wrap_primitive_arrays(b)
            self._sink.write(b)

    def _split_table(self, data: pa.Table):
        desired_num_batches = data.nbytes / self._MAX_NUM_BYTES_PER_BATCH
        num_rows_per_batch = int(len(data) // desired_num_batches)
        return data.to_batches(max_chunksize=num_rows_per_batch)

    @property
    def _schema(self):
        if self._last_batch is None:
            raise RuntimeError(
                "No batch has been written yet to the Table, cannot access schema"
            )

        return self._last_batch.schema

    def _append(self, batch: ArrowBatch):
        self._last_batch = batch._batch
        self._num_batches += 1
        self._sink.write(batch._batch)

    @property
    def num_rows(self) -> int:
        return self._sink._size

    @property
    def num_columns(self) -> int:
        return len(self._schema)

    @property
    def num_batches(self) -> int:
        return self._num_batches

    @property
    def column_names(self) -> List[str]:
        return self._schema.names

    def __repr__(self) -> str:
        schema = self._last_batch.schema if self._last_batch is not None else "Empty"
        return f"{__class__}(schema={schema}, num_batches={self.num_batches})"


class ArrowWriteTable(_ArrowWriteTableImpl):
    def __init__(
        self,
        sink,
        data: Union["pandas.DataFrame", pa.Table],
        sentinel: Optional[Union[str, int]] = None,
    ):
        if data is None:
            raise ValueError("Data cannot be 'None' when creating write table")
        super().__init__(sink)
        self._put_table(data, sentinel)


class ArrowBatchWriteTable(_ArrowWriteTableImpl):
    def __init__(self, sink):
        super().__init__(sink)

    def append(
        self,
        data: Union[ArrowBatch, "pandas.DataFrame", pa.RecordBatch],
        sentinel: Optional[Union[str, int]] = None,
    ):
        if isinstance(data, ArrowBatch):
            batch = data
        else:
            batch = ArrowBatch(data, sentinel)

        _ArrowWriteTableImpl._append(self, batch)


class ArrowBackend(kta._Backend):
    def __init__(self, sink_creator: LambdaType):
        """
        Create the Apache Arrow backend for KNIME Python tables.
        Automatically creates Java-backed sinks for WriteTables.
        
        Remember to call close() on this object to close all created sinks.

        Args:
            sink_creator: A lambda function without args that creates 
                          a Python data sink when called
        """
        self._sink_creator = sink_creator
        self._sinks = []

    def batch_write_table(self) -> ArrowBatchWriteTable:
        return ArrowBatchWriteTable(self._create_sink())

    def write_table(
        self,
        data: Union[ArrowReadTable, "pandas.DataFrame", pa.Table],
        sentinel: Optional[Union[str, int]] = None,
    ) -> ArrowWriteTable:
        if isinstance(data, ArrowReadTable):
            data = data.to_pyarrow()
        return ArrowWriteTable(self._create_sink(), data, sentinel)

    def batch(
        self,
        data: Union["pandas.DataFrame", pa.RecordBatch],
        sentinel: Optional[Union[str, int]] = None,
    ) -> ArrowBatch:
        return ArrowBatch(data, sentinel)

    def _create_sink(self):
        new_sink = self._sink_creator()
        self._sinks.append(new_sink)
        return new_sink

    def close(self):
        """Closes all sinks that were opened in this session"""
        for s in self._sinks:
            s.close()


def _select_rows(
    data: Union[pa.RecordBatch, pa.Table], selection
) -> Union[pa.RecordBatch, pa.Table]:
    if not isinstance(selection, slice):
        raise IndexError(
            f"Invalid row selection '{selection}' for '{data}', must be a slice object"
        )

    start, stop, stride = selection.indices(len(data))

    if stride == 1:
        return data.slice(offset=start, length=stop - start)
    else:
        return data.take(list(range(start, stop, stride)))


def _select_columns(
    data: Union[pa.RecordBatch, pa.Table], selection
) -> Union[pa.RecordBatch, pa.Table]:
    columns = []
    if isinstance(selection, slice):
        columns = list(range(*selection.indices(len(data.schema.names))))

    elif isinstance(selection, list):
        schema = data.schema
        for col in selection:
            if isinstance(col, str):
                try:
                    columns.append(schema.names.index(col))
                except ValueError:
                    raise IndexError(
                        f"Invalid column selection, '{col}' is not available in {schema.names}"
                    )
            elif isinstance(col, int):
                if not 0 <= col < len(schema.names):
                    raise IndexError(f"Column index {col} out of bounds")
                columns.append(col)
            else:
                raise IndexError(f"Invalid column index {col}")
    else:
        raise IndexError(f"Invalid column selection '{selection}' for '{data}'")

    if isinstance(data, pa.Table):
        return data.select(columns)
    else:
        arrays = []
        fields = []

        for c in columns:
            arrays.append(data.column(c))
            fields.append(data.schema.field(c))
        return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))
