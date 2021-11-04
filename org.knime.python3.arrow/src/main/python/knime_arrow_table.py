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

from typing import Iterator, List, Optional, Tuple, Union
import knime_table as kta
import knime_arrow as ka
import knime_arrow_types as kat
import pyarrow as pa


class ArrowBatch(kta.Batch):
    def __init__(self, *args, **kwargs):
        """
        Create an arrow batch from given data. Either "data_frame" or "record_batch" must be present.

        Arguments:
            data_frame: A pandas.DataFrame to use as data for this batch.
            record_batch: A pyarrow.RecordBatch containing the data for this batch
        """

        if "data_frame" in kwargs:
            df = kwargs["data_frame"]
            import knime_arrow_pandas as kap

            self._pandas_df = df
            self._batch = kap.pandas_df_to_arrow_batch(df)
        elif "record_batch" in kwargs:
            self._batch = kwargs["record_batch"]
        else:
            raise ValueError("Can only create an ArrowBatch with data")

    @staticmethod
    def from_pandas(data_frame: "pandas.DataFrame", *args, **kwargs) -> "ArrowBatch":
        return ArrowBatch(args, data_frame=data_frame, **kwargs)

    @staticmethod
    def from_pyarrow(
        record_batch: "pyarrow.RecordBatch", *args, **kwargs
    ) -> "ArrowBatch":
        return ArrowBatch(*args, record_batch=record_batch, **kwargs)

    def to_pandas(self, *args, **kwargs) -> "pandas.DataFrame":
        import knime_arrow_pandas as kap

        return kap.arrow_batch_to_pandas_df(self.to_pyarrow(*args, **kwargs))

    def to_pyarrow(
        self,
        rows: Optional[Union[int, Tuple[int, int]]] = None,
        columns: Optional[Union[List[int], Tuple[int, int], List[str]]] = None,
        sentinel: Optional[Union[str, int]] = None,
    ) -> "pyarrow.RecordBatch":
        batch = self._batch
        if sentinel is not None:
            batch = kat._replace_sentinels_in_batch(batch, sentinel)

        if columns is not None:
            batch = _select_columns(batch, columns)

        if rows is not None:
            batch = _select_rows(batch, rows)

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

    def to_pandas(self, *args, **kwargs) -> "pandas.DataFrame":
        import knime_arrow_pandas as kap

        return kap.arrow_table_to_pandas_df(self.to_pyarrow(*args, **kwargs))

    def to_pyarrow(
        self,
        rows: Optional[Union[int, Tuple[int, int]]] = None,
        columns: Optional[Union[List[int], Tuple[int, int], List[str]]] = None,
        sentinel: Optional[Union[str, int]] = None,
    ) -> "pyarrow.Table":
        table = self._source.to_arrow_table()
        if sentinel is not None:
            table = kat._replace_sentinels_in_table(table, sentinel)

        if columns is not None:
            table = _select_columns(table, columns)

        if rows is not None:
            table = _select_rows(table, rows)

        return table

    @property
    def num_rows(self) -> int:
        # ToDo: wait if we're still reading
        return self.to_pyarrow().num_rows

    @property
    def num_columns(self) -> int:
        return len(self._source.schema)

    @property
    def num_batches(self) -> int:
        # ToDo: wait if we're still reading?
        return len(self._source)

    @property
    def column_names(self) -> List[str]:
        return self._source.schema.names

    def batches(self) -> Iterator[ArrowBatch]:
        batch_idx = 0
        while batch_idx < len(self._source):
            yield ArrowBatch(df=None, record_batch=self._source[batch_idx])
            batch_idx += 1

    def __str__(self) -> str:
        return str(self._source.to_arrow_table())

    def __repr__(self) -> str:
        return f"{__class__}(schema={self._source.schema}, data={str(self)})"


class ArrowWriteTable(kta.WriteTable):
    def __init__(self, sink):
        """
        Remember to close the sink, the ArrowWriteTable does not do this for you.
        """
        self._sink = sink
        self._last_batch = None
        self._num_batches = 0

    def from_pandas(self, data: "pandas.DataFrame"):
        self._sink.write(data)

    def from_pyarrow(self, data: "pyarrow.Table"):
        self._sink.write(data)

    @property
    def _schema(self):
        if self._last_batch is None:
            raise RuntimeError(
                "No batch has been written yet to the Table, cannot access schema"
            )

        return self._last_batch.schema

    def append(self, batch: ArrowBatch):
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

    def __str__(self) -> str:
        if self._last_batch is None:
            return "ArrowWriteTable[empty]"

        return str(self._last_batch)

    def __repr__(self) -> str:
        schema = self._last_batch.schema if self._last_batch is not None else "Empty"
        return f"{__class__}(schema={schema}, data={str(self)})"


class ArrowBackend(kta._Backend):
    def create_batch_from_pandas(self, *args, **kwargs) -> ArrowBatch:
        return ArrowBatch.from_pandas(*args, **kwargs)

    def create_batch_from_pyarrow(self, *args, **kwargs) -> ArrowBatch:
        """Create a batch with pyarrow data Hands arguments to the backend"""
        return ArrowBatch.from_pyarrow(*args, **kwargs)


def _select_rows(
    data: Union[pa.RecordBatch, pa.Table], selection
) -> Union[pa.RecordBatch, pa.Table]:
    if isinstance(selection, int):
        return data.slice(offset=0, length=selection)
    elif isinstance(selection, tuple) and len(selection) == 2:
        start = 0 if selection[0] is None else selection[0]
        end = len(data) if selection[1] is None else selection[1]
        return data.slice(offset=start, length=end - start)
    else:
        raise IndexError(f"Invalid row selection '{selection}' for '{data}'")


def _select_columns(
    data: Union[pa.RecordBatch, pa.Table], selection
) -> Union[pa.RecordBatch, pa.Table]:
    columns = []
    if isinstance(selection, tuple) and len(selection) == 2:
        start = 0 if selection[0] is None else selection[0]
        end = len(data.schema.names) if selection[1] is None else selection[1]

        if not 0 <= start < len(data.schema.names):
            raise IndexError(f"Column index {start} out of bounds")

        if not 0 <= end <= len(data.schema.names):
            raise IndexError(f"Column index {end} out of bounds")

        columns = list(range(start, end))
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
