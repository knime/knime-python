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
Arrow backend for tables used in pure-Python nodes

@author Carsten Haubold, KNIME GmbH, Konstanz, Germany
"""

from typing import Iterator, List, Optional, Union
import pyarrow as pa
import logging

import knime_arrow
import knime_arrow_types as katy
import knime_arrow_struct_dict_encoding as kas
import knime_schema as ks
import knime_node_table as knt

LOGGER = logging.getLogger(__name__)


class _ArrowBackend(knt._Backend):
    def __init__(self, sink_factory):
        self._sink_factory = sink_factory
        self._sinks = []

    def create_table_from_pyarrow(self, data, sentinel):
        if sentinel is not None:
            data = katy.sentinel_to_missing_value(data, sentinel)
        data = katy.wrap_primitive_arrays(data)
        return ArrowTable(data)

    def create_table_from_pandas(self, data, sentinel):
        import knime_arrow_pandas as kap
        import pandas as pd

        if not isinstance(data, pd.DataFrame):
            raise ValueError(
                f"Table.from_pandas expects a pandas.DataFrame, but got {type(data)}"
            )
        data = kap.pandas_df_to_arrow(data)
        return self.create_table_from_pyarrow(data, sentinel)

    def create_batch_output_table(self):
        return ArrowBatchOutputTable(self.create_sink())

    def create_sink(self):
        sink = self._sink_factory()
        self._sinks.append(sink)
        return sink

    def close(self):
        """Closes all sinks that were opened in this session"""
        for s in self._sinks:
            s.close()


class ArrowBatchOutputTable(knt.BatchOutputTable):
    def __init__(self, sink):
        self._num_batches = 0
        self._sink = sink

    def append(self, batch: "ArrowTable"):
        self._num_batches += 1
        self._sink.write(batch._table)

    @property
    def num_batches(self):
        return self._num_batches


class ArrowTable(knt.Table):
    def __init__(self, table):
        self._table = table

    def to_pandas(
        self,
        sentinel: Optional[Union[str, int]] = None,
    ) -> "pandas.DataFrame":
        import knime_arrow_pandas as kap

        return kap.arrow_data_to_pandas_df(self.to_pyarrow(sentinel))

    def to_pyarrow(self, sentinel: Optional[Union[str, int]] = None) -> pa.Table:
        table = katy.unwrap_primitive_arrays(self._get_table())

        if sentinel is not None:
            table = katy.insert_sentinel_for_missing_values(table, sentinel)

        return table

    def _get_table(self) -> pa.Table:
        return self._table

    def _select_rows(self, selection) -> "ArrowTable":
        import knime_arrow_table as kat

        return ArrowTable(kat._select_rows(self._get_table(), selection))

    def _select_columns(self, selection) -> "ArrowTable":
        import knime_arrow_table as kat

        return ArrowTable(
            kat._select_columns(self._get_table(), selection, auto_include_row_key=True)
        )

    def _append(self, other: "ArrowTable") -> "ArrowTable":
        # FIXME exclude row key from second table - AP-19077
        # a = self._get_table()
        # b = other._get_table()
        # appended = pa.Table.from_arrays(
        #     a.columns + b.columns, names=a.schema.names + b.schema.names
        # )
        # return ArrowTable(appended)

        raise NotImplementedError(
            "append/insert for Arrow tables is not yet implemented"
        )

    @property
    def num_rows(self) -> int:
        return len(self._table)

    @property
    def num_columns(self) -> int:
        # NOTE: We don't count the row key column
        return len(self._table.schema) - 1

    @property
    def column_names(self) -> List[str]:
        # NOTE: We don't include the row key column
        return self._table.schema.names[1:]

    @property
    def schema(self) -> ks.Schema:
        return _convert_arrow_schema_to_knime(self._get_table().schema)

    def __str__(self):
        return f"ArrowTable[shape=({self.num_columns}, {self.num_rows})]"

    def _write_to_sink(self, sink):
        """
        Used at the end of execute() to return the data to KNIME
        """
        batches = self._split_table(self._get_table())
        for b in batches:
            sink.write(b)

    _MAX_NUM_BYTES_PER_BATCH = (
        1 << 26
    )  # same target batch size as in org.knime.core.columnar.cursor.ColumnarWriteCursor

    def _split_table(self, data: pa.Table):
        """
        Split a table into batches of KNIMEs desired batch size.
        """
        desired_num_batches = data.nbytes / self._MAX_NUM_BYTES_PER_BATCH
        if desired_num_batches < 1:
            return data.to_batches()
        num_rows_per_batch = int(len(data) // desired_num_batches)
        return data.to_batches(max_chunksize=num_rows_per_batch)


class ArrowSourceTable(ArrowTable):
    def __init__(self, source: "knime_arrow.ArrowDataSource"):
        self._source = source

    def _get_table(self):
        return self._source.to_arrow_table()

    @property
    def num_rows(self) -> int:
        return self._source.num_rows

    @property
    def num_columns(self) -> int:
        # NOTE: We don't count the row key column
        return len(self._source.schema) - 1

    @property
    def column_names(self) -> List[str]:
        # NOTE: We don't include the row key column
        return self._source.schema.names[1:]

    @property
    def schema(self) -> ks.Schema:
        return _convert_arrow_schema_to_knime(self._source.schema)

    def __str__(self):
        return f"ArrowSourceTable[cols={self.num_columns}, rows={self.num_rows}, batches={self.num_batches}]"

    @property
    def num_batches(self) -> int:
        return len(self._source)

    def to_batches(self) -> Iterator[knt.Table]:
        """
        Returns an generator for the batches in this table. If the generator is advanced to a batch
        that is not available yet, it will block until the data is present.

        **Example**::

            processed_table = BatchOutputTable.create()
            for batch in my_table.to_batches():
                input_batch = batch.to_pandas()
                # process the batch
                processed_table.append(Table.from_pandas(input_batch))
        """
        batch_idx = 0
        while batch_idx < len(self._source):
            yield ArrowTable(self._source[batch_idx])
            batch_idx += 1


def _convert_arrow_schema_to_knime(schema: pa.Schema) -> ks.Schema:
    # TODO the metadata is always "{}"
    types, names, metadata = zip(
        *[
            (
                _convert_arrow_type_to_knime(schema[i].type),
                schema.names[i],
                schema[i].metadata,
            )
            for i in range(1, len(schema))
        ]
    )

    return ks.Schema(types, names, metadata)


_arrow_to_knime_types = {
    pa.int32(): ks.int32(),
    pa.int64(): ks.int64(),
    pa.string(): ks.string(),
    pa.bool_(): ks.bool_(),
    pa.float64(): ks.double(),
    pa.large_binary(): ks.blob()
    # pa.null(): ks.void(), ?
}


def _convert_arrow_type_to_knime(dtype: pa.DataType) -> ks.KnimeType:
    if katy.is_dict_encoded_value_factory_type(dtype):
        return ks.LogicalType(
            "structDictEncodedValueFactory",
            _convert_arrow_type_to_knime(dtype.value_type),
        )
    elif kas.is_struct_dict_encoded(dtype):
        return ks.LogicalType(
            "structDictEncoded", _convert_arrow_type_to_knime(dtype.value_type)
        )
    elif katy.is_value_factory_type(dtype):
        if katy._is_knime_primitive_type(dtype):
            return _convert_arrow_type_to_knime(dtype.storage_type)
        return ks.LogicalType(
            dtype.logical_type, _convert_arrow_type_to_knime(dtype.storage_type)
        )
    elif dtype in _arrow_to_knime_types:
        return _arrow_to_knime_types[dtype]
    elif isinstance(dtype, pa.ListType) or isinstance(dtype, pa.LargeListType):
        return ks.list_(_convert_arrow_type_to_knime(dtype.value_type))
    elif isinstance(dtype, pa.StructType):
        return ks.StructType(
            [_convert_arrow_type_to_knime(field.type) for field in dtype]
        )
    else:
        raise TypeError(f"Cannot convert PyArrow type {dtype} to KNIME type")
