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
Arrow backend for tables used in pure-Python nodes and Python scripting nodes

@author Carsten Haubold, KNIME GmbH, Konstanz, Germany
"""

from typing import Iterator, List, Optional, Union
import pyarrow as pa
import logging

import knime._arrow._backend as _backend
import knime._arrow._types as katy
import knime._arrow._dictencoding as kas
import knime.api.schema as ks
import knime.api.table as knt

LOGGER = logging.getLogger(__name__)


def _create_table_from_pyarrow(data, sentinel, row_ids="auto", first_row_id=0):
    # TODO(AP-20353) remove this check when we support variable sized batches
    _check_batch_sizes_constant(data)

    # Handle RowID
    if row_ids == "auto":
        rk_field = data.schema[0]
        if rk_field.name == "<RowID>" and pa.types.is_string(rk_field.type):
            # Keep the RowID column
            pass
        elif rk_field.name == "<RowID>" and pa.types.is_integer(rk_field.type):
            # Convert to string with the format f"Row{n}"
            data = _replace_with_formatted_row_ids(data)
        else:
            if rk_field.name == "<RowID>":
                # There is a column named <RowID> but we can't use it
                LOGGER.warning(
                    'The first column is named "<RowID>" but has the type '
                    f'"{rk_field.type}" which cannot be used as RowID. '
                    "Keeping the column and generating new RowIDs."
                )
            # No RowID column that can be used -> generate new IDs
            data = _add_generated_row_ids(data, first_row_id)
    elif row_ids == "generate":
        data = _add_generated_row_ids(data, first_row_id)
    elif row_ids == "keep":
        # Nothing to do
        pass
    else:
        raise ValueError('row_ids must be one of ["auto", "generate", "keep"]')

    if sentinel is not None:
        data = katy.sentinel_to_missing_value(data, sentinel)
    data = katy.wrap_primitive_arrays(data)

    return ArrowTable(data)


def _check_batch_sizes_constant(data: Union[pa.Table, pa.RecordBatch]):
    """If the data is a pyarrow Table, check that all batches have the same size"""
    if isinstance(data, pa.Table) and len(data) > 0:
        batch_sizes = [len(rb) for rb in data.to_batches()]
        chunk_size = batch_sizes[0]

        # Check all batches except the last one
        for idx, size in enumerate(batch_sizes[:-1]):
            if chunk_size != size:
                raise ValueError(
                    "all batches of the table must have the same size, "
                    f"but batch {idx} has size {size} (expected: {chunk_size})"
                )

        # Check the last batch
        if chunk_size < batch_sizes[-1]:
            raise ValueError(
                f"last batch has size {batch_sizes[-1]}, "
                f"but must not be bigger than the other batches of size {chunk_size}"
            )


def _create_table_from_pandas(data, sentinel, row_ids="auto", first_row_id=0):
    import knime._arrow._pandas as kap
    import pandas as pd

    if not isinstance(data, pd.DataFrame):
        raise ValueError(
            f"Table.from_pandas expects a pandas.DataFrame, but got {type(data)}"
        )
    if row_ids == "auto" and data.shape == (0, 0):
        # ignored by kap.pandas_df_to_arrow because the DataFrame is empty
        pandas_row_ids = row_ids
        arrow_row_ids = "generate"
    elif row_ids in ["auto", "keep"]:
        # The "<RowID>" column is added by the kap.pandas_df_to_arrow function from
        # the DataFrame index
        pandas_row_ids = row_ids
        # We use the "<RowID>" when calling _create_table_from_pyarrow
        arrow_row_ids = "keep"
    elif row_ids == "generate":
        # The kap.pandas_df_to_arrow should not create a "<RowID>" column from the
        # DataFrame index
        pandas_row_ids = "none"
        # We generate new RowIDs when calling _create_table_from_pyarrow
        arrow_row_ids = "generate"
    else:
        raise ValueError('row_ids must be one of ["auto", "keep", "generate"]')
    data = kap.pandas_df_to_arrow(data, row_ids=pandas_row_ids)
    return _create_table_from_pyarrow(
        data, sentinel, row_ids=arrow_row_ids, first_row_id=first_row_id
    )


class _ArrowBackend(knt._Backend):
    def __init__(self, sink_factory):
        self._sink_factory = sink_factory
        self._sinks = []

    def create_table_from_pyarrow(self, data, sentinel, row_ids="auto"):
        return _create_table_from_pyarrow(data, sentinel, row_ids)

    def create_table_from_pandas(self, data, sentinel, row_ids="auto"):
        return _create_table_from_pandas(data, sentinel, row_ids)

    def create_batch_output_table(self, row_ids="keep"):
        return ArrowBatchOutputTable(self.create_sink(), row_ids=row_ids)

    def create_sink(self):
        sink = self._sink_factory()
        self._sinks.append(sink)
        return sink

    def close(self):
        """Closes all sinks that were opened in this session"""
        for s in self._sinks:
            s.close()
        self._sinks = []


class ArrowBatchOutputTable(knt.BatchOutputTable):
    def __init__(self, sink, row_ids="keep"):
        self._num_batches = 0
        self._sink = sink

        if row_ids not in ["keep", "generate"]:
            raise ValueError('row_ids must be one of ["keep", "generate"]')
        self._row_ids = row_ids
        self._num_rows = 0

    def append(
        self, batch: Union["ArrowTable", "pandas.DataFrame", pa.Table, pa.RecordBatch]
    ):
        if isinstance(batch, pa.Table) or isinstance(batch, pa.RecordBatch):
            batch = _create_table_from_pyarrow(
                batch,
                sentinel=None,
                row_ids=self._row_ids,
                first_row_id=self._num_rows,
            )
        elif isinstance(batch, ArrowTable):
            if self._row_ids == "generate":
                # Remove the RowIDs from the table and generate new ones
                batch = _create_table_from_pyarrow(
                    pa.table(
                        batch._table.columns[1:], schema=batch._table.schema.remove(0)
                    ),
                    sentinel=None,
                    row_ids="generate",
                    first_row_id=self._num_rows,
                )
            # else: no need to convert
        else:
            import pandas as pd

            if not isinstance(batch, pd.DataFrame):
                raise TypeError(
                    f"Can only append batches of type knime.api.Table, pyarrow.Table, pyarrow.RecordBatch, pandas.DataFrame but got {type(batch)}"
                )
            batch = _create_table_from_pandas(
                batch,
                sentinel=None,
                row_ids=self._row_ids,
                first_row_id=self._num_rows,
            )

        self._num_rows += batch.num_rows
        self._num_batches += 1
        self._sink.write(batch._table)

    @property
    def num_batches(self):
        return self._num_batches

    def _write_empty_batch(self) -> None:
        empty_table = _backend.create_empty_table(pa.schema([("<RowID>", pa.string())]))
        self.append(empty_table)


class ArrowTable(knt.Table):
    def __init__(self, table: Union[pa.Table, pa.RecordBatch]):
        self._table = table

    def to_pandas(
        self,
        sentinel: Optional[Union[str, int]] = None,
    ) -> "pandas.DataFrame":
        import knime._arrow._pandas as kap

        return kap.arrow_data_to_pandas_df(self.to_pyarrow(sentinel))

    def to_pyarrow(self, sentinel: Optional[Union[str, int]] = None) -> pa.Table:
        table = katy.unwrap_primitive_arrays(self._get_table())

        if sentinel is not None:
            table = katy.insert_sentinel_for_missing_values(table, sentinel)

        return table

    def _get_table(self) -> pa.Table:
        return self._table

    def _select_rows(self, selection) -> "ArrowTable":
        import knime.scripting._deprecated._arrow_table as kat

        return ArrowTable(kat._select_rows(self._get_table(), selection))

    def _select_columns(self, selection) -> "ArrowTable":
        import knime.scripting._deprecated._arrow_table as kat

        return ArrowTable(
            kat._select_columns(self._get_table(), selection, auto_include_row_key=True)
        )

    def _append(self, other: "ArrowTable") -> "ArrowTable":
        # FIXME exclude RowID from second table - AP-19077
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
        # NOTE: We don't count the RowID column
        return len(self._table.schema) - 1

    @property
    def column_names(self) -> List[str]:
        # NOTE: We don't include the RowID column
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
        data = self._get_table()

        if isinstance(data, pa.RecordBatch):
            batches = [data]
        else:
            batches = self._split_table(data)

        for batch in batches:
            sink.write(batch)

    _MAX_NUM_BYTES_PER_BATCH = (
        1 << 26
    )  # same target batch size as in org.knime.core.columnar.cursor.ColumnarWriteCursor

    def _split_table(self, data: pa.Table) -> List[pa.RecordBatch]:
        """
        Split a table into batches of KNIMEs desired batch size.
        """
        if len(data) == 0:
            # Return data so that we write the schema even if no rows are present
            return [data]

        desired_num_batches = data.nbytes / self._MAX_NUM_BYTES_PER_BATCH
        if desired_num_batches < 1:
            return data.to_batches()
        num_rows_per_batch = int(len(data) // desired_num_batches)
        return data.to_batches(max_chunksize=num_rows_per_batch)


class ArrowSourceTable(ArrowTable):
    def __init__(self, source: "_backend.ArrowDataSource"):
        self._source = source
        self._schema = _convert_arrow_schema_to_knime(self._source.schema)

    def _get_table(self):
        return self._source.to_arrow_table()

    @property
    def num_rows(self) -> int:
        return self._source.num_rows

    @property
    def num_columns(self) -> int:
        # NOTE: We don't count the RowID column
        return len(self._source.schema) - 1

    @property
    def column_names(self) -> List[str]:
        # NOTE: We don't include the RowID column
        return self._source.schema.names[1:]

    @property
    def schema(self) -> ks.Schema:
        return self._schema

    def __str__(self):
        return f"ArrowSourceTable[cols={self.num_columns}, rows={self.num_rows}, batches={self.num_batches}]"

    @property
    def num_batches(self) -> int:
        return len(self._source)

    def batches(self) -> Iterator[knt.Table]:
        """
        Returns a generator for the batches in this table. If the generator is advanced to a batch
        that is not available yet, it will block until the data is present.

        **Example**::

            processed_table = BatchOutputTable.create()
            for batch in my_table.batches():
                input_batch = batch.to_pandas()
                # process the batch
                processed_table.append(Table.from_pandas(input_batch))
        """
        batch_idx = 0
        while batch_idx < len(self._source):
            yield ArrowTable(self._source[batch_idx])
            batch_idx += 1

    def _inject_metadata(self, metadata_provider):
        """
        We allow KNIME to inject additional metadata, which we only use when executing pure-Python nodes
        for now. This additional metadata can e.g. be the preferred_value_type which is required to make
        type filtering in the column filter work -- which is only needed for nodes, not for scripts.

        WARNING: if the metadata contained values with the same keys, they will be overwritten.
        """
        import json

        schema_string = json.dumps(self._schema.serialize())
        metadatas = json.loads(metadata_provider(schema_string))

        for column, metadata in zip(self._schema, metadatas):
            if not isinstance(metadata, dict):
                raise TypeError(
                    f"The injected column metadata should be a dict, but is {metadata}"
                )

            if column.metadata is None:
                column.metadata = {}
            column.metadata.update(metadata)


def _convert_arrow_schema_to_knime(schema: pa.Schema) -> ks.Schema:
    # TODO the metadata is always "{}"
    if len(schema) <= 1:
        return ks.Schema.from_columns([])

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
    pa.large_binary(): ks.blob(),
    pa.null(): ks.null(),
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
        if dtype.logical_type == katy._arrow_to_knime_list_type:
            return _convert_arrow_type_to_knime(dtype.storage_type)
        if dtype.logical_type == katy._arrow_to_knime_primitive_types[pa.null()]:
            return ks.null()
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
    elif not isinstance(dtype, pa.ExtensionType) and (
        dtype in katy._arrow_to_knime_datetime_types
        or isinstance(dtype, pa.TimestampType)
    ):
        logical_type, storage_type, _ = katy.parse_datetime_type(dtype)
        return ks.LogicalType(logical_type, storage_type=storage_type)
    else:
        raise TypeError(f"Cannot convert PyArrow type {dtype} to KNIME type")


def _add_formatted_row_ids(
    columns: List[pa.Array], schema: pa.Schema, row_ids
) -> pa.Table:
    row_ids = pa.array(map("Row{}".format, row_ids))
    return pa.table(
        [row_ids, *columns],
        schema=schema.insert(0, pa.field("<RowID>", pa.string())),
    )


def _add_generated_row_ids(data: Union[pa.Table, pa.RecordBatch], start_idx=0):
    return _add_formatted_row_ids(
        data.columns,
        data.schema,
        range(start_idx, start_idx + len(data)),
    )


def _replace_with_formatted_row_ids(data: Union[pa.Table, pa.RecordBatch]) -> pa.Table:
    return _add_formatted_row_ids(
        data.columns[1:],
        data.schema.remove(0),
        data.columns[0],
    )
