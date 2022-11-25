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
Defines the general table object that is used to transfer data from and to KNIME.

Python Scripting nodes using the import 'knime.scripting.io' as well as pure-Python
nodes will use the table API defined in this file here.

Note that this table API is the successor to the table defined in the deprecated module 'knime.scripting._deprecated._table',
which only remains available in Python Scripting nodes that import the deprecated module 'knime_io'.
"""

from abc import abstractmethod, abstractproperty

from typing import Iterator, List, Optional, Tuple, Union
import logging

import knime.api.schema as ks

LOGGER = logging.getLogger(__name__)

# --------------------------------------------------------------
# _Tabular interface, _TabularView and Operations


class _Tabular(ks._Columnar):
    """
    Common interface for Table and _TableView
    """

    @abstractproperty
    def num_rows(self):
        pass

    @property
    def shape(self) -> Tuple[int, int]:
        return (self.num_columns, self.num_rows)

    def __getitem__(
        self,
        slicing: Union[
            Union[slice, List[int], List[str]],
            Tuple[Union[slice, List[int], List[str]], slice],
        ],
    ) -> "_TabularView":
        """
        Creates a view of this Table by slicing rows and columns. The slicing syntax is similar to that of numpy arrays,
        but columns can also be addressed as index lists or via a list of column names.

        The syntax is `[column_slice, row_slice]`. Note that this is the exact opposite order than in the deprecated scripting
        API's ReadTable.

        Args:
            column_slice:
                A column index, a column name, a slice object, a list of column indices, or a list of column names.
            row_slice:
                Optional: A slice object describing which rows to use.

        Returns:
            A _TabularView representing a slice of the original Table

        **Example**::

            row_sliced_table = table[:, :100] # Get the first 100 rows
            column_sliced_table = table[["name", "age"]] # Get all rows of the columns "name" and "age"
            row_and_column_sliced_table = table[1:5, :100] # Get the first 100 rows of columns 1,2,3,4

        """
        # we need to return IndexError already here if slicing is an int to end for-loops,
        # otherwise `for i in table` will run forever
        if isinstance(slicing, int) and slicing >= self.num_columns:
            raise IndexError(
                f"Index {slicing} is too large for {self.__class__.__name__} with {self.num_columns} columns"
            )

        if isinstance(slicing, tuple):
            column_sliced = _TabularView(self, ks._ColumnSlicingOperation(slicing[0]))
            return _TabularView(column_sliced, _RowSlicingOperation(slicing[1]))
        else:
            return _TabularView(self, ks._ColumnSlicingOperation(slicing))

    def to_pandas(
        self, sentinel: Optional[Union[str, int]] = None
    ) -> "pandas.DataFrame":
        """
        Access this table as a pandas.DataFrame.

        Args:
            sentinel:
                Replace missing values in integral columns by the given value, one of:

                * ``"min"`` min int32 or min int64 depending on the type of the column
                * ``"max"`` max int32 or max int64 depending on the type of the column
                * An integer value that should be inserted for each missing value
        """
        return self.get().to_pandas()

    def to_pyarrow(self, sentinel: Optional[Union[str, int]] = None) -> "pyarrow.Table":
        """
        Access this table as a pyarrow.Table.

        Args:
            sentinel:
                Replace missing values in integral columns by the given value, one of:

                * ``"min"`` min int32 or min int64 depending on the type of the column
                * ``"max"`` max int32 or max int64 depending on the type of the column
                * An integer value that should be inserted for each missing value
        """
        return self.get().to_pyarrow()

    @abstractmethod
    def _select_rows(self, selection):
        """Implement row slicing here"""
        pass

    @abstractproperty
    def schema(self) -> ks.Schema:
        """
        The schema of this table, containing column names, types, and potentially metadata
        """
        pass


class _TabularView(_Tabular, ks._ColumnarView):
    """
    A _TabularView is created whenever operations such as slicing or appending
    are applied to an object that implements _Tabular, which are _TabularView and Table.

    Those operations are performed lazily, because especially on tables they can
    involve allocating and copying large amounts of memory and copying.

    If you need the materialized result of the operation, call the `.get()` method.
    """

    def __init__(self, delegate: _Tabular, operation: ks._ColumnarOperation):
        super().__init__(delegate, operation)

    def to_pandas(
        self, sentinel: Optional[Union[str, int]] = None
    ) -> "pandas.DataFrame":
        """See _Tabular.to_pandas"""
        return self.get().to_pandas(sentinel)

    def to_pyarrow(self, sentinel: Optional[Union[str, int]] = None) -> "pyarrow.Table":
        """See _Tabular.to_pyarrow"""
        return self.get().to_pyarrow(sentinel)

    @property
    def num_rows(self):
        return self.get().num_rows

    @property
    def column_names(self):
        return self.get().column_names

    @property
    def schema(self) -> ks.Schema:
        return self.get().schema

    def __str__(self):
        return f"TableView<delegate={self._delegate}, op={self._operation}>"

    def _select_rows(self, selection):
        raise NotImplementedError(
            "Cannot execute row selection on a view, do that on real data instead!"
        )


class _TabularOperation(ks._ColumnarOperation):
    @abstractmethod
    def apply(self, input: _Tabular) -> _Tabular:
        pass


class _RowSlicingOperation(_TabularOperation):
    def __init__(self, row_slice):
        self._row_slice = row_slice

    def apply(self, input):
        if not isinstance(input, _Tabular):
            return input
        else:
            return input._select_rows(self._row_slice)

    def __str__(self):
        return f"RowSlicingOp({self._row_slice})"


# ------------------------------------------------------------------
# Table
# ------------------------------------------------------------------
class _Backend:
    @abstractmethod
    def create_table_from_pyarrow(self, data, sentinel, row_ids: str = "auto"):
        raise RuntimeError("Not implemented")

    @abstractmethod
    def create_table_from_pandas(self, data, sentinel, row_ids: str = "auto"):
        raise RuntimeError("Not implemented")

    @abstractmethod
    def create_batch_output_table(self, row_ids: str = "generate"):
        raise RuntimeError("Not implemented")

    @abstractmethod
    def close(self):
        raise RuntimeError("Not implemented")


_backend = None


class Table(_Tabular):
    """This class serves as public API to create KNIME tables either from pandas or pyarrow.
    These tables can than be sent back to KNIME.
    This class has to be instantiated by calling either :func:`~knime.api.Table.from_pyarrow()` or
    :func:`~knime.api.Table.from_pandas()`"""

    def __init__(self):
        raise RuntimeError(
            "Do not use this constructor directly, use the from_ methods instead"
        )

    @staticmethod
    def from_pyarrow(
        data: "pyarrow.Table",
        sentinel: Optional[Union[str, int]] = None,
        row_ids: str = "auto",
    ):
        """
        Factory method to create a Table given a pyarrow.Table.

        **Example**::

            Table.from_pyarrow(my_pyarrow_table, sentinel="min")

        Args:
            data:
                A pyarrow.Table
            sentinel:
                Interpret the following values in integral columns as missing value:

                * ``"min"`` min int32 or min int64 depending on the type of the column
                * ``"max"`` max int32 or max int64 depending on the type of the column
                * a special integer value that should be interpreted as missing value
            row_ids:
                Defines what RowID should be used. Must be one of the following values:

                * ``"keep"``: Use the first column of the table as RowID. The first
                  column must be of type string.
                * ``"generate"``: Generate new RowIDs of the format ``f"Row{i}"``
                  where ``i`` is the position of the row (from ``0`` to ``length-1``).
                * ``"auto"``: Use the first column of the table if it has the name
                  "<RowID>" and is of type string or integer.

                  * If the "<RowID>" column is of type string, use it directly
                  * If the "<RowID>" column is of an integer type use ``f"Row{n}``
                    where ``n`` is the value of the integer column.
                  * Generate new RowIDs (``"generate"``) if the first column has
                    another type or name.
        """
        return _backend.create_table_from_pyarrow(data, sentinel, row_ids=row_ids)

    @staticmethod
    def from_pandas(
        data: "pandas.DataFrame",
        sentinel: Optional[Union[str, int]] = None,
        row_ids: str = "auto",
    ):
        """
        Factory method to create a Table given a pandas.DataFrame.
        The index of the data frame will be used as RowKey by KNIME.

        **Example**::

            Table.from_pandas(my_pandas_df, sentinel="min")

        Args:
            data:
                A pandas.DataFrame
            sentinel:
                Interpret the following values in integral columns as missing value:

                * ``"min"`` min int32 or min int64 depending on the type of the column
                * ``"max"`` max int32 or max int64 depending on the type of the column
                * a special integer value that should be interpreted as missing value
            row_ids:
                Defines what RowID should be used. Must be one of the following values:

                * ``"keep"``: Keep the ``DataFrame.index`` as the RowID. Convert the
                  index to strings if necessary.
                * ``"generate"``: Generate new RowIDs of the format ``f"Row{i}"``
                  where ``i`` is the position of the row (from ``0`` to ``length-1``).
                * ``"auto"``: If the ``DataFrame.index`` is of type int or unsigned int,
                  use ``f"Row{n}"`` where ``n`` is the index of the row. Else, use
                  "keep".
        """
        return _backend.create_table_from_pandas(data, sentinel, row_ids=row_ids)

    def to_batches(self) -> Iterator["Table"]:
        """
        Alias for ``Table.batches()``
        """
        return self.batches()

    def batches(self) -> Iterator["Table"]:
        """
        Returns a generator over the batches in this table. A batch is part of the table
        with all columns, but only a subset of the rows. A batch should always fit into
        memory (max size currently 64mb). The table being passed to execute() is already
        present in batches, so accessing the data this way is very efficient.

        **Example**::

            output_table = BatchOutputTable.create()
            for batch in my_table.batches():
                input_batch = batch.to_pandas()
                # process the batch
                output_table.append(Table.from_pandas(input_batch))
        """
        raise RuntimeError(
            "Retrieving batches is only allowed for unmodified tables, as they are passed to execute()"
        )


class BatchOutputTable:
    """
    An output table generated by combining smaller tables (also called batches).

    All batches must have the same number, names and types of columns.

    Does not provide means to continue to work with the data but is meant to be used
    as a return value of a Node's execute() method.
    """

    def __init__(self):
        raise RuntimeError(
            "Do not use this constructor directly, use the create or from_batches methods instead"
        )

    @staticmethod
    def create(row_ids: str = "generate"):
        """
        Create an empty BatchOutputTable

        Args:
            row_ids:
                Defines what RowID should be used. Must be one of the following values:

                * ``"generate"``: Generate new RowIDs of the format ``f"Row{i}"``
                * ``"keep"``:

                  * For appending DataFrames: Keep the ``DataFrame.index`` as the RowID.
                    Convert the index to strings if necessary.
                  * For appending Arrow tables or record batches: Use the first column
                    of the table as RowID. The first column must be of type string.
        """
        return _backend.create_batch_output_table(row_ids=row_ids)

    @staticmethod
    def from_batches(generator, row_ids: str = "generate"):
        """
        Create output table where each batch is provided by a generator

        Args:
            row_ids: See ``BatchOutputTable.create``.
        """
        out = _backend.create_batch_output_table(row_ids=row_ids)
        for b in generator:
            out.append(b)
        return out

    @abstractmethod
    def append(
        batch: Union[Table, "pandas.DataFrame", "pyarrow.Table", "pyarrow.RecordBatch"]
    ):
        """
        Append a batch to this output table. The first batch defines the structure of the table,
        and all subsequent batches must have the same number of columns, column names and column types.

        Note:
          Keep in mind that the RowID will be handled according to the "row_ids"
          mode chosen in ``BatchOutputTable.create``.
        """
        raise RuntimeError("Not implemented")

    @abstractproperty
    def num_batches() -> int:
        """
        The number of batches written to this output table
        """
        raise RuntimeError("Not implemented")
