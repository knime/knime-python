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

Note that this is the knime_table version for pure-Python KNIME nodes, 
not for the Python Scripting (Labs) node!

@author Carsten Haubold, KNIME GmbH, Konstanz, Germany
"""

from abc import ABC, abstractmethod, abstractproperty

from typing import Dict, Iterator, List, Optional, Sequence, Tuple, Union
import unittest
import pandas as pd
import pyarrow as pa
import numpy as np
import logging

import knime_schema as ks

LOGGER = logging.getLogger(__name__)

# --------------------------------------------------------------
# _Tabular interface, _TabularView and Operations


class _Tabular(ks._Columnar):
    """
    Interface for 
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
        
        The syntax is `[column_slice, row_slice]`. Note that this is the exact opposite order than in the Python Script 
        (Labs) node's ReadTable.

        Args:
            column_slice:
                A column index, a column name, a slice object, a list of column indices, or a list of column names.
            row_slice:
                Optional: A slice object describing which rows to use.

        Returns:
            A _TabularView representing a slice of the original Table

        **Examples:**

        Get the first 100 rows of columns 1,2,3,4:
        ``sliced_table = table[1:5, :100]``

        Get all rows of the columns "name" and "age":
        ``sliced_table = table[["name", "age"]]``
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

    def to_pyarrow(
        self, sentinel: Optional[Union[str, int]] = None
    ) -> Union["pyarrow.Table", "pyarrow.RecordBatch"]:
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
    def __init__(self, delegate: _Tabular, operation: ks._ColumnarOperation):
        super().__init__(delegate, operation)

    def to_pandas(
        self, sentinel: Optional[Union[str, int]] = None
    ) -> "pandas.DataFrame":
        """See _Tabular.to_pandas"""
        return self.get().to_pandas()

    def to_pyarrow(
        self, sentinel: Optional[Union[str, int]] = None
    ) -> Union["pyarrow.Table", "pyarrow.RecordBatch"]:
        """See _Tabular.to_pyarrow"""
        return self.get().to_pyarrow()

    @property
    def num_rows(self):
        return self.get().num_rows

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
    def create_table_from_pyarrow(self, data, sentinel):
        raise RuntimeError("Not implemented")

    @abstractmethod
    def create_table_from_pandas(self, data, sentinel):
        raise RuntimeError("Not implemented")

    @abstractmethod
    def create_batch_output_table(self):
        raise RuntimeError("Not implemented")

    @abstractmethod
    def close(self):
        raise RuntimeError("Not implemented")


_backend = None


class Table(_Tabular):
    """This is what we'd show as public API"""

    def __init__(self):
        raise RuntimeError(
            "Do not use this constructor directly, use the from_ methods instead"
        )

    @staticmethod
    def from_pyarrow(data: pa.Table, sentinel: Optional[Union[str, int]] = None):
        """
        Factory method to create a Table given a pyarrow.Table.
        The first column of the pyarrow.Table must contain unique row identifiers of type 'string'.

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
        """
        return _backend.create_table_from_pyarrow(data, sentinel)

    @staticmethod
    def from_pandas(data: pa.Table, sentinel: Optional[Union[str, int]] = None):
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
        """
        return _backend.create_table_from_pandas(data, sentinel)

    def to_batches(self) -> Iterator["Table"]:
        """
        Returns a generator over the batches in this table. A batch is part of the table
        with all columns, but only a subset of the rows. A batch should always fit into
        memory (max size currently 64mb). The table being passed to execute() is already
        present in batches, so accessing the data this way is very efficient.
        
        **Example**::
        
            output_table = BatchOutputTable.create()
            for batch in my_table.to_batches():
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

    Does not provide means to continue to work with the data but is meant to be used
    as a return value of a Node's execute() method.
    """

    def __init__(self):
        raise RuntimeError(
            "Do not use this constructor directly, use the create or from_batches methods instead"
        )

    @staticmethod
    def create():
        """
        Create an empty BatchOutputTable
        """
        return _backend.create_batch_output_table()

    @staticmethod
    def from_batches(generator):
        """
        Create output table where each batch is provided by a generator
        """
        out = _backend.create_batch_output_table()
        for b in generator:
            out.append(b)
        return out

    @abstractmethod
    def append(batch: Table):
        """
        Append a batch to this output table
        """
        raise RuntimeError("Not implemented")

    @abstractproperty
    def num_batches() -> int:
        """
        The number of batches written to this output table
        """
        raise RuntimeError("Not implemented")
