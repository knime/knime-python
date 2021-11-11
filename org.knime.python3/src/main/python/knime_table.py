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
Defines the general Table object that is used to transfer data from and to KNIME.

@author Carsten Haubold, KNIME GmbH, Konstanz, Germany
"""


from abc import abstractmethod, ABC
from typing import Iterator, List, Optional, Tuple, Type, Union


class _Backend(ABC):
    """
    The backend instanciates the appropriate types of Tables and Batches.
    """

    @abstractmethod
    def batch(
        data: Union["pandas.DataFrame", "pyarrow.RecordBatch"],
        sentinel: Optional[Union[str, int]] = None,
    ) -> "Batch":
        pass

    @abstractmethod
    def write_table(
        data: Optional[Union["pandas.DataFrame", "pyarrow.Table"]] = None,
        sentinel: Optional[Union[str, int]] = None,
    ) -> "WriteTable":
        pass


_backend: _Backend = None  # The globally instanciated backend for creating tables and batches


class Batch(ABC):
    """
    A Batch is a part of a table containing data. A Batch should always fit into the system memory,
    thus all methods accessing the data will be processed immediately and synchronously.
    """

    @property
    def shape(self) -> Tuple[int, int]:
        """ 
        Returns a tuple in the form (numRows, numColumns) representing the shape of this Batch.
        """
        return (self.num_rows, self.num_columns)

    @property
    @abstractmethod
    def num_rows(self) -> int:
        """Return the number of rows in the Batch."""
        pass

    @property
    @abstractmethod
    def num_columns(self) -> int:
        """Return the number of columns in the Batch."""
        pass

    @property
    @abstractmethod
    def column_names(self) -> List[str]:
        """
        Return the list of column names
        """
        pass

    @abstractmethod
    def to_pandas(
        self,
        rows: Optional[Union[int, Tuple[int, int]]] = None,
        columns: Optional[Union[List[int], Tuple[int, int], List[str]]] = None,
        sentinel: Optional[Union[str, int]] = None,
    ) -> "pandas.DataFrame":
        """
        Access this Batch as a pandas.DataFrame.

        Example:

        >>> for batch in readTable.batches():
        ...     df = batch.to_pandas(rows=(0,20), columns=(2,6))
        ...     assert df.shape == (20, 4)

        Arguments:
            rows: 
                Specify one of the following to restrict which rows are returned:
                    - An integer describing the number of rows to use, starting at the beginning.
                    - A tuple describing start (inclusive) and end (exclusive) of the range of rows that will be returned. 
                      Pass None as start or end to leave it at the default.
            columns: 
                Specify one of the following to restrict which columns are returned:
                    - A list of column indices
                    - A tuple describing start (inclusive) and end (exclusive) of the range of columns that will be returned. 
                      Pass None as start or end to leave it at the default.
                    - A list of column names
            sentinel: 
                Replace missing values in integral columns by the given value, one of:
                    - "min" min int32 or min int64 depending on the type of the column
                    - "max" max int32 or max int64 depending on the type of the column
                    - An integer value that should be inserted for each missing value

        Raises:
            IndexError: If rows or columns were requested outside of the available shape
        """
        pass

    @abstractmethod
    def to_pyarrow(
        self,
        rows: Optional[Union[int, Tuple[int, int]]] = None,
        columns: Optional[Union[List[int], Tuple[int, int], List[str]]] = None,
        sentinel: Optional[Union[str, int]] = None,
    ) -> "pyarrow.RecordBatch":
        """
        Access this Batch as a pyarrow.RecordBatch.

        Example:

        >>> for batch in readTable.batches():
        ...     arrow_batch = batch.to_pyarrow(rows=(0,20), columns=(2,6))
        ...     assert len(arrow_batch) == 20

        Arguments:
            rows: 
                Specify one of the following to restrict which rows are returned:
                    - An integer describing the number of rows to use, starting at the beginning.
                    - A tuple describing start (inclusive) and end (exclusive) of the range of rows that will be returned. 
                      Pass None as start or end to leave it at the default.
            columns: 
                Specify one of the following to restrict which columns are returned:
                    - A list of column indices
                    - A tuple describing start (inclusive) and end (exclusive) of the range of columns that will be returned. 
                      Pass None as start or end to leave it at the default.
                    - A list of column names
            sentinel: 
                Replace missing values in integral columns by the given value, one of:
                    - "min" min int32 or min int64 depending on the type of the column
                    - "max" max int32 or max int64 depending on the type of the column
                    - An integer value that should be inserted for each missing value

        Raises:
            IndexError: If rows or columns were requested outside of the available shape
        """
        pass

    @staticmethod
    def from_pandas(
        data: "pandas.DataFrame", sentinel: Optional[Union[str, int]] = None
    ) -> "Batch":
        """
        Create a Batch from a pandas.DataFrame.

        Arguments:
            data:
                A pandas.DataFrame.
            sentinel: 
                Interpret the following values in integral columns as missing value:
                    - "min" min int32 or min int64 depending on the type of the column
                    - "max" max int32 or max int64 depending on the type of the column
                    - a special integer value that should be interpreted as missing value
        """
        return _backend.batch(data, sentinel)

    @staticmethod
    def from_pyarrow(
        data: "pyarrow.RecordBatch", sentinel: Optional[Union[str, int]] = None
    ) -> "Batch":
        """
        Create a Batch from a pyarrow.RecordBatch.

        Arguments:
            data:
                A pyarrow.RecordBatch.
            sentinel: 
                Interpret the following values in integral columns as missing value:
                    - "min" min int32 or min int64 depending on the type of the column
                    - "max" max int32 or max int64 depending on the type of the column
                    - a special integer value that should be interpreted as missing value
        """
        return _backend.batch(data, sentinel)


class Table(ABC):
    """
    A KNIME Table provides the general functionality to access KNIME tabular data which might be larger than the 
    system's memory and/or not yet completely written to disk.

    The underlying data is available in batches of rows. At least one Batch of this Table is available, providing
    immediate access to the number of columns and the column names.
    """

    @property
    def shape(self) -> Tuple[int, int]:
        """ 
        Returns a tuple in the form (numRows, numColumns) representing the shape of this Table.

        If the Table is not completely available yet because Batches are still appended to it, 
        querying the shape blocks until all data is available
        """
        return (self.num_rows, self.num_columns)

    @property
    @abstractmethod
    def num_rows(self) -> int:
        """
        Return the number of rows in the Table. 
        
        If the Table is not completely available yet because Batches are still appended to it, 
        querying the number of rows blocks until all data is available.
        """
        pass

    @property
    @abstractmethod
    def num_columns(self) -> int:
        """
        Return the number of columns in the Table.
        """
        pass

    @property
    @abstractmethod
    def column_names(self) -> List[str]:
        """
        Return the list of column names
        """
        pass

    @property
    @abstractmethod
    def num_batches(self) -> int:
        """
        Return the number of Batches in this Table.

        If the Table is not completely available yet because Batches are still appended to it, 
        querying the number of batches blocks until all data is available.
        """
        pass

    def __len__(self) -> int:
        """Return the number of Batches of this Table"""
        return self.num_batches


class ReadTable(Table):
    """
    A KNIME ReadTable provides access to the data provided from KNIME, either in full (must fit into memory) 
    or split into row-wise Batches.
    """

    @abstractmethod
    def to_pandas(
        self,
        rows: Optional[Union[int, Tuple[int, int]]] = None,
        columns: Optional[Union[List[int], Tuple[int, int], List[str]]] = None,
        sentinel: Optional[Union[str, int]] = None,
    ) -> "pandas.DataFrame":
        """
        Convert the full table to a pandas.DataFrame. 

        If the Table is not completely available yet because Batches are still appended to it, 
        this method blocks until all data is available.

        Attention: if the size of the table exceeds the system memory 

        Example:

        >>> df = batch.to_pandas(rows=(0,20), columns=(2,6))
        ... df.shape == (20, 4)

        Arguments:
            rows: 
                Specify one of the following to restrict which rows are returned:
                    - An integer describing the number of rows to use, starting at the beginning.
                    - A tuple describing start (inclusive) and end (exclusive) of the range of rows that will be returned. 
                      Pass None as start or end to leave it at the default.
            columns: 
                Specify one of the following to restrict which columns are returned:
                    - A list of column indices
                    - A tuple describing start (inclusive) and end (exclusive) of the range of columns that will be returned. 
                      Pass None as start or end to leave it at the default.
                    - A list of column names
            sentinel: 
                Replace missing values in integral columns by the given value, one of:
                    - "min" min int32 or min int64 depending on the type of the column
                    - "max" max int32 or max int64 depending on the type of the column
                    - An integer value that should be inserted for each missing value

        Raises:
            IndexError: If rows or columns were requested outside of the available shape
        """
        pass

    @abstractmethod
    def to_pyarrow(
        self,
        rows: Optional[Union[int, Tuple[int, int]]] = None,
        columns: Optional[Union[List[int], Tuple[int, int], List[str]]] = None,
        sentinel: Optional[Union[str, int]] = None,
    ) -> "pyarrow.Table":
        """
        Access to the full Table as pyarrow.Table.

        If the Table is not completely available yet because Batches are still appended to it, 
        this method blocks until all data is available.

        Arguments:
            rows: 
                Specify one of the following to restrict which rows are returned:
                    - An integer describing the number of rows to use, starting at the beginning.
                    - A tuple describing start (inclusive) and end (exclusive) of the range of rows that will be returned. 
                      Pass None as start or end to leave it at the default.
            columns: 
                Specify one of the following to restrict which columns are returned:
                    - A list of column indices
                    - A tuple describing start (inclusive) and end (exclusive) of the range of columns that will be returned. 
                      Pass None as start or end to leave it at the default.
                    - A list of column names
            sentinel: 
                Replace missing values in integral columns by the given value, one of:
                    - "min" min int32 or min int64 depending on the type of the column
                    - "max" max int32 or max int64 depending on the type of the column
                    - An integer value that should be inserted for each missing value

        Raises:
            IndexError: If rows or columns were requested outside of the available shape
        """
        pass

    @abstractmethod
    def batches(self) -> Iterator[Batch]:
        """
        Return an iterator over the Batches in this Table. If the iterator is advanced to a Batch 
        that is not available yet, it will block until the data is present.
        """
        pass


class WriteTable(Table):
    """
    A Table that can be filled as a whole, or by appending individual batches. The data is serialized
    to disk batch by batch. Individual batches will be available to KNIME as soon as they are written.
    """

    @staticmethod
    def create() -> "WriteTable":
        """Create an empty WriteTable"""
        return _backend.write_table()

    @staticmethod
    def from_pandas(
        data: "pandas.DataFrame", sentinel: Optional[Union[str, int]] = None
    ) -> "WriteTable":
        """
        Create and fill the table with a pandas DataFrame

        Arguments:
        data:
            A pandas.DataFrame
        sentinel: 
            Interpret the following values in integral columns as missing value:
                - "min" min int32 or min int64 depending on the type of the column
                - "max" max int32 or max int64 depending on the type of the column
                - a special integer value that should be interpreted as missing value
        """
        return _backend.write_table(data, sentinel)

    @staticmethod
    def from_pyarrow(
        data: "pyarrow.Table", sentinel: Optional[Union[str, int]] = None
    ) -> "WriteTable":
        """
        Create and fill the table with a pyarrow.Table
        
        Arguments:
        data:
            A pyarrow.RecordBatch
        sentinel: 
            Interpret the following values in integral columns as missing value:
                - "min" min int32 or min int64 depending on the type of the column
                - "max" max int32 or max int64 depending on the type of the column
                - a special integer value that should be interpreted as missing value
        """
        return _backend.write_table(data, sentinel)

    @abstractmethod
    def append(self, batch: Batch):
        """
        Appends the given batch to the end of this Table. The number of columns as well as their
        data types must match that of the previous batches in this Table.

        Raise:
            ValueError:
                If the new batch does not have the same columns as previous batches in this 
                WriteTable.      
        """
        pass


def batch(
    data: Union["pandas.DataFrame", "pyarrow.RecordBatch"],
    sentinel: Optional[Union[str, int]] = None,
) -> Batch:
    """
    Factory method to create a Batch given a pandas.DataFrame or a pyarrow.RecordBatch.
    Internally uses its from_pandas or from_pyarrow methods.

    Arguments:
        data:
            A pandas.DataFrame or a pyarrow.RecordBatch
        sentinel: 
            Interpret the following values in integral columns as missing value:
                - "min" min int32 or min int64 depending on the type of the column
                - "max" max int32 or max int64 depending on the type of the column
                - a special integer value that should be interpreted as missing value
    """
    return _backend.batch(data, sentinel)


def write_table(
    data: Optional[Union["pandas.DataFrame", "pyarrow.Table"]] = None,
    sentinel: Optional[Union[str, int]] = None,
) -> WriteTable:
    """
    Factory method to create a WriteTable given no data, a pandas.DataFrame, or a pyarrow.Table.
    Internally creates a WriteTable using its create, from_pandas or from_pyarrow methods respectively.

    Arguments:
        data:
            None, a pandas.DataFrame or a pyarrow.RecordBatch
        sentinel: 
            Interpret the following values in integral columns as missing value:
                - "min" min int32 or min int64 depending on the type of the column
                - "max" max int32 or max int64 depending on the type of the column
                - a special integer value that should be interpreted as missing value
    """
    return _backend.table(data, sentinel)
