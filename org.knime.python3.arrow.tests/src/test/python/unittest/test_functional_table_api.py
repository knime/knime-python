from abc import ABC, abstractmethod, abstractproperty

from typing import Iterator, List, Optional, Sequence, Tuple, Union
import unittest
import pandas as pd
import pyarrow as pa
import numpy as np

import pythonpath
import knime_arrow as ka
import knime_arrow_table as kat
import knime_arrow_types as katy
import knime_schema as ks


class Columnar(ABC):
    @abstractproperty
    def num_columns(self):
        pass

    def insert(self, other: "Columnar", at: int) -> "Columnar":
        n = self.num_columns
        permuted_indices = list(range(n))
        permuted_indices[at:at] = [i + n for i in range(other.num_columns)]
        return self.append()[permuted_indices]

    def map(
        self,
        func,
        output_name: str,
        output_type: ks.KnimeType = None,
        output_metadata=None,
    ) -> "Columnar":
        if output_type is None:
            import inspect

            output_type = type(inspect.signature(func).return_annotation)

        if output_type is None:
            raise ValueError("Cannot determine output type of map function")

        if not issubclass(output_type, ks.KnimeType):
            raise TypeError(
                "Output type of a map operation must be a subclass of KnimeType"
            )

        return self._map(func, output_type, output_name, output_metadata)

    def _map(
        self, func, output_type: ks.KnimeType, output_name: str, output_metadata=None,
    ) -> "ColumnarView":
        return ColumnarView(
            delegate=self,
            operation=MapOperation(func, output_type, output_name, output_metadata),
        )

    def __getitem__(
        self, slicing: Union[slice, List[int], List[str]]
    ) -> "ColumnarView":
        return ColumnarView(delegate=self, operation=ColumnSlicingOperation(slicing))

    def append(self, other: "Columnar") -> "ColumnarView":
        return ColumnarView(delegate=self, operation=AppendOperation(other))

    def filter(self, func) -> "Columnar":
        return ColumnarView(delegate=self, operation=RowFilterOperation(func))

    @abstractmethod
    def _select_columns(self, selection):
        """Implement column slicing here"""
        pass

    @abstractmethod
    def _append(self, other: "Columnar") -> "Columnar":
        """Implement append here"""
        pass


class ColumnarView(Columnar):
    def __init__(self, delegate: Columnar, operation: "ColumnarOperation"):
        self._delegate = delegate
        self._operation = operation

    @property
    def delegate(self) -> Columnar:
        return self._delegate

    @property
    def operation(self) -> "ColumnarOperation":
        return self._operation

    @property
    def num_columns(self):
        return self.get().num_columns

    def __str__(self):
        return f"ColumnarView<delegate={self._delegate}, op={self._operation}>"

    def get(self) -> Columnar:
        if isinstance(self._delegate, ColumnarView):
            input = self._delegate.get()
        else:
            input = self._delegate
        return self._operation.apply(input)

    def _select_columns(self, selection):
        raise NotImplementedError(
            "Cannot execute column selection on a view, do that on real data instead!"
        )

    def _append(self, other: "Columnar") -> "Columnar":
        raise NotImplementedError(
            "Cannot execute 'append' on a view, do that on real data instead!"
        )

    def __getattr__(self, name):
        # as a last resort, create and call the delegate
        return getattr(self.get(), name)


class Tabular(Columnar):
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
    ) -> "TabularView":
        """column major slicing"""
        if isinstance(slicing, tuple):
            column_sliced = TabularView(self, ColumnSlicingOperation(slicing[0]))
            return TabularView(column_sliced, RowSlicingOperation(slicing[1]))
        else:
            return TabularView(self, ColumnSlicingOperation(slicing))

    def to_pandas(self) -> "pandas.DataFrame":
        return self.get().to_pandas()

    def to_pyarrow(self) -> Union["pyarrow.Table", "pyarrow.RecordBatch"]:
        return self.get().to_pyarrow()

    @abstractmethod
    def _select_rows(self, selection):
        """Implement row slicing here"""
        pass


class TabularView(Tabular, ColumnarView):
    def __init__(self, delegate: Tabular, operation: "ColumnarOperation"):
        super().__init__(delegate, operation)

    def to_pandas(self) -> "pandas.DataFrame":
        return self.get().to_pandas()

    def to_pyarrow(self) -> Union["pyarrow.Table", "pyarrow.RecordBatch"]:
        return self.get().to_pyarrow()

    @property
    def num_rows(self):
        return self.get().num_rows

    def __str__(self):
        return f"TableView<delegate={self._delegate}, op={self._operation}>"

    def _select_rows(self, selection):
        raise NotImplementedError(
            "Cannot execute row selection on a view, do that on real data instead!"
        )


# do we need the distinction between batches and tables at all?

# --------------------------------------------------------------
# Operations
class ColumnarOperation(ABC):
    @abstractmethod
    def apply(self, input: Columnar) -> Columnar:
        # The input should NOT be a view
        pass


class ColumnSlicingOperation(ColumnarOperation):
    def __init__(self, col_slice):
        self._col_slice = col_slice

    def apply(self, input):
        return input._select_columns(self._col_slice)

    def __str__(self):
        return f"ColumnSlicingOp({self._col_slice})"


class AppendOperation(ColumnarOperation):
    def __init__(self, other):
        self._other = other

    def apply(self, input):
        if isinstance(self._other, ColumnarView):
            other = self._other.get()
        else:
            other = self._other

        return input._append(other)

    def __str__(self):
        return f"AppendOp({self._other})"


class MapOperation(ColumnarOperation):
    def __init__(self, func, output_type, output_name, output_metadata):
        self._func = func
        self._output_name = output_name
        self._output_type = output_type
        self._output_metadata = output_metadata

    def apply(self, input):
        if isinstance(input, Schema):
            return Schema(
                [self._output_type], [self._output_name], [self._output_metadata]
            )
        else:
            return input._map(
                self._func,
                output_name=self._output_name,
                output_type=self._output_type,
                output_metadata=self._output_metadata,
            )

    def __str__(self):
        return (
            f"MapOp({self._output_type}, {self._output_name}, {self._output_metadata})"
        )


# --------------------------------------------------------------
# Operations on Tables only
class TabularOperation(ColumnarOperation):
    @abstractmethod
    def apply(self, input: Tabular) -> Tabular:
        pass


class RowSlicingOperation(TabularOperation):
    def __init__(self, row_slice):
        self._row_slice = row_slice

    def apply(self, input):
        if not isinstance(input, Tabular):
            return input
        else:
            return input._select_rows(self._row_slice)

    def __str__(self):
        return f"RowSlicingOp({self._row_slice})"


class RowFilterOperation(TabularOperation):
    def __init__(self, func):
        self._func = func

    def apply(self, input):
        if not isinstance(input, Tabular):
            return input
        else:
            return input._filter(self._func)

    def __str__(self):
        return f"RowFilterOp({self._row_slice})"


# ------------------------------------------------------------------
# Schema
# ------------------------------------------------------------------


class Schema(Columnar):
    @staticmethod
    def from_columns(columns):
        types, names, metadata = zip(*[(c.type, c.name, c.metadata) for c in columns])
        return Schema(types, names, metadata)

    @staticmethod
    def from_types(types, names, metadata=None):
        return Schema(types, names, metadata)

    def __init__(self, types, names, metadata=None):
        if not isinstance(types, Sequence) or not all(
            isinstance(t, ks.KnimeType) or issubclass(t, ks.KnimeType) for t in types
        ):
            raise TypeError(
                f"Schema expected types to be a sequence of KNIME types but got {type(types)}: {types}"
            )

        if (not isinstance(names, list) and not isinstance(names, tuple)) or not all(
            isinstance(n, str) for n in names
        ):
            raise TypeError(
                f"Schema expected names to be a sequence of strings, but got {type(names)}"
            )

        if len(types) != len(names):
            raise ValueError(
                f"Number of types must match number of names, but {len(types)} != {len(names)}"
            )

        if metadata is not None:
            if not isinstance(metadata, Sequence):
                # DOESNT WORK: or not all(m is None or isinstance(m, str) for m in metadata):
                raise TypeError(
                    "Schema expected Metadata to be None or a sequence of strings or Nones"
                )

            if len(types) != len(metadata):
                raise ValueError(
                    f"Number of types must match number of metadata fields, but {len(types)} != {len(metadata)}"
                )
        else:
            metadata = [None] * len(types)

        self._columns = [ks.Column(t, n, m) for t, n, m in zip(types, names, metadata)]

    @property
    def column_names(self):
        return [c.name for c in self._columns]

    @property
    def num_columns(self):
        return len(self._columns)

    def __len__(self):
        return len(self._columns)

    def _select_columns(self, index) -> Union[ks.Column, "Schema"]:
        if isinstance(index, int):
            if index < 0 or index > len(self._columns):
                raise IndexError(
                    f"Index {index} does not exist in schema with {len(self)} columns"
                )
            return self._columns[index]
        elif isinstance(index, str):
            for c in self._columns:
                if c.name == index:
                    return Schema.from_columns([c])
            raise IndexError(f"Schema has no column named '{index}'")
        elif isinstance(index, slice):
            return Schema.from_columns(self._columns[index])
        else:
            raise TypeError(
                f"Schema can only be indexed by int or slice, not {type(index)}"
            )

    def __eq__(self, other) -> bool:
        if not isinstance(other, Schema):
            return False

        return all(a == b for a, b in zip(self._columns, other._columns))

    def _append(self, other: Union["Schema", ks.Column]) -> "Schema":
        cols = self._columns.copy()

        if isinstance(other, Schema):
            cols.extend(other._columns)
        elif isinstance(other, ks.Column):
            cols.append(other)
        else:
            raise ValueError(
                f"Can only append columns or schemas to this schema, not {type(other)}"
            )

        return Schema.from_columns(cols)

    def __str__(self) -> str:
        sep = ",\n\t"
        return f"Schema<\n\t{sep.join(str(c) for c in self._columns)}>"

    def __repr__(self) -> str:
        return str(self)


# ------------------------------------------------------------------
# Table
# ------------------------------------------------------------------


class Table(Tabular):
    """This is what we'd show as public API"""

    @staticmethod
    def from_pyarrow(data: pa.Table, sentinel: Optional[Union[str, int]] = None):
        if sentinel is not None:
            data = katy.sentinel_to_missing_value(data, sentinel)
        data = katy.wrap_primitive_arrays(data)
        return ArrowTable(data)

    @staticmethod
    def from_pandas(data: pa.Table, sentinel: Optional[Union[str, int]] = None):
        import knime_arrow_pandas as kap
        import pandas as pd

        if not isinstance(data, pd.DataFrame):
            raise ValueError(
                f"Table.from_pandas expects a pandas.DataFrame, but got {type(data)}"
            )
        data = kap.pandas_df_to_arrow(data)
        return Table.from_pyarrow(data, sentinel)


def _knime_type_to_pyarrow(knime_type):
    _knime_to_pyarrow_type = {
        type(ks.int32()): pa.int32(),
        type(ks.int64()): pa.int64(),
        type(ks.string()): pa.string(),
        type(ks.double()): pa.float64(),
        type(ks.bool_()): pa.bool_(),
        type(ks.blob()): pa.large_binary(),
    }

    if isinstance(knime_type, ks.ListType):
        raise NotImplementedError()
    elif isinstance(knime_type, ks.StructType):
        raise NotImplementedError()
    elif isinstance(knime_type, ks.ExtensionType):
        raise NotImplementedError()

    return _knime_to_pyarrow_type[knime_type]


class ArrowTable(Table):
    def __init__(self, table):
        self._table = table

    def to_pandas(
        self, sentinel: Optional[Union[str, int]] = None,
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
        return ArrowTable(kat._select_rows(self._get_table(), selection))

    def _select_columns(self, selection) -> "ArrowTable":
        return ArrowTable(kat._select_columns(self._get_table(), selection))

    def _append(self, other: "ArrowTable") -> "ArrowTable":
        a = self._get_table()
        b = other._get_table()
        appended = pa.Table.from_arrays(
            a.columns + b.columns, names=a.schema.names + b.schema.names
        )
        return ArrowTable(appended)

    def _map(
        self, func, output_type: ks.KnimeType, output_name: str, output_metadata=None
    ) -> "ArrowTable":
        data = self._get_table()
        # TODO: handle chunked array, handle extension types
        out = []
        for i in range(len(data)):
            params = [c[i].as_py() for c in data.columns]
            out.append(func(*params))
        array = pa.array(out, type=_knime_type_to_pyarrow(output_type))
        # TODO: pass in metadata
        return ArrowTable(pa.Table.from_arrays([array], names=[output_name]))

    def _filter(self, func) -> "ArrowTable":
        data = self._get_table()
        # TODO: handle chunked array, handle extension types
        mask = []
        for i in range(len(data)):
            params = [c[i].as_py() for c in data.columns]
            mask.append(func(*params))  # should return boolean...
        mask = pa.array(mask)
        # TODO: pass in metadata
        return ArrowTable(data.filter(mask))

    @property
    def num_rows(self) -> int:
        return len(self._table)

    @property
    def num_columns(self) -> int:
        return len(self._table.schema)

    @property
    def column_names(self) -> List[str]:
        return self._table.schema.names

    @property
    def schema(self) -> ks.Schema:
        return kat._convert_arrow_schema_to_knime(self._table.schema)

    def __str__(self):
        return f"ArrowTable[shape=({self.num_columns}, {self.num_rows})]"


class ArrowSourceTable(ArrowTable):
    def __init__(self, source: ka.ArrowDataSource):
        self._source = source

    def _get_table(self):
        return self._source.to_arrow_table()

    @property
    def num_rows(self) -> int:
        return self._source.num_rows

    @property
    def num_columns(self) -> int:
        return len(self._source.schema)

    @property
    def column_names(self) -> List[str]:
        return self._source.schema.names

    @property
    def schema(self) -> ks.Schema:
        return kat._convert_arrow_schema_to_knime(self._source.schema)

    def __str__(self):
        return f"ArrowSourceTable[cols={self.num_columns}, rows={self.num_rows}, batches={self.num_batches}]"

    @property
    def num_batches(self) -> int:
        return len(self._source)

    def batches():
        pass


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------


class SchemaTest(unittest.TestCase):
    def test_schema_ops(self):
        types = [ks.int32(), ks.int64(), ks.double(), ks.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = Schema.from_types(types, names)
        sv = s[0:2]
        self.assertIsInstance(sv, ColumnarView)
        self.assertNotIsInstance(sv, TabularView)
        self.assertIsInstance(sv.operation, ColumnSlicingOperation)
        self.assertIsInstance(sv.delegate, Schema)
        # the following line delegates the call to -> apply operation -> call column_names on result
        self.assertEqual(["Ints", "Longs"], sv.column_names)
        self.assertIsInstance(s[2], ColumnarView)
        self.assertEqual("Doubles", s[2].name)
        self.assertIsInstance(s[3], ColumnarView)
        self.assertEqual(ks.string(), s[3].type)
        with self.assertRaises(AttributeError):
            s[1].foobar(2)

    def test_map(self):
        types = [ks.int32(), ks.int64(), ks.double(), ks.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = Schema.from_types(types, names)

        def fn(val: ks.double()) -> ks.string():
            return str(val)

        s2 = s["Doubles"].map(fn, output_name="DoubleStrings")
        s3 = s.append(s2)
        self.assertEqual(
            ["Ints", "Longs", "Doubles", "Strings", "DoubleStrings"], s3.column_names
        )

    def test_filter(self):
        types = [ks.int32(), ks.int64(), ks.double(), ks.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = Schema.from_types(types, names)

        def my_filter(row_key, a, *args):
            return a % 2 == 0

        out_s = s.filter(my_filter).get()
        self.assertEqual(s, out_s)


class TableTest(unittest.TestCase):
    def test_table_ops(self):
        df = pd.DataFrame()
        df[0] = [1, 2, 3, 4]
        df[1] = [5.0, 6.0, 7.0, 8.0]
        t = Table.from_pandas(df)
        tv = t[0:2]  # Row Key and first column
        self.assertIsInstance(tv, ColumnarView)
        self.assertIsInstance(tv, TabularView)
        self.assertIsInstance(tv.operation, ColumnSlicingOperation)
        self.assertIsInstance(tv.delegate, ArrowTable)
        self.assertEqual(t.num_rows, tv.num_rows)
        col_df = tv.to_pandas()
        self.assertEqual(list(df.iloc[:, 0]), list(col_df.iloc[:, 0]))

    def test_map_add(self):
        df = pd.DataFrame()
        df["A"] = [1, 2, 3, 4]
        df["B"] = [10, 20, 30, 40]
        df["reference"] = [11, 22, 33, 44]
        t = Table.from_pandas(df)

        def my_add(a: ks.int64(), b: ks.int64()) -> ks.int64():
            return a + b

        out_t = t.append(t[["A", "B"]].map(my_add, output_name="result"))
        out_df = out_t.to_pandas()
        self.assertTrue(all(out_df.loc[:, "reference"] == out_df.loc[:, "result"]))

    def test_filter(self):
        df = pd.DataFrame()
        df["A"] = [1, 2, 3, 4]
        df["B"] = [10, 20, 30, 40]
        t = Table.from_pandas(df)

        def my_filter(row_key, a, *args):
            return a % 2 == 0

        out_t = t.filter(my_filter)
        out_df = out_t.to_pandas()
        self.assertEqual(2, len(out_df))
        self.assertEqual(2, out_df.iloc[0, 0])
        self.assertEqual(4, out_df.iloc[1, 0])


# TODO: look at / use sympy for arithmetic operations of tables

if __name__ == "__main__":
    unittest.main()
