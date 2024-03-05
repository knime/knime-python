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
@author Carsten Haubold, KNIME GmbH, Konstanz, Germany
"""

from abc import ABC, abstractmethod
import os
import unittest
import json
import datetime as dt

import knime.api.schema as k
import knime.api.types as kt
import knime._arrow._types as katy


class TypeTest(ABC):
    @abstractmethod
    def create_type(self):
        pass

    @abstractmethod
    def isinstance(self, o):
        pass

    def is_singleton(self):
        return True

    def test_type(self):
        self.assertTrue(self.isinstance(self.create_type()))
        self.assertIsInstance(self.create_type(), k.KnimeType)

    def test_equals(self):
        a = self.create_type()
        b = self.create_type()
        self.assertEqual(a, b)
        self.assertFalse(a != b)
        if self.is_singleton():
            self.assertTrue(a is b)


class IntTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.int32()

    def isinstance(self, o):
        return isinstance(o, k.PrimitiveType) and o._type_id == k.PrimitiveTypeId.INT


class BoolTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.bool_()

    def isinstance(self, o):
        return isinstance(o, k.PrimitiveType) and o._type_id == k.PrimitiveTypeId.BOOL


class BlobTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.blob()

    def isinstance(self, o):
        return isinstance(o, k.PrimitiveType) and o._type_id == k.PrimitiveTypeId.BLOB


class LongTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.int64()

    def isinstance(self, o):
        return isinstance(o, k.PrimitiveType) and o._type_id == k.PrimitiveTypeId.LONG


class StringTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.string()

    def isinstance(self, o):
        return isinstance(o, k.PrimitiveType) and o._type_id == k.PrimitiveTypeId.STRING


class DoubleTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.double()

    def isinstance(self, o):
        return isinstance(o, k.PrimitiveType) and o._type_id == k.PrimitiveTypeId.DOUBLE


class NullTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.null()

    def isinstance(self, o):
        return isinstance(o, k.PrimitiveType) and o._type_id == k.PrimitiveTypeId.NULL


class IntListTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.list_(k.int32())

    def isinstance(self, o):
        return (
            isinstance(o, k.ListType)
            and isinstance(o.inner_type, k.PrimitiveType)
            and o.inner_type._type_id == k.PrimitiveTypeId.INT
        )

    def is_singleton(self):
        return False


class IntStringStructTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.struct(k.int32(), k.string())

    def isinstance(self, o):
        return (
            isinstance(o, k.StructType)
            and isinstance(o.inner_types[0], k.PrimitiveType)
            and o.inner_types[0]._type_id == k.PrimitiveTypeId.INT
            and isinstance(o.inner_types[1], k.PrimitiveType)
            and o.inner_types[1]._type_id == k.PrimitiveTypeId.STRING
        )

    def is_singleton(self):
        return False


class LogicalStringTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.LogicalType(k._knime_logical_type("StringValueFactory"), k.string())

    def isinstance(self, o):
        return (
            isinstance(o, k.LogicalType)
            and o.storage_type == k.string()
            and o.logical_type == k._knime_logical_type("StringValueFactory")
        )

    def test_value_type_fails(self):
        with self.assertRaises(ValueError):
            t = self.create_type()
            print(t.value_type)

    def is_singleton(self):
        return False


class LogicalTimeTest(TypeTest, unittest.TestCase):
    """
    This test shows how extension types can be created
    with KNIME's Python type system
    """

    def setUpClass():
        _register_extension_types()

    def create_type(self):
        return k.logical(dt.date)

    def isinstance(self, o):
        return isinstance(o, k.LogicalType)

    def test_value_type(self):
        t = self.create_type()
        self.assertEqual(
            str(dt.date.__module__) + "." + str(dt.date.__name__), t.value_type
        )

    def is_singleton(self):
        return False


class UnknownLogicalTest(unittest.TestCase):
    def test_unknown_extension_creation_fails(self):
        class Dummy:
            pass

        with self.assertRaises(TypeError):
            t = k.logical(Dummy())

        with self.assertRaises(TypeError):
            t = k.logical(int)


class KnimeType(unittest.TestCase):
    def test_knime_type_cannot_be_created(self):
        with self.assertRaises(TypeError):
            t = k.KnimeType()


class KnimeTypeInDict(unittest.TestCase):
    def test_knime_types_are_hashable(self):
        d = {
            k.int32(): "int32",
            k.int64(): "int64",
            k.double(): "double",
            k.string(): "string",
            k.string(k.DictEncodingKeyType.BYTE): "string[dict_encoding=BYTE_KEY]",
            k.bool_(): "bool",
            k.blob(): "blob",
            k.null(): "null",
            k.list_(k.int32()): "list<int32>",
            k.struct(k.int32(), k.string()): "struct<int32, string>",
        }

        for key, v in d.items():
            self.assertEqual(v, str(key))


# ----------------------------------------------------------


def _register_extension_types():
    import knime.api.types as kt

    kt.register_python_value_factory(
        "knime.types.builtin",
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
        "knime.types.builtin",
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
        "knime.types.builtin",
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


# ----------------------------------------------------------


class SchemaTest(unittest.TestCase):
    def test_creation_without_metadata(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = k.Schema.from_types(types, names)
        self.assertEqual(len(types), s.num_columns)
        for i, (t, n) in enumerate(zip(types, names)):
            self.assertIsInstance(s[i].get(), k.Column)
            self.assertEqual(t, s[i].ktype)
            self.assertEqual(n, s[i].name)
            self.assertIsNone(s[i].metadata)

    def test_creation_with_metadata(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = k.Schema.from_types(types, names, names)
        self.assertEqual(len(types), s.num_columns)
        for i, (t, n) in enumerate(zip(types, names)):
            self.assertIsInstance(s[i].get(), k.Column)
            self.assertEqual(t, s[i].ktype)
            self.assertEqual(n, s[i].name)
            self.assertEqual(n, s[i].metadata)

    def test_column_creation_without_metadata(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        columns = [k.Column(t, n, None) for t, n in zip(types, names)]
        s = k.Schema.from_columns(columns)
        self.assertEqual(len(types), s.num_columns)
        for i, (t, n) in enumerate(zip(types, names)):
            self.assertIsInstance(s[i].get(), k.Column)
            self.assertEqual(t, s[i].ktype)
            self.assertEqual(n, s[i].name)
            self.assertIsNone(s[i].metadata)

    def test_column_creation_with_metadata(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        columns = [k.Column(t, n, n) for t, n in zip(types, names)]
        s = k.Schema.from_columns(columns)
        self.assertEqual(len(types), s.num_columns)
        for i, (t, n) in enumerate(zip(types, names)):
            self.assertIsInstance(s[i].get(), k.Column)
            self.assertEqual(t, s[i].ktype)
            self.assertEqual(n, s[i].name)
            self.assertEqual(n, s[i].metadata)

    def test_creation_preconditions(self):
        with self.assertRaises(TypeError):
            k.Schema.from_types(k.int32(), ["Ints"])

        with self.assertRaises(TypeError):
            k.Schema.from_types([k.int32()], "Ints")

        with self.assertRaises(TypeError):
            k.Schema.from_types([int], ["Ints"])

        with self.assertRaises(TypeError):
            k.Schema.from_types([k.int32()], [1.5])

        with self.assertRaises(ValueError):
            k.Schema.from_types([k.int32()], ["Too", "many", "names"])

        with self.assertRaises(ValueError):
            k.Schema.from_types([k.int32()] * 4, ["Too", "few", "names"])

    def test_equals(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s1 = k.Schema.from_types(types, names)
        s2 = k.Schema(types, names)
        s3 = k.Schema(types, names, metadata=names)
        s4 = k.Schema.from_types(types, names, names)
        self.assertTrue(s1 == s2)
        self.assertFalse(s1 != s2)
        self.assertTrue(s3 == s4)
        self.assertFalse(s1 == s3)
        self.assertTrue(s1 != s3)
        self.assertTrue(s2 != s4)

    def test_append(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = k.Schema(types, names)
        s_with_m = k.Schema(types, names, metadata=names)
        s_double = k.Schema(types + types, names + names)
        s_with_m_double = k.Schema(types + types, names + names, names + names)
        self.assertEqual(2 * s.num_columns, s_double.num_columns)
        self.assertEqual(2 * s_with_m.num_columns, s_with_m_double.num_columns)

        added_no_meta = s.append(s)
        added_with_meta = s_with_m.append(s_with_m)
        self.assertEqual(s_double, added_no_meta.get())
        self.assertEqual(s_with_m_double, added_with_meta.get())

    def test_slicing(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = k.Schema(types, names)
        sl = s[1:3]
        self.assertEqual(2, sl.num_columns)
        self.assertEqual(["Longs", "Doubles"], sl.column_names)

    def test_column_selection(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = k.Schema(types, names)
        self.assertEqual(s[1], s["Longs"])
        self.assertEqual(s[1].ktype, s["Longs"].ktype)
        self.assertEqual(s[1].name, s["Longs"].name)

    def test_column_selection_wraparound(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = k.Schema(types, names)
        self.assertEqual(s[2], s[-2])
        self.assertEqual(s[2].ktype, s[-2].ktype)
        self.assertEqual(s[2].name, s[-2].name)

    def test_column_permutation(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = k.Schema(types, names)

        selection = ["Strings", "Ints", "Longs"]
        s_int = s[[3, 0, 1]]
        s_str = s[selection]
        self.assertEqual(s_int.num_columns, s_str.num_columns)
        self.assertEqual(s_int.column_names, s_str.column_names)
        self.assertEqual(selection, s_str.column_names)

    def test_empty_column_name_throws(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "", "Strings"]

        with self.assertRaises(ValueError):
            s = k.Schema(types, names)

        with self.assertRaises(ValueError):
            c = k.Column(k.int32(), "")

        with self.assertRaises(ValueError):
            c = k.Column(k.int32(), "\t    ")

    def test_duplicate_column_name_throws_when_converted_to_knime_dict(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Ints", "Doubles", "Strings"]
        s = k.Schema(types, names)

        with self.assertRaises(RuntimeError):
            s.serialize()

    def test_wrong_type_throws(self):
        types = [k.int32(), int, k.double(), k.string()]
        names = ["Ints", "Longs", "Double", "Strings"]

        with self.assertRaises(TypeError):
            s = k.Schema(types, names)

        with self.assertRaises(TypeError):
            c = k.Column(int, "Longs")

        with self.assertRaises(TypeError):
            c = k.Column(k.int32(), 12)

    def test_column_creation_from_extension_type(self):
        # note: extension types are NOT passed through k.logical() here!
        types = [dt.date, dt.time, dt.datetime]
        names = ["Date", "Time", "DateTime"]

        columns = [k.Column(t, n) for t, n in zip(types, names)]

        for i, t in enumerate(types):
            self.assertEqual(k.logical(t), columns[i].ktype)

        s1 = k.Schema.from_columns(columns)
        s2 = k.Schema(types, names)

        self.assertEqual(s1, s2)

    def test_column_creation_from_unkown_type_fails(self):
        # note: extension types are NOT passed through k.logical() here!
        class UnknownType(str):
            pass

        with self.assertRaises(TypeError):
            k.logical(UnknownType)

        with self.assertRaises(TypeError):
            k.Column(UnknownType, "Unknown Type Column")

        with self.assertRaises(TypeError):
            k.Schema([UnknownType], ["Unknown Type Column"])

    def test_to_str(self):
        types = [
            k.int32(),
            k.int64(),
            k.double(),
            k.string(),
            k.list_(k.bool_()),
            k.struct(k.int64(), k.string()),
        ]
        names = ["Ints", "Longs", "Doubles", "Strings", "List", "Struct"]
        s = k.Schema(types, names)
        self.assertEqual("int32", str(k.int32()))
        self.assertEqual("int64", str(k.int64()))
        self.assertEqual("string", str(k.string()))
        self.assertEqual("double", str(k.double()))
        self.assertEqual("bool", str(k.bool_()))
        self.assertEqual("list<bool>", str(k.list_(k.bool_())))
        self.assertEqual("struct<int64, string>", str(k.struct(k.int64(), k.string())))

        sep = ",\n\t"
        self.assertEqual(
            str(s),
            f"Schema<\n\t{sep.join(str(k.Column(t,n,None)) for t,n in zip(types, names))}>",
        )

    def test_logical_type_wrapping(self):
        types = [
            k.int32(),
            k.int64(),
            k.double(),
            k.string(),
            k.bool_(),
            k.null(),
            k.list_(k.int32()),
            k.list_(k.int64()),
            k.list_(k.double()),
            k.list_(k.string()),
            k.list_(k.bool_()),
            k.list_(k.null()),
        ]
        names = [
            "Int",
            "Long",
            "Double",
            "String",
            "Bool",
            "Null",
            "Ints",
            "Longs",
            "Doubles",
            "Strings",
            "Bools",
            "Nulls",
        ]
        s = k.Schema(types, names)
        knime_schema = s.serialize()
        traits = knime_schema["schema"]["traits"]

        # all data types must be wrapped in a logical type and first column must be row key
        self.assertTrue(all("logical_type" in t["traits"] for t in traits))

        self.assertTrue(k._row_key_type == traits[0]["traits"]["logical_type"])
        self.assertTrue(
            all(
                k._logical_list_type != t["traits"]["logical_type"] for t in traits[:-1]
            )
        )

        # check roundtrip leads to equality
        out_s = k.Schema.deserialize(knime_schema)
        self.assert_schema_dict_equality(knime_schema, out_s.serialize())
        self.assertEqual(s, out_s)

    def test_extension_type_wrapping(self):
        _register_extension_types()

        types = [
            k.logical(dt.date),
            k.logical(dt.time),
            k.logical(dt.datetime),
        ]
        names = ["Date", "Time", "DateTime"]
        s = k.Schema(types, names)
        knime_schema = s.serialize()
        traits = knime_schema["schema"]["traits"]

        # all data types must be wrapped in a logical type and first column must be row key
        self.assertTrue(all("logical_type" in t["traits"] for t in traits))
        self.assertTrue(k._row_key_type == traits[0]["traits"]["logical_type"])

        # check roundtrip leads to equality
        out_s = k.Schema.deserialize(knime_schema)
        self.assert_schema_dict_equality(knime_schema, out_s.serialize())
        self.assertEqual(s, out_s)

    def test_list_wrapping(self):
        _register_extension_types()
        types = [
            k.list_(k.logical(dt.date)),
            k.list_(k.logical(dt.time)),
            k.list_(k.logical(dt.datetime)),
            k.list_(k.struct(k.int64(), k.string())),
        ]
        names = ["Dates", "Times", "DateTimes", "Structs"]
        s = k.Schema(types, names)
        knime_schema = s.serialize()
        traits = knime_schema["schema"]["traits"]

        # all data types must be wrapped in a logical type and first column must be row key
        self.assertTrue(all("logical_type" in t["traits"] for t in traits))
        self.assertTrue(k._row_key_type == traits[0]["traits"]["logical_type"])
        self.assertTrue(
            all(k._logical_list_type == t["traits"]["logical_type"] for t in traits[1:])
        )

        # check roundtrip leads to equality
        out_s = k.Schema.deserialize(knime_schema)
        self.assert_schema_dict_equality(knime_schema, out_s.serialize())
        self.assertEqual(s, out_s)

    def test_serialization_roundtrip(self):
        _register_extension_types()

        # load a JSON schema as it is coming from KNIME for a table created with the Test Data Generator
        with open(
            os.path.normpath(os.path.join(__file__, "..", "schema.json")), "rt"
        ) as f:
            table_schema = json.load(f)

        specs = table_schema["schema"]["specs"]
        traits = table_schema["schema"]["traits"]
        names = table_schema["columnNames"]
        metadata = table_schema["columnMetaData"]

        # perform roundtrip
        s = k.Schema.deserialize(table_schema)
        # print(s)
        out_schema = s.serialize()
        # with open("out_schema.json", "wt") as f:
        #     json.dump(out_schema, f, indent=4)

        self.assert_schema_dict_equality(table_schema, out_schema)

    def assert_schema_dict_equality(self, schema_a_dict, schema_b_dict):
        self.assertEqual(schema_a_dict["columnNames"], schema_b_dict["columnNames"])
        self.assertEqual(
            schema_a_dict["columnMetaData"], schema_b_dict["columnMetaData"]
        )
        a_specs = schema_a_dict["schema"]["specs"]
        a_traits = schema_a_dict["schema"]["traits"]
        b_specs = schema_b_dict["schema"]["specs"]
        b_traits = schema_b_dict["schema"]["traits"]
        self.assertEqual(len(a_specs), len(b_specs))
        self.assertEqual(len(a_traits), len(b_traits))

        for i in range(len(a_specs)):
            self.assertEqual(
                a_specs[i],
                b_specs[i],
                f"Specs of Col {i} differ: {a_specs[i]}, {b_specs[i]}",
            )

        for i in range(len(a_traits)):
            self.assertEqual(
                a_traits[i],
                b_traits[i],
                f"Traits of Col {i} differ: {a_traits[i]}, {b_traits[i]}",
            )

    def test_pyarrow_and_pandas_extension_types(self):
        """Tests the methods `to_pandas()` and `to_pyarrow()` of `knime_schema.LogicalType`.
        Should give back `knime._arrow._types.LogicalTypeExtensionType` and `knime._arrow._pandas.PandasLogicalTypeExtensionType`
        """
        import pyarrow as pa
        import knime.types.builtin as et
        import knime._arrow._types as kat

        logical_type = k.logical(dt.time)
        pandas_dtype = logical_type.to_pandas()
        pyarrow_extension_type = logical_type.to_pyarrow()
        # Test pandas dtype
        self.assertEqual(
            pandas_dtype.name,
            'PandasLogicalTypeExtensionType(int64, {"value_factory_class":"org.knime.core.data.v2.time.LocalTimeValueFactory"})',
        )
        # Test pyarrow extension type
        self.assertEqual(pyarrow_extension_type.storage_type, pa.int64())
        self.assertEqual(
            pyarrow_extension_type.logical_type,
            '{"value_factory_class":"org.knime.core.data.v2.time.LocalTimeValueFactory"}',
        )
        self.assertEqual(
            pyarrow_extension_type._converter.__class__, et.LocalTimeValueFactory
        )
        self.assertEqual(pyarrow_extension_type.__class__, kat.LogicalTypeExtensionType)

    def test_data_spec_to_arrow(self):
        """Tests the method knime._arrow._types._data_spec_to_arrow with two nested scenarios
        to see (amongst others) that lists can contain other lists or structs."""
        import knime._arrow._types as kat
        import pyarrow as pa

        data_spec = {
            "type": "struct",
            "inner_types": [
                {"type": "struct", "inner_types": ["long", "long"]},
                {"type": "list", "inner_type": "long"},
            ],
        }
        pa_storage_type = kat.data_spec_to_arrow(data_spec=data_spec)
        pa_type = pa.struct(
            [
                ("0", pa.struct([("0", pa.int64()), ("1", pa.int64())])),
                ("1", pa.large_list(pa.int64())),
            ]
        )
        self.assertEqual(
            str(pa_storage_type),
            "struct<0: struct<0: int64, 1: int64>, 1: large_list<item: int64>>",
        )
        self.assertEqual(pa_storage_type, pa_type)
        data_spec2 = {
            "type": "struct",
            "inner_types": [
                {
                    "type": "struct",
                    "inner_types": [
                        {"type": "struct", "inner_types": ["long", "long"]},
                        {
                            "type": "struct",
                            "inner_types": [
                                {"type": "list", "inner_type": "long"},
                                {"type": "struct", "inner_types": ["long", "long"]},
                            ],
                        },
                    ],
                },
                {
                    "type": "list",
                    "inner_type": {
                        "type": "struct",
                        "inner_types": [{"type": "list", "inner_type": "long"}, "long"],
                    },
                },
            ],
        }
        pa_storage_type2 = kat.data_spec_to_arrow(data_spec=data_spec2)
        self.assertEqual(
            str(pa_storage_type2),
            "struct<0: struct<0: struct<0: int64, 1: int64>, 1: struct<0: large_list<item: int64>, 1: struct<0: int64, 1: int64>>>, 1: large_list<item: struct<0: large_list<item: int64>, 1: int64>>>",
        )


class MyTime:
    def __init__(self, nano_of_day):
        self.nano_of_day = float(nano_of_day)

    def __str__(self):
        return f"MyTime(nano_of_day={self.nano_of_day})"


class MyLocalTimeValueFactory(kt.PythonValueFactory):
    def __init__(self) -> None:
        kt.PythonValueFactory.__init__(self, MyTime)

    def decode(self, nano_of_day):
        if nano_of_day is None:
            return None
        return MyTime(nano_of_day)

    def encode(self, time):
        if time is None:
            return None
        return int(time.nano_of_day)


class ProxyTests(unittest.TestCase):
    def test_proxy_types(self):
        """
        Tests knime_schema.logical with registered proxy types.
        """
        import pyarrow as pa
        import knime._arrow._types as kat

        _register_extension_types()
        data_spec_json = '"long"'
        data_traits = """
                {
                    "type": "simple",
                    "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.LocalTimeValueFactory\\"}" }
                }
                """
        kt.register_python_value_factory(
            __name__,
            "MyLocalTimeValueFactory",
            data_spec_json,
            data_traits,
            "test_knime_schema.MyTime",
            False,
        )

        EXPECTED_VALUE_FACTORY = '{"value_factory_class":"org.knime.core.data.v2.time.LocalTimeValueFactory"}'

        logical_type = k.logical(MyTime)
        self.assertEqual(k.int64(), logical_type.storage_type)
        self.assertEqual(EXPECTED_VALUE_FACTORY, logical_type.logical_type)
        self.assertEqual(MyTime, logical_type.proxy_type)

        # Test pandas dtype
        pandas_dtype = logical_type.to_pandas()
        self.assertEqual(
            pandas_dtype.name,
            f"PandasLogicalTypeExtensionType(int64, {EXPECTED_VALUE_FACTORY})",
        )

        # Test pyarrow extension type
        pyarrow_extension_type = logical_type.to_pyarrow()
        self.assertEqual(pyarrow_extension_type.storage_type, pa.int64())
        self.assertEqual(
            pyarrow_extension_type.logical_type,
            EXPECTED_VALUE_FACTORY,
        )
        self.assertEqual(
            pyarrow_extension_type._converter.__class__, MyLocalTimeValueFactory
        )
        self.assertEqual(pyarrow_extension_type.__class__, kat.ProxyExtensionType)

        import knime._arrow._types as katy

        proxy_type = str(pyarrow_extension_type)
        p_string = katy.extract_string_from_pa_dtype(pyarrow_extension_type)
        self.assertEqual(p_string, proxy_type)

    def test_proxy_types_to_string(self):
        """
        Tests knime_schema.logical with registered proxy types.
        """

        _register_extension_types()
        data_spec_json = '"long"'
        data_traits = """
                {
                    "type": "simple",
                    "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.LocalTimeValueFactory\\"}" }
                }
                """
        kt.register_python_value_factory(
            __name__,
            "MyLocalTimeValueFactory",
            data_spec_json,
            data_traits,
            "test_knime_schema.MyTime",
            False,
        )

        logical_type = k.logical(MyTime)
        pyarrow_extension_type = logical_type.to_pyarrow()

        proxy_type = str(pyarrow_extension_type)
        p_string = katy.extract_string_from_pa_dtype(pyarrow_extension_type)
        self.assertEqual(p_string, proxy_type)

        p_type = katy.extract_pa_dtype_from_string(proxy_type)
        self.assertEqual(p_type, pyarrow_extension_type)

    def test_type_casting(self):
        """
        Tests knime_schema.logical with registered proxy types.
        """
        import pyarrow as pa
        import knime._arrow._types as kat

        _register_extension_types()
        kt._python_proxy_type_to_factory_info[MyTime] = (
            MyLocalTimeValueFactory(),
            dt.time,
        )

        orig_type = k.logical(dt.time)
        proxy_type = k.logical(MyTime)

        import pandas as pd

        df = pd.DataFrame()
        t = dt.time(13, 37, 42)
        df["times"] = pd.Series([t, t], dtype=orig_type.to_pandas())
        df["casted-times"] = df["times"].astype(proxy_type.to_pandas())
        self.assertEqual(49062000000000, df["casted-times"][0].nano_of_day)

        df["casted-times"][1] = MyTime(df["casted-times"][1].nano_of_day + 10_000)
        df["roundtrip"] = df["casted-times"].astype(orig_type.to_pandas())
        self.assertEqual(df["times"][0], df["roundtrip"][0])

        expected = (
            dt.datetime.combine(dt.date.today(), df["times"][1])
            + dt.timedelta(microseconds=10)
        ).time()
        self.assertEqual(expected, df["roundtrip"][1])

    def test_print_byte_vector(self):
        _register_extension_types()

        import knime.types.builtin as et

        knime_type = k.logical(et.DenseByteVectorValue)

        import pandas as pd

        df = pd.DataFrame()
        v = "This is a byte 🖤 string".encode()
        df["byte_vectors"] = pd.Series([v, v, v], dtype=knime_type.to_pandas())
        # Try to convert to string because that caused an exception inside Pandas
        # when we did not implement __array__ in the KnimePandasExtensionArray
        str(df)


if __name__ == "__main__":
    unittest.main()
