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
import unittest
import json
import datetime as dt

import pythonpath
import knime_schema as k


class TypeTest(ABC):
    @abstractmethod
    def create_type(self):
        pass

    @abstractmethod
    def isinstance(self, o):
        pass

    def test_type(self):
        self.assertTrue(self.isinstance(self.create_type()))
        self.assertIsInstance(self.create_type(), k.KnimeType)

    def test_equals(self):
        self.assertTrue(self.create_type() == self.create_type())
        self.assertFalse(self.create_type() != self.create_type())


class IntTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.int32()

    def isinstance(self, o):
        return isinstance(o, k.IntType)


class BoolTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.bool_()

    def isinstance(self, o):
        return isinstance(o, k.BoolType)


class BlobTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.blob()

    def isinstance(self, o):
        return isinstance(o, k.BlobType)


class LongTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.int64()

    def isinstance(self, o):
        return isinstance(o, k.LongType)


class StringTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.string()

    def isinstance(self, o):
        return isinstance(o, k.StringType)


class DoubleTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.double()

    def isinstance(self, o):
        return isinstance(o, k.DoubleType)


class IntListTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.list_(k.int32())

    def isinstance(self, o):
        return isinstance(o, k.ListType)


class IntStringStructTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.struct_(k.int32(), k.string())

    def isinstance(self, o):
        return isinstance(o, k.StructType)


class ExtensionStringTest(TypeTest, unittest.TestCase):
    def create_type(self):
        return k.ExtensionType(k._knime_logical_type("StringValueFactory"), k.string())

    def isinstance(self, o):
        return isinstance(o, k.ExtensionType)

    def test_value_type_fails(self):
        with self.assertRaises(TypeError):
            t = self.create_type()
            print(t.value_type)


class ExtensionTimeTest(TypeTest, unittest.TestCase):
    """
    This test shows how extension types can be created
    with KNIME's Python type system
    """

    def setUpClass():
        _register_extension_types()

    def create_type(self):
        return k.extension(dt.date)

    def isinstance(self, o):
        return isinstance(o, k.ExtensionType)

    def test_value_type(self):
        t = self.create_type()
        self.assertEqual(dt.date, t.value_type)


class UnknownExtensionTest(unittest.TestCase):
    def test_unknown_extension_creation_fails(self):
        class Dummy:
            pass

        with self.assertRaises(TypeError):
            t = k.extension(Dummy())

        with self.assertRaises(TypeError):
            t = k.extension(int)


class KnimeType(unittest.TestCase):
    def test_knime_type_cannot_be_created(self):
        with self.assertRaises(TypeError):
            t = k.KnimeType()


# ----------------------------------------------------------


def _register_extension_types():
    import knime_types as kt

    kt.register_python_value_factory(
        "extension_types",
        "LocalTimeValueFactory",
        '"long"',
        """
                {
                    "type": "simple", 
                    "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.LocalTimeValueFactory\\"}" }
                }
                """,
    )

    kt.register_python_value_factory(
        "extension_types",
        "LocalDateValueFactory",
        '"long"',
        """
                {
                    "type": "simple", 
                    "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.LocalDateValueFactory\\"}" }
                }
                """,
    )

    kt.register_python_value_factory(
        "extension_types",
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
    )


# ----------------------------------------------------------


class SchemaTest(unittest.TestCase):
    def test_creation_without_metadata(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = k.Schema.from_types(types, names)
        self.assertEqual(len(types), len(s))
        for i, (t, n) in enumerate(zip(types, names)):
            self.assertIsInstance(s[i], k.Column)
            self.assertEqual(t, s[i].type)
            self.assertEqual(n, s[i].name)
            self.assertIsNone(s[i].metadata)

    def test_creation_with_metadata(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = k.Schema.from_types(types, names, names)
        self.assertEqual(len(types), len(s))
        for i, (t, n) in enumerate(zip(types, names)):
            self.assertIsInstance(s[i], k.Column)
            self.assertEqual(t, s[i].type)
            self.assertEqual(n, s[i].name)
            self.assertEqual(n, s[i].metadata)

    def test_column_creation_without_metadata(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        columns = [k.Column(t, n, None) for t, n in zip(types, names)]
        s = k.Schema.from_columns(columns)
        self.assertEqual(len(types), len(s))
        for i, (t, n) in enumerate(zip(types, names)):
            self.assertIsInstance(s[i], k.Column)
            self.assertEqual(t, s[i].type)
            self.assertEqual(n, s[i].name)
            self.assertIsNone(s[i].metadata)

    def test_column_creation_with_metadata(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        columns = [k.Column(t, n, n) for t, n in zip(types, names)]
        s = k.Schema.from_columns(columns)
        self.assertEqual(len(types), len(s))
        for i, (t, n) in enumerate(zip(types, names)):
            self.assertIsInstance(s[i], k.Column)
            self.assertEqual(t, s[i].type)
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
        self.assertEqual(2 * len(s), len(s_double))
        self.assertEqual(2 * len(s_with_m), len(s_with_m_double))

        added_no_meta = s.append(s)
        added_with_meta = s_with_m.append(s_with_m)
        self.assertEqual(s_double, added_no_meta)
        self.assertEqual(s_with_m_double, added_with_meta)

    def test_slicing(self):
        types = [k.int32(), k.int64(), k.double(), k.string()]
        names = ["Ints", "Longs", "Doubles", "Strings"]
        s = k.Schema(types, names)
        sl = s[1:3]
        self.assertEqual(2, len(sl))
        self.assertEqual(["Longs", "Doubles"], sl.column_names)

    def test_to_str(self):
        types = [
            k.int32(),
            k.int64(),
            k.double(),
            k.string(),
            k.list_(k.bool_()),
            k.struct_(k.int64(), k.string()),
        ]
        names = ["Ints", "Longs", "Doubles", "Strings", "List", "Struct"]
        s = k.Schema(types, names)
        self.assertEqual("int32", str(k.int32()))
        self.assertEqual("int64", str(k.int64()))
        self.assertEqual("string", str(k.string()))
        self.assertEqual("double", str(k.double()))
        self.assertEqual("bool", str(k.bool_()))
        self.assertEqual("list<bool>", str(k.list_(k.bool_())))
        self.assertEqual("struct<int64, string>", str(k.struct_(k.int64(), k.string())))

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
            k.list_(k.int32()),
            k.list_(k.int64()),
            k.list_(k.double()),
            k.list_(k.string()),
            k.list_(k.bool_()),
        ]
        names = [
            "Int",
            "Long",
            "Double",
            "String",
            "Bool",
            "Ints",
            "Longs",
            "Doubles",
            "Strings",
            "Bools",
        ]
        s = k.Schema(types, names)
        knime_schema = s.to_knime_dict()
        traits = knime_schema["schema"]["traits"]

        # all data types must be wrapped in a logical type and first column must be row key
        self.assertTrue(all("logical_type" in t["traits"] for t in traits))
        self.assertTrue(k._row_key_type == traits[0]["traits"]["logical_type"])
        self.assertTrue(
            all(k._logical_list_type != t["traits"]["logical_type"] for t in traits)
        )

        # check roundtrip leads to equality
        out_s = k.Schema.from_knime_dict(knime_schema)
        self.assert_schema_dict_equality(knime_schema, out_s.to_knime_dict())
        self.assertEqual(s, out_s)

    def test_extension_type_wrapping(self):
        _register_extension_types()

        types = [
            k.extension(dt.date),
            k.extension(dt.time),
            k.extension(dt.datetime),
        ]
        names = ["Date", "Time", "DateTime"]
        s = k.Schema(types, names)
        knime_schema = s.to_knime_dict()
        traits = knime_schema["schema"]["traits"]

        # all data types must be wrapped in a logical type and first column must be row key
        self.assertTrue(all("logical_type" in t["traits"] for t in traits))
        self.assertTrue(k._row_key_type == traits[0]["traits"]["logical_type"])

        # check roundtrip leads to equality
        out_s = k.Schema.from_knime_dict(knime_schema)
        self.assert_schema_dict_equality(knime_schema, out_s.to_knime_dict())
        self.assertEqual(s, out_s)

    def test_list_wrapping(self):
        _register_extension_types()
        types = [
            k.list_(k.extension(dt.date)),
            k.list_(k.extension(dt.time)),
            k.list_(k.extension(dt.datetime)),
            k.list_(k.struct_(k.int64(), k.string())),
        ]
        names = ["Dates", "Times", "DateTimes", "Structs"]
        s = k.Schema(types, names)
        knime_schema = s.to_knime_dict()
        traits = knime_schema["schema"]["traits"]

        # all data types must be wrapped in a logical type and first column must be row key
        self.assertTrue(all("logical_type" in t["traits"] for t in traits))
        self.assertTrue(k._row_key_type == traits[0]["traits"]["logical_type"])
        self.assertTrue(
            all(k._logical_list_type == t["traits"]["logical_type"] for t in traits[1:])
        )

        # check roundtrip leads to equality
        out_s = k.Schema.from_knime_dict(knime_schema)
        self.assert_schema_dict_equality(knime_schema, out_s.to_knime_dict())
        self.assertEqual(s, out_s)

    def test_serialization_roundtrip(self):
        _register_extension_types()

        # load a JSON schema as it is coming from KNIME for a table created with the Test Data Generator
        with open("schema.json", "rt") as f:
            table_schema = json.load(f)

        specs = table_schema["schema"]["specs"]
        traits = table_schema["schema"]["traits"]
        names = table_schema["columnNames"]
        metadata = table_schema["columnMetaData"]

        # perform roundtrip
        s = k.Schema.from_knime_dict(table_schema)
        # print(s)
        out_schema = s.to_knime_dict()
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


if __name__ == "__main__":
    unittest.main()

