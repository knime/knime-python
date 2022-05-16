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

from typing import List, Tuple
import pythonpath  # adds knime_node to the Python path
import knime_node as kn
import knime_node_parameter as knp
import knime_table as kt
import knime_schema as ks
from packaging.version import Version
import unittest


class MyDecoratedNode(kn.PythonNode):
    def __init__(self) -> None:
        super().__init__()
        self._param1 = 42
        self._param2 = "awesome"
        self._backwards_compat_param = "Some parameter introduced in a later version"

    @kn.parameter
    def param2(self):
        return self._param2

    @param2.setter
    def param2(self, value):
        self._param2 = value

    @kn.rule(
        effect="ENABLE", scope=param2, schema={"enum": ["foo"]}
    )  # not evaluated yet
    @kn.ui(label="My first parameter")  # not evaluated yet
    @kn.parameter
    def param1(self):
        return self._param1

    @param1.setter
    def param1(self, value):
        if value < 0:
            raise ValueError("The value must be non-negative.")
        self._param1 = value

    @kn.parameter(since_version=Version("4.6.0"))
    def backwards_compatible_parameter(self):
        return self._backwards_compat_param

    @backwards_compatible_parameter.setter
    def backwards_compatible_parameter(self, value):
        self._backwards_compat_param = value

    def configure(self, input_schemas: List[ks.Schema]) -> List[ks.Schema]:
        return input_schemas

    def execute(
        self, tables: List[kt.ReadTable], objects: List, exec_context
    ) -> Tuple[List[kt.WriteTable], List]:
        out_t = [t for t in tables]
        return (out_t, objects)


class DescriptorTest(unittest.TestCase):
    def setUp(self):
        self.node = MyDecoratedNode()

    def test_extract_parameters(self):
        params = knp.extract_parameters(self.node)
        expected = {
            "param1": 42,
            "param2": "awesome",
            "backwards_compatible_parameter": "Some parameter introduced in a later version",
        }
        self.assertEqual(expected, params)
        self.node.param2 = "foo"
        expected["param2"] = "foo"
        self.assertEqual(expected, knp.extract_parameters(self.node))

    def test_extract_validate(self):
        params = knp.extract_parameters(self.node)
        version = Version("4.6.0")
        self.assertIsNone(knp.validate_parameters(self.node, params, version))
        params["param1"] = 3
        self.assertIsNone(knp.validate_parameters(self.node, params, version))
        params["param1"] = -1
        self.assertEqual(
            "The value must be non-negative.",
            knp.validate_parameters(self.node, params, version),
        )

    def test_extract_inject(self):
        params = knp.extract_parameters(self.node)
        version = Version("4.6.0")
        knp.inject_parameters(self.node, params, version)
        params["param1"] = 3
        knp.inject_parameters(self.node, params, version)
        assert self.node.param1 == 3
        params["param1"] = -1
        try:
            knp.inject_parameters(self.node, params, version)
            assert False
        except ValueError:
            pass
        assert self.node.param1 == 3

    def test_get_schema(self):
        schema = knp.extract_schema(self.node)
        expected_schema = {
            "type": "object",
            "properties": {
                "param1": {"type": "number"},
                "param2": {"type": "string"},
                "backwards_compatible_parameter": {"type": "string"},
            },
        }
        self.assertEqual(expected_schema, schema)

    def test_get_ui_schema(self):
        ui_schema = knp.extract_ui_schema(self.node)
        expected_schema = {
            "type": "VerticalLayout",
            "elements": [
                {"type": "Control", "scope": "#/properties/param2"},
                {"type": "Control", "scope": "#/properties/param1"},
                {
                    "type": "Control",
                    "scope": "#/properties/backwards_compatible_parameter",
                },
            ],
        }
        self.assertEqual(expected_schema, ui_schema)

    def test_parameter_versioning(self):
        old_parameters = {"param1": 42, "param2": "foobar"}
        version = Version("4.5.1")
        self.assertIsNone(knp.validate_parameters(self.node, old_parameters, version))
        knp.inject_parameters(self.node, old_parameters, version)
        self.assertEqual(
            "Missing the parameter backwards_compatible_parameter.",
            knp.validate_parameters(self.node, old_parameters, Version("4.6.0")),
        )
