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
@author Steffen Fissler, KNIME GmbH, Konstanz, Germany
"""

import unittest
import knime.api.schema as ks
import knime.api.types as kt
import logging
import sys

LOGGER = logging.getLogger(__name__)


class TypesTest(unittest.TestCase):
    def test_types_not_automatically_loaded(self):
        python_type_name = "lala.TestValue"
        data_traits_mockup = """
                {
                    "type": "simple",
                    "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.Mockup\\"}" }
                }
                """
        logical_type_string = '{"value_factory_class":"org.knime.Mockup"}'

        kt.register_python_value_factory(
            "testing_module",
            "TestValueFactory",
            "TestTypeName",
            '"long"',
            data_traits_mockup,
            python_type_name,
        )

        # Verify that the module is only loaded when we access its factory
        bundle = kt.get_value_factory_bundle_for_java_value_factory(logical_type_string)
        sys.modules.pop("testing_module", None)
        factory = bundle.value_factory
        self.assertTrue("testing_module" in sys.modules)

        # Verify that the module works as expected
        encoded_value = factory.encode(8)
        self.assertIsInstance(encoded_value, str)

        import testing_module as tm

        decoded_value = factory.decode(encoded_value)
        self.assertIsInstance(decoded_value, tm.TestValue)


if __name__ == "__main__":
    unittest.main()
