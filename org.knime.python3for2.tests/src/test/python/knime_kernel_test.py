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
@author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
"""

import math
import unittest
from typing import Optional, Union

import knime_gateway as kg
from knime_kernel import PythonKernel
from knime_testing import PythonTestResult


class Python3KernelBackendProxyTestRunner(kg.EntryPoint):
    def testPutTableIntoWorkspace(self, java_table_data_source):
        suite = unittest.TestSuite(
            [
                # Manually construct test case to be able to inject the Java data source.
                PutTableIntoWorkspaceTest(
                    "testPutTableIntoWorkspace", java_table_data_source
                )
            ]
        )
        # TextTestRunner returns test results of type TextTestResult.
        return PythonTestResult(unittest.TextTestRunner(verbosity=0).run(suite))


class PutTableIntoWorkspaceTest(unittest.TestCase):
    def __init__(self, test_name, java_table_data_source):
        super(PutTableIntoWorkspaceTest, self).__init__(test_name)
        self._java_table_data_source = java_table_data_source

    def testPutTableIntoWorkspace(self):
        for num_rows in [60, 50]:
            for sentinel in [None, "min", "max", 123]:
                with self.subTest(num_rows=num_rows, sentinel=sentinel):
                    self._testPutTableIntoWorkspace(
                        self._java_table_data_source, num_rows, sentinel
                    )

    def _testPutTableIntoWorkspace(
        self, java_table_data_source, num_rows: int, sentinel: Optional[Union[str, int]]
    ):
        kernel = PythonKernel()
        variable_name = "my_test_table"
        assert variable_name not in kernel._workspace
        kernel.putTableIntoWorkspace(
            variable_name, java_table_data_source, num_rows, sentinel
        )
        assert variable_name in kernel._workspace
        table = kernel._workspace[variable_name]
        assert table is not None

        self.assertEqual(4, len(table.columns))
        my_double_col_idx = 0
        my_int_col_idx = 1
        my_long_col_idx = 2
        my_string_col_idx = 3
        my_double_col_name = table.columns[my_double_col_idx]
        my_int_col_name = table.columns[my_int_col_idx]
        my_long_col_name = table.columns[my_long_col_idx]
        my_string_col_name = table.columns[my_string_col_idx]
        self.assertEqual("My double col", my_double_col_name)
        self.assertEqual("My int col", my_int_col_name)
        self.assertEqual("My long col", my_long_col_name)
        self.assertEqual("My string col", my_string_col_name)
        self.assertEqual("float64", table[my_double_col_name].dtype)
        if sentinel is None:
            self.assertEqual("float64", table[my_int_col_name].dtype)
            self.assertEqual("float64", table[my_long_col_name].dtype)
        else:
            self.assertEqual("int32", table[my_int_col_name].dtype)
            self.assertEqual("int64", table[my_long_col_name].dtype)
        self.assertEqual("object", table[my_string_col_name].dtype)

        self.assertEqual(num_rows, len(table))
        for i, (index, row) in enumerate(table.iterrows()):
            self.assertEqual(f"Row{i}", index)
            my_double = row[my_double_col_idx]
            my_int = row[my_int_col_idx]
            my_long = row[my_long_col_idx]
            my_string = row[my_string_col_idx]
            expect_missings = i % 13 == 0
            if expect_missings:
                self.assertIsNan(my_double)
                if sentinel is None:
                    self.assertIsNan(my_int)
                    self.assertIsNan(my_long)
                else:
                    if sentinel == "min":
                        int_sentinel = -(2 ** 31)
                        long_sentinel = -(2 ** 63)
                    elif sentinel == "max":
                        int_sentinel = 2 ** 31 - 1
                        long_sentinel = 2 ** 63 - 1
                    else:
                        int_sentinel = long_sentinel = int(sentinel)
                    self.assertEqual(int_sentinel, my_int)
                    self.assertEqual(long_sentinel, my_long)
                self.assertEqual(None, my_string)
            else:
                self.assertEqual(float(i), my_double)
                self.assertEqual(i * 2, my_int)
                self.assertEqual(i * 10, my_long)
                self.assertEqual(f"This is row {i}", my_string)

    def assertIsNan(self, obj):
        is_nan = False
        try:
            is_nan = math.isnan(obj)
        except TypeError:
            pass
        if not is_nan:
            self.fail(f"{obj} is not {math.nan}")

    class Java:
        implements = [
            "org.knime.python3for2.Python3KernelBackendProxyTest.Python3KernelBackendProxyTestRunner"
        ]


kg.connect_to_knime(Python3KernelBackendProxyTestRunner())
