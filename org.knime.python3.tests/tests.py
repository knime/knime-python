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
@author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
"""
import unittest
import kg_pythonpath

import knime_gateway as kg


class JavaFooDataSource:
    def __init__(self, value) -> None:
        self.value = value

    def getIdentifier(self):  # NOSONAR
        return "foo"


class JavaFooDataSink:
    def setValue(self, value):  # NOSONAR
        self.value = value

    def getIdentifier(self):  # NOSONAR
        return "foo"


class FooDataSource:
    def __init__(self, java_data_source) -> None:
        self.s = java_data_source
        self.entered = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.entered = False
        return False

    def get_value(self):
        return self.s.value


class FooDataSink:
    def __init__(self, java_data_sink) -> None:
        self.s = java_data_sink
        self.entered = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.entered = False
        return False

    def set_value(self, value):
        return self.s.setValue(value)


class TestDataSourceSinkMapping(unittest.TestCase):
    def test_source_map(self):
        in_obj = JavaFooDataSource(2)

        @kg.data_source("foo")
        def foo_mapper(obj):
            self.assertEqual(in_obj, obj)
            return FooDataSource(obj)

        out_obj = kg.data_source_mapper(in_obj)
        self.assertIsInstance(out_obj, FooDataSource)
        self.assertEqual(2, out_obj.get_value())

    def test_sink_map(self):
        in_obj = JavaFooDataSink()

        @kg.data_sink("foo")
        def foo_mapper(obj):
            self.assertEqual(in_obj, obj)
            return FooDataSink(obj)

        out_obj = kg.data_sink_mapper(in_obj)
        self.assertIsInstance(out_obj, FooDataSink)
        out_obj.set_value(2)
        self.assertEqual(2, in_obj.value)

    def test_map_multiple_sources(self):
        in_objs = [JavaFooDataSource(i) for i in range(10)]

        @kg.data_source("foo")
        def foo_mapper(obj):
            return FooDataSource(obj)

        out_objs = [kg.data_source_mapper(i) for i in in_objs]
        with kg.SequenceContextManager(out_objs):
            for i, o in enumerate(out_objs):
                self.assertIsInstance(o, FooDataSource)
                self.assertEqual(i, o.get_value())
                self.assertTrue(o.entered)

        for o in out_objs:
            self.assertFalse(o.entered)

    def test_map_multiple_sinks(self):
        in_objs = [JavaFooDataSink() for _ in range(10)]

        @kg.data_sink("foo")
        def foo_mapper(obj):
            return FooDataSink(obj)

        out_objs = [kg.data_sink_mapper(i) for i in in_objs]
        with kg.SequenceContextManager(out_objs):
            for i, o in enumerate(out_objs):
                self.assertIsInstance(o, FooDataSink)
                o.set_value(i)
                self.assertTrue(o.entered)

        for o in out_objs:
            self.assertFalse(o.entered)

        for i, in_obj in enumerate(in_objs):
            self.assertEqual(i, in_obj.value)


if __name__ == "__main__":
    unittest.main()
