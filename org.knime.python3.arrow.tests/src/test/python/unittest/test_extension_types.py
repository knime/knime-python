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
@author Jonas Klotz, KNIME GmbH, Berlin, Germany
"""
import unittest

import extension_types as et


class TimeExtensionTypeTest(unittest.TestCase):
    _too_large_int = (
        100000000000000000000000000000000  # for testing integers too large to show in c
    )
    _too_large_days = 3649635  # for testing days out of the possible DateTime interval
    _too_large_nanos = (
        999999999999999999999999  # for testing ns out of the possible DateTime interval
    )

    def test_zoned_date_time_value_factory2(self):
        factory = et.ZonedDateTimeValueFactory2()
        storage = {
            "0": self._too_large_days,  # days since epoch
            "1": 0,
            "2": 0,  # days since epoch
            "3": 0,
        }
        self.assertRaises(OverflowError, factory.decode, storage=storage)

    def test_local_date_time_value_factory(self):
        factory = et.LocalDateTimeValueFactory()
        storage = {"0": self._too_large_days, "1": 0}  # days since epoch  # nanoseconds

        self.assertRaises(OverflowError, factory.decode, storage=storage)

        storage = {
            "0": 0,  # days since epoch
            "1": self._too_large_int,
        }  # duration: nanoseconds

        self.assertRaises(OverflowError, factory.decode, storage=storage)

    def test_duration_value_factory(self):
        factory = et.DurationValueFactory()
        storage = {
            "0": self._too_large_int,  # duration: seconds
            "1": self._too_large_int,
        }  # duration: nanoseconds

        self.assertRaises(OverflowError, factory.decode, storage=storage)

        storage = {
            "0": 0,  # duration: seconds
            "1": self._too_large_nanos,
        }  # duration: nanoseconds

        self.assertRaises(OverflowError, factory.decode, storage=storage)

    def test_local_date_value_factory(self):
        factory = et.LocalDateValueFactory()
        self.assertRaises(
            OverflowError, factory.decode, day_of_epoch=self._too_large_days
        )
        self.assertRaises(
            OverflowError, factory.decode, day_of_epoch=self._too_large_int
        )

    def test_local_time_value_factory(self):
        factory = et.LocalTimeValueFactory()
        self.assertRaises(
            OverflowError, factory.decode, nano_of_day=self._too_large_int
        )
        self.assertRaises(
            OverflowError, factory.decode, nano_of_day=self._too_large_nanos
        )
