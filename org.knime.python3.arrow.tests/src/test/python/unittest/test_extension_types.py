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

import knime.api.types as kt
import knime.types.builtin as et
import testing_utility


def _register_extension_types():
    ext_types = "knime.types.builtin"
    kt.register_python_value_factory(
        ext_types,
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
        ext_types,
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
        ext_types,
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
        ext_types,
        "ZonedDateTimeValueFactory2",
        '{"type": "struct", "inner_types": ["long", "long", "int", "string"]}',
        """
                    {
                        "type": "struct",
                        "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.ZonedDateTimeValueFactory2\\"}" },
                        "inner": [
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}}
                        ]
                    }
                    """,
        "datetime.datetime",
    )
    kt.register_python_value_factory(
        ext_types,
        "ZonedDateTimeValueFactory2",
        '{"type": "struct", "inner_types": ["long", "long", "int", "string"]}',
        """
                    {
                        "type": "struct",
                        "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.ZonedDateTimeValueFactory2\\"}" },
                        "inner": [
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}}
                        ]
                    }
                    """,
        "pandas._libs.tslibs.timestamps.Timestamp",
        is_default_python_representation=False,
    )
    kt.register_python_value_factory(
        ext_types,
        "DurationValueFactory",
        '{"type": "struct", "inner_types": ["long", "int"]}',
        """
                    {
                        "type": "struct",
                        "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.DurationValueFactory\\"}" },
                        "inner": [
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}}
                        ]
                    }
                    """,
        "datetime.timedelta",
    )
    kt.register_python_value_factory(
        ext_types,
        "DurationValueFactory",
        '{"type": "struct", "inner_types": ["long", "int"]}',
        """
                    {
                        "type": "struct",
                        "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.core.data.v2.time.DurationValueFactory\\"}" },
                        "inner": [
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}}
                        ]
                    }
                    """,
        "pandas._libs.tslibs.timedeltas.Timedelta",
        is_default_python_representation=False,
    )
    kt.register_python_value_factory(
        ext_types,
        "FSLocationValueFactory",
        '{"type": "struct", "inner_types": ["string", "string", "string"]}',
        """
                    {
                        "type": "struct",
                        "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.filehandling.core.data.location.FSLocationValueFactory\\"}" },
                        "inner": [
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}},
                            {"type": "simple", "traits": {}}
                        ]
                    }
                    """,
        "knime.types.builtin.FSLocationValue",
    )


class TimeExtensionTypeTest(unittest.TestCase):
    _too_large_int = (
        100000000000000000000000000000000  # for testing integers too large to show in c
    )
    _too_large_days = 3649635  # for testing days out of the possible DateTime interval
    _too_large_nanos = (
        999999999999999999999999  # for testing ns out of the possible DateTime interval
    )

    @classmethod
    def setUpClass(cls):
        # setup value factories for timestamps
        _register_extension_types()

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

    def test_passing_arrow_time_types_to_knime(self):
        import pyarrow as pa

        arrow_backend, node_backend = testing_utility._generate_backends()
        n = 15
        types = {
            "t32": pa.time32("ms"),  # no pandas
            "t64": pa.time64("ns"),  # no pandas
            "d32": pa.date32(),  # pandas
            "d64": pa.date64(),  # pandas
            "dur": pa.duration("ns"),  # pandas
            "tz_ts": pa.timestamp("ms", tz="America/New_York"),
        }
        schema = (
            "RowKey: string\n"
            "t32: time32[ms]\n"
            "t64: time64[ns]\n"
            "d32: date32[day]\n"
            "d64: date64[ms]\n"
            "dur: duration[ns]\n"
            "tz_ts: timestamp[ms, tz=America/New_York]"
        )
        # in pa versions lower than 9 timezones are parsed differently such that half tz's like tz=+07:30 do not work
        if int(pa.__version__.split(".")[0]) >= 9:
            types["half_tz_ts"] = pa.timestamp("s", tz="+07:30")
            types["half_tz_ts2"] = pa.timestamp("s", tz="-07:30")
            schema += (
                "\nhalf_tz_ts: timestamp[s, tz=+07:30]\n"
                "half_tz_ts2: timestamp[s, tz=-07:30]"
            )

        arrays = [pa.array([f"Row{i}" for i in range(n)])]
        for type_name in types.keys():
            dtype = types[type_name]
            arrays.append(pa.array([i * 10 for i in range(n)], type=dtype))

        names = ["RowKey"] + list(types.keys())
        table = pa.table(arrays, names=names)
        arrow_table = arrow_backend.write_table(table)  # send to KNIME
        self.assertEqual(str(arrow_table._schema), schema)

    def test_passing_pandas_time_types_to_knime(self):
        import pandas as pd

        arrow_backend, node_backend = testing_utility._generate_backends()

        timestamp_series = pd.Series(
            pd.date_range("2012-1-1", periods=3, freq="D"), name="timestamp"
        )

        timedelta_series = pd.Series(
            [pd.Timedelta(days=i) for i in range(3)], name="timedelta"
        )
        tz_timestamp_series = pd.Series(
            pd.date_range("3/6/2012 05:00", periods=3, freq="D", tz="America/New_York"),
            name="tz_timestamp",
        )

        content = [tz_timestamp_series, timestamp_series, timedelta_series]
        for ser in content:
            ser.index = [f"Row{i}" for i in range(3)]

        df = pd.concat(content, axis=1)
        empty_series = pd.Series([pd.NA] * len(df.columns), index=df.columns)
        df = df.append(empty_series, ignore_index=True)
        arrow_table = arrow_backend.write_table(df)
        schema = (
            "<Row Key>: string\n"
            "tz_timestamp: extension<knime.logical_type<LogicalTypeExtensionType>>\n"
            "timestamp: extension<knime.logical_type<LogicalTypeExtensionType>>\n"
            "timedelta: extension<knime.logical_type<LogicalTypeExtensionType>>"
        )
        # for pa 7 and 9 there is apparently different precision
        self.assertEqual(
            schema, arrow_table._schema.to_string(show_schema_metadata=False)
        )

    def test_passing_datetime_time_types_to_knime(self):
        """These are interpreted as objects in pandas"""
        from datetime import time, timedelta, datetime, date, timezone
        import pandas as pd

        arrow_backend, node_backend = testing_utility._generate_backends()

        df = testing_utility._generate_test_data_frame("dates.zip", columns=["date"])
        arrow_table = arrow_backend.write_table(df)
        schema = (
            "<Row Key>: string\n"
            "date: extension<knime.logical_type<LogicalTypeExtensionType>>"
        )
        self.assertEqual(
            schema, arrow_table._schema.to_string(show_schema_metadata=False)
        )

        # pandas timedelta,  pandas datetime and pandas datetime TZD type
        # are all handled by the column converter as they're not treated as objects
        time_delta_series = pd.Series(
            [timedelta(days=64, seconds=29156, microseconds=i) for i in range(3)]
        )
        datetime_series = pd.Series([datetime.now()] * 3)

        # object in pandas
        date_series = pd.Series([date(2002, 12, i) for i in range(1, 4)])
        time_series = pd.Series([time(hour=i) for i in range(1, 4)])

        content = [time_delta_series, datetime_series, date_series, time_series]

        for ser in content:
            ser.index = [f"Row{i}" for i in range(1, 4)]
        df = pd.concat(content, axis=1)

        arrow_table = arrow_backend.write_table(df)
        schema = (
            "<Row Key>: string\n"
            "0: extension<knime.logical_type<LogicalTypeExtensionType>>\n"
            "1: extension<knime.logical_type<LogicalTypeExtensionType>>\n"
            "2: extension<knime.logical_type<LogicalTypeExtensionType>>\n"
            "3: extension<knime.logical_type<LogicalTypeExtensionType>>"
        )
        self.assertEqual(
            schema, arrow_table._schema.to_string(show_schema_metadata=False)
        )

    def test_timezone_support(self):
        from dateutil.zoneinfo import get_zonefile_instance
        from datetime import datetime
        from dateutil import tz
        import pandas as pd
        import pytz

        arrow_backend, node_backend = testing_utility._generate_backends()

        zonenames = list(get_zonefile_instance().zones)  # 595 elements
        pytz_tz = sorted(pytz.all_timezones)  # 594 elements

        datetime_tz_series = pd.Series(
            [datetime.now(tz=tz.gettz(timezone)) for timezone in zonenames]
        )

        df = datetime_tz_series.to_frame()

        arrow_table = arrow_backend.write_table(df)
        schema = (
            "<Row Key>: string\n"
            "0: extension<knime.logical_type<LogicalTypeExtensionType>>"
        )
        print(arrow_table._schema.to_string(show_schema_metadata=False))
        self.assertEqual(
            schema, arrow_table._schema.to_string(show_schema_metadata=False)
        )

        tz_timestamp_series = pd.Series(
            [pd.Timestamp(1513393355, unit="s", tz=timezone) for timezone in pytz_tz],
        )
        df = tz_timestamp_series.to_frame()
        empty_series = pd.Series([pd.NA] * len(df.columns), index=df.columns)
        df = df.append(empty_series, ignore_index=True)
        arrow_table = arrow_backend.write_table(df)
        schema = (
            "<Row Key>: string\n"
            "0: extension<knime.logical_type<LogicalTypeExtensionType>>"
        )
        self.assertEqual(
            schema, arrow_table._schema.to_string(show_schema_metadata=False)
        )

    def test_fslocation(self):
        arrow_backend, node_backend = testing_utility._generate_backends()

        df = testing_utility._generate_test_data_frame(
            "dict_enc_fs_location.zip", columns=["fslocation"]
        )
        string = ""
        for i in range(10):
            string += f"""{{'fs_category': 'LOCAL', 'fs_specifier': None, 'path': '/home/jonasklotz/Documents/test/{i}.txt'}}\n"""
        self.assertEqual(
            str(df.to_string(header=False, index=False, index_names=False)) + "\n",
            string,
        )
