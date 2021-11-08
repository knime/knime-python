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
PythonValueFactory implementations for types defined in KNIME.
@author Adrian Nembach, KNIME GmbH, Konstanz, Germany
"""
import knime_types as kt
import datetime as dt
from dateutil import tz


_start_of_epoch = dt.datetime(1970, 1, 1)
_microsecond_delta = dt.timedelta(microseconds=1)
_second_delta = dt.timedelta(seconds=1)


class ZonedDateTimeValueFactory2(
    kt.PythonValueFactory
):  # The 2 is used to match the name on Java side
    def __init__(self):
        kt.PythonValueFactory.__init__(self, dt.datetime)
        self._local_dt_factory = LocalDateTimeValueFactory()

    def decode(self, storage):
        zone_offset = storage["2"]
        zone_id = storage["3"]
        time_zone = tz.tzoffset(zone_id, zone_offset)
        local_datetime = self._local_dt_factory.decode(storage)
        return local_datetime.replace(tzinfo=time_zone)

    def encode(self, value):
        local_dt_dict = self._local_dt_factory.encode(value)
        tz_info = value.tzinfo
        tz_offset = tz_info.utcoffset(value).seconds
        tz_name = tz_info.tzname(value)
        local_dt_dict["2"] = tz_offset
        local_dt_dict["3"] = tz_name
        return local_dt_dict

    def can_convert(self, value):
        if isinstance(value, dt.datetime):
            return value.tzinfo is not None
        else:
            return False


class LocalDateTimeValueFactory(kt.PythonValueFactory):
    def __init__(self) -> None:
        kt.PythonValueFactory.__init__(self, dt.datetime)

    def decode(self, storage):
        day_of_epoch = storage["0"]
        nano_of_day = storage["1"]
        micro_of_day = nano_of_day // 1000  # here we lose precision
        return _start_of_epoch + dt.timedelta(
            days=day_of_epoch, microseconds=micro_of_day
        )

    def encode(self, datetime):
        delta = datetime.replace(tzinfo=None) - _start_of_epoch
        day_of_epoch = delta.days
        micro_of_day = (
            datetime - datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        ) // _microsecond_delta
        nano_of_day = micro_of_day * 1000
        return {"0": day_of_epoch, "1": nano_of_day}

    def can_convert(self, value):
        if isinstance(value, dt.datetime):
            return value.tzinfo is None
        else:
            return False


class DurationValueFactory(kt.PythonValueFactory):
    def __init__(self):
        kt.PythonValueFactory.__init__(self, dt.timedelta)

    def decode(self, storage):
        seconds = storage["0"]
        nanos = storage["1"]
        return dt.timedelta(seconds=seconds, microseconds=nanos // 1000)

    def encode(self, value):
        seconds = value // _second_delta
        nanos = value.microseconds * 1000
        return {"0": seconds, "1": nanos}

    def can_convert(self, value):
        return isinstance(value, dt.timedelta)


class LocalDateValueFactory(kt.PythonValueFactory):
    def __init__(self) -> None:
        kt.PythonValueFactory.__init__(self, dt.date)

    def decode(self, day_of_epoch):
        return _start_of_epoch.date() + dt.timedelta(days=day_of_epoch)

    def encode(self, date):
        return (date - _start_of_epoch.date()).days


class LocalTimeValueFactory(kt.PythonValueFactory):
    def __init__(self) -> None:
        kt.PythonValueFactory.__init__(self, dt.time)

    def decode(self, nano_of_day):
        micro_of_day = nano_of_day // 1000  # here we lose precision
        local_dt = dt.datetime.min + dt.timedelta(microseconds=micro_of_day)
        return local_dt.time()

    def encode(self, time):
        time_on_first_day = dt.datetime.min.replace(
            hour=time.hour,
            minute=time.minute,
            second=time.second,
            microsecond=time.microsecond,
        )
        delta = (time_on_first_day - dt.datetime.min) // _microsecond_delta
        return delta * 1000

    def can_convert(self, value):
        return type(value) == dt.time


class FsLocationValue:
    def __init__(self, fs_category, fs_specifier, path):
        self.fs_category = fs_category
        self.fs_specifier = fs_specifier
        self.path = path

    def to_dict(self):
        return {
            "fs_category": self.fs_category,
            "fs_specifier": self.fs_specifier,
            "path": self.path,
        }


class FsLocationValueFactory(kt.PythonValueFactory):
    def __init__(self):
        kt.PythonValueFactory.__init__(self, FsLocationValue)

    def decode(self, storage):
        # TODO we could change the keys of storage to integers (or use a list) which would be more compliant with
        #  the behavior in java
        return FsLocationValue(storage["0"], storage["1"], storage["2"])

    def encode(self, value):
        return {"0": value.fs_category, "1": value.fs_specifier, "2": value.path}
