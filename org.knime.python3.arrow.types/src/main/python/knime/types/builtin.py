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
import datetime as dt
import warnings

from dateutil import tz

import knime.api.schema as ks
import knime.api.types as kt
import knime._arrow._pandas as kap

_start_of_epoch = dt.datetime(1970, 1, 1)
_microsecond_delta = dt.timedelta(microseconds=1)
_second_delta = dt.timedelta(seconds=1)


def _before_or_after(value):
    if value < 0:
        return "before"
    else:
        return "after"


def _utc_conversion_warning(tzname):
    import logging
    import warnings

    logging.captureWarnings(True)
    warnings.warn(
        f"The timezone {tzname} is not supported in KNIME, it is converted to a UTC Timezone. "
        f"No time is lost, but spatial information could be missing. "
        f"E.G. British Summertime is converted to UTC+1. The information of the "
        f"daylight saving time is not preserved. ",
        stacklevel=5,
    )
    logging.captureWarnings(False)


class ZonedDateTimeValueFactory2(
    kt.PythonValueFactory
):  # The 2 is used to match the name on Java side
    def __init__(self):
        kt.PythonValueFactory.__init__(self, dt.datetime)
        self._java_timezones = None
        self._local_dt_factory = LocalDateTimeValueFactory()

    def decode(self, storage):
        if storage is None:
            return None
        zone_offset = storage["2"]
        zone_id = storage["3"]
        time_zone = tz.tzoffset(zone_id, zone_offset)
        local_datetime = self._local_dt_factory.decode(storage)
        return local_datetime.replace(tzinfo=time_zone)

    def encode(self, value):
        if value is None:
            return None
        tz_info = value.tzinfo
        if tz_info is None:  # if we do not have a timezone object in a tz column
            raise TypeError(
                f"When trying to convert the element '{value}' to a ZonedDateTime no timezone was detected. "
                f"Maybe it is a LocalTimeZone? If you're using pandas, please assign a type to the Pandas "
                f"series using knime.schema.logical(correct_dtype).to_pandas()"
            )
        # get time zone name for tzfile object, pytz object or tzinfo object
        tz_name = self.extract_tz_name(tz_info, value)
        if not self._java_timezones:
            self.get_java_timezones()

        # Java does not support the timezone eg BST. We convert it to UTC
        if (
            self._java_timezones
            and tz_name not in self._java_timezones
            or tz_name is None
        ):
            return self.convert_tz_to_utc(tz_info, tz_name, value)

        local_dt_dict = self._local_dt_factory.encode(value)
        tz_offset_seconds = self.get_offset_seconds(tz_info, value)
        local_dt_dict["2"] = tz_offset_seconds
        local_dt_dict["3"] = tz_name
        return local_dt_dict

    def extract_tz_name(self, tz_info, value):
        if isinstance(tz_info, tz.tzfile):  # this is if pandas converts to an object
            tz_name = "/".join(
                tz_info._filename.split("/")[-2:]
            )  # extract the actual timezone name from tz file
            tz_name = tz_name.replace(" ", "_")  # java cannot handle spaces
        # in case it's a pytz object, it has a zone attribute which is recognized in java
        # pytz is used by pandas
        elif hasattr(tz_info, "zone"):
            tz_name = tz_info.zone
        else:
            tz_name = tz_info.tzname(value)
        return tz_name

    def get_offset_seconds(self, tz_info, value):
        tz_offset = tz_info.utcoffset(value)
        # python handles overflow by setting day to -1 , which is not represented in the seconds
        if tz_offset.days < 0:
            tz_offset_seconds = tz_offset.seconds - (24 * 60 * 60)  # subtract one day
        else:
            tz_offset_seconds = tz_offset.seconds
        return tz_offset_seconds

    def convert_tz_to_utc(self, tz_info, tz_name, value):
        """
        If we do not have a Java representation for the timezone, it is converted to UTC. Thereby possibly losing
        spatial or daylight saving time information.
        """
        _utc_conversion_warning(tz_name)
        utc_value = value.astimezone(tz.UTC)
        local_dt_dict = self._local_dt_factory.encode(utc_value)
        tz_offset = tz_info.utcoffset(value)
        offset_string = self.parse_utc_offset(tz_offset)

        local_dt_dict[
            "2"
        ] = 0  # we handle the complete offset with the offset (ZoneID) String
        local_dt_dict["3"] = offset_string
        return local_dt_dict

    def parse_utc_offset(self, tz_offset):
        """
        :param tz_offset: offset to be parsed as string
        :return: offset string of an UTC offset in the form of +07:30 or -07:30
        """
        tz_offset_hours = int(tz_offset.seconds / 60 / 60)  # convert to hours
        tz_offset_mins = int((tz_offset.seconds / 60) % 60)
        # python handles overflow by setting day to -1 , which this is not represented in the hours
        if tz_offset.days < 0:
            tz_offset_hours -= 24  # subtract one day, java supports negative values
            if (
                tz_offset_mins > 0
            ):  # if we have minutes in a negative timezone eg: "-07:30" we have to add one more hour
                tz_offset_hours += 1
        offset_string = str(abs(tz_offset_hours))
        if tz_offset_mins > 0:  # add minutes for half timezones
            offset_string += ":" + str(tz_offset_mins)
        if abs(tz_offset_hours) < 10:  # we do need a trailing zero
            offset_string = "0" + offset_string

        sign = "-" if tz_offset_hours < 0 else "+"
        offset_string = sign + offset_string
        return offset_string

    def get_java_timezones(self):
        from knime._arrow._backend import gateway

        try:
            self._java_timezones = (
                gateway().jvm.org.knime.python3.PythonEntryPointUtils.getSupportedTimeZones()
            )
        except RuntimeError:
            warnings.warn(
                "Could not load Java Timezones. This can happen in UNIT Tests"
            )

    def can_convert(self, value):
        if isinstance(value, dt.datetime):
            return value.tzinfo is not None
        else:
            return False


class LocalDateTimeValueFactory(kt.PythonValueFactory):
    def __init__(self) -> None:
        kt.PythonValueFactory.__init__(self, dt.datetime)

    def decode(self, storage):
        if storage is None:
            return None
        try:
            if storage["0"] is None:
                return None
            day_of_epoch = storage["0"]
            nano_of_day = storage["1"]
            micro_of_day = nano_of_day // 1000  # here we lose precision
            return _start_of_epoch + dt.timedelta(
                days=day_of_epoch, microseconds=micro_of_day
            )

        except OverflowError as e:

            if e.args[0] == "date value out of range":
                raise OverflowError(
                    f"Cannot represent the date {day_of_epoch} days and {micro_of_day} ms {_before_or_after(day_of_epoch)}"
                    f" {_start_of_epoch} in Pandas, the data range only allows dates from {dt.date.min}"
                    f" to {dt.date.max}"
                ) from None
            else:
                raise OverflowError(
                    f"Cannot represent the value {day_of_epoch} or {micro_of_day} as date as it too large,"
                    f"the data range only allows dates from {dt.date.min} to {dt.date.max}"
                ) from None

    def encode(self, datetime):
        if datetime is None:
            return None
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
        if storage is None:
            return None
        try:
            seconds = storage["0"]
            nanos = storage["1"]
            return dt.timedelta(seconds=seconds, microseconds=nanos // 1000)
        except OverflowError as e:
            if e.args[0] == "date value out of range":
                raise OverflowError(
                    f"Cannot represent {seconds} and {nanos} {_before_or_after(seconds)} the date {_start_of_epoch}"
                    f" in Pandas, the data range only allows dates from {dt.date.min} to {dt.date.max}"
                ) from None
            else:
                raise OverflowError(
                    f"Cannot represent the value {seconds} or {nanos} as date as it is too large, "
                    f"the data range only allows dates from {dt.date.min} to {dt.date.max}"
                ) from None

    def encode(self, value):
        if value is None:
            return None
        seconds = value // _second_delta
        nanos = value.microseconds * 1000
        return {"0": seconds, "1": nanos}

    def can_convert(self, value):
        return isinstance(value, dt.timedelta)


class LocalDateValueFactory(kt.PythonValueFactory):
    def __init__(self) -> None:
        kt.PythonValueFactory.__init__(self, dt.date)

    def decode(self, day_of_epoch):
        try:
            if day_of_epoch is None:
                return None

            return _start_of_epoch.date() + dt.timedelta(days=day_of_epoch)
        except OverflowError as e:
            if e.args[0] == "date value out of range":
                raise OverflowError(
                    f"Cannot represent the  Date value of {day_of_epoch} days {_before_or_after(day_of_epoch)}"
                    f" {_start_of_epoch.date()} in Pandas, "
                    f"the data range only allows dates from {dt.date.min} to {dt.date.max}"
                ) from None
            else:
                raise OverflowError(
                    f"Cannot represent the value {day_of_epoch} as date as it is too large, "
                    f"the data range only allows dates from {dt.date.min} to {dt.date.max}"
                ) from None

    def encode(self, date):
        if date is None:
            return None
        return (date - _start_of_epoch.date()).days


class LocalTimeValueFactory(kt.PythonValueFactory):
    def __init__(self) -> None:
        kt.PythonValueFactory.__init__(self, dt.time)

    def decode(self, nano_of_day):
        if nano_of_day is None:
            return None
        try:
            micro_of_day = nano_of_day // 1000  # here we lose precision
            local_dt = dt.datetime.min + dt.timedelta(microseconds=micro_of_day)
            return local_dt.time()

        except OverflowError as e:
            if e.args[0] == "date value out of range":
                raise OverflowError(
                    f"Cannot represent the  Date value of {micro_of_day} microseconds after {dt.datetime.min} in Pandas,"
                    f"the data range only allows dates from {dt.date.min} to {dt.date.max}"
                ) from None
            else:
                raise OverflowError(
                    f"Cannot represent the value {micro_of_day} microseconds after {dt.datetime.min} as date"
                    f" as it is too large the data range only allows dates from"
                    f" {dt.date.min} to {dt.date.max}"
                ) from None

    def encode(self, time):
        if time is None:
            return None
        if hasattr(time, "tzinfo") and time.tzinfo is not None:
            warnings.warn(
                f"KNIME does not support time objects with timezones. Therefore the timezone information "
                f"'{time.tzinfo}' is lost. Please consider using a datetime object with a timezone."
            )
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


class FSLocationValue:
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

    def __repr__(self):
        return str(self.to_dict())


class FSLocationValueFactory(kt.PythonValueFactory):
    def __init__(self):
        kt.PythonValueFactory.__init__(self, FSLocationValue)

    def decode(self, storage):
        if storage is None:
            return None
        # TODO we could change the keys of storage to integers (or use a list) which would be more compliant with
        #  the behavior in java
        return FSLocationValue(storage["0"], storage["1"], storage["2"])

    def encode(self, value):
        if value is None:
            return None
        return {"0": value.fs_category, "1": value.fs_specifier, "2": value.path}


class BooleanSetValue:
    def __init__(self, has_true, has_false, has_missing):
        self.has_true = has_true
        self.has_false = has_false
        self.has_missing = has_missing

    def to_dict(self):
        return {
            "has_true": self.has_true,
            "has_false": self.has_false,
            "has_missing": self.has_missing,
        }


class BooleanSetValueFactory(kt.PythonValueFactory):
    def __init__(self):
        kt.PythonValueFactory.__init__(self, BooleanSetValue)

    def decode(self, storage):
        if storage is None:
            return None
        return BooleanSetValue(storage["0"], storage["1"], storage["2"])

    def encode(self, value):
        if value is None:
            return None
        return {"0": value.has_true, "1": value.has_false, "2": value.has_missing}


class DenseBitVectorValue(str):
    """
    Represents a DenseBitVectorValue from KNIME as bitstring in Python
    """

    def to_bytes(self):
        length = len(self)
        length_bytes = length.to_bytes(length=8, byteorder="little")
        return length_bytes + int(self, 2).to_bytes(length=length, byteorder="little")

    @classmethod
    def from_bytes(cls, data):
        length = int.from_bytes(data[:8], byteorder="little")
        binary_strings = [format(b, "08b") for b in reversed(data[8:])]
        return cls("".join(binary_strings)[-length:])


class DenseBitVectorValueFactory(kt.PythonValueFactory):
    def __init__(self):
        kt.PythonValueFactory.__init__(self, DenseBitVectorValue)

    def decode(self, storage):
        if storage is None:
            return None
        return DenseBitVectorValue.from_bytes(storage)

    def encode(self, value):
        if value is None:
            return None
        return value.to_bytes()


class DenseByteVectorValue(bytes):
    pass


class DenseByteVectorValueFactory(kt.PythonValueFactory):
    def __init__(self):
        kt.PythonValueFactory.__init__(self, DenseByteVectorValue)

    def decode(self, storage):
        if storage is None:
            return None
        return DenseByteVectorValue(storage)

    def encode(self, value):
        if value is None:
            return None
        return value


def _knime_value_factory(name):
    return '{"value_factory_class":"' + name + '"}'


class FromDTPandasColumnConverter(kt.FromPandasColumnConverter):
    def can_convert(self, dtype) -> bool:
        return False

    def convert_column(
        self, data_frame: "pandas.dataframe", column_name: str
    ) -> "pandas.Series":
        import datetime

        column = data_frame[column_name]
        local_dt = True
        for elem in column:  # we iterate to see if we have a local or zoned dt
            if elem.tzinfo is not None:
                local_dt = False
                break
        if local_dt:
            dtype = kap._create_local_dt_type()
            return column.astype(dtype)
        # we have a zoned datetime and can parse as usual
        dtype = ks.logical(datetime.datetime).to_pandas()
        return column.astype(dtype)
