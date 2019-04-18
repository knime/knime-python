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
@author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
@author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
@author Christian Dietz, KNIME GmbH, Konstanz, Germany
"""

import struct


class Message(object):
    KEY_ID = "id"

    KEY_CATEGORY = "category"

    @staticmethod
    def create(header, payload):
        header_fields = dict()
        fields = header.split("@")
        for field in fields:
            if not field:
                continue
            index_of_delimiter = field.find("=")
            key = field[:index_of_delimiter]
            value = field[index_of_delimiter + 1:]
            header_fields[key] = value

        id = header_fields.pop(Message.KEY_ID, None)
        if id is None:
            raise AttributeError("No id in message header  '" + header + "'.")
        category = header_fields.pop(Message.KEY_CATEGORY, None)
        if category is None:
            raise AttributeError("No category in message header  '" + header + "'.")

        return Message(id, category, payload, header_fields)

    def __init__(self, id, category, payload=None, additional_options=None):
        self._id = id
        self._category = category
        self._payload = payload
        self._header_fields = {Message.KEY_ID: str(id), Message.KEY_CATEGORY: category}
        if additional_options is not None:
            self._header_fields.update(additional_options)

    def __repr__(self):
        return self.header

    @property
    def id(self):
        return self._id

    @property
    def category(self):
        return self._category

    @property
    def header(self):
        return ''.join(['@' + k + '=' + v for k, v in self._header_fields.items()])

    def get_header_field(self, field_key):
        return self._header_fields.get(field_key)  # returns None if no mapping present

    @property
    def payload(self):
        return self._payload  # Used for decoding the payload of a Message.


class PayloadDecoder(object):
    """
    Used for decoding the payload of a Message.
    Schema: variable size types: (length: int32)(object)
            fixed size types: (object)
    """

    def __init__(self, buffer):
        self._buffer = buffer
        self._pointer = 0

    def get_next_bytes(self):
        size = struct.unpack('>L', self._read_bytes(4))[0]
        return self._read_bytes(size)

    def get_next_int(self):
        return struct.unpack('>l', self._read_bytes(4))[0]

    def get_next_long(self):
        return struct.unpack('>q', self._read_bytes(8))[0]

    def get_next_string(self):
        size = struct.unpack('>l', self._read_bytes(4))[0]
        return self._read_bytes(size).decode('utf-8') if size != -1 else None

    def _read_bytes(self, size):
        start_pointer = self._pointer
        self._pointer += size
        return self._buffer[start_pointer:self._pointer]


class PayloadEncoder(object):
    """
    Used for encoding the payload of a Message.
    Schema: variable size types: (length: int32)(object)
            fixed size types: (object)
    """

    def __init__(self):
        self._buffer = bytes()

    @property
    def payload(self):
        return self._buffer

    def put_bytes(self, data_bytes):
        data_bytes_size = struct.pack('>L', len(data_bytes))
        self._buffer += data_bytes_size + data_bytes
        return self

    def put_int(self, integer):
        self._buffer += struct.pack('>l', integer)
        return self

    def put_long(self, long):
        self._buffer += struct.pack('>q', long)
        return self

    def put_string(self, string):
        if string is not None:
            string_bytes = string.encode('utf-8')
            string_bytes_size = struct.pack('>l', len(string_bytes))
            self._buffer += string_bytes_size + string_bytes
        else:
            string_bytes_size = struct.pack('>l', -1)
            self._buffer += string_bytes_size
        return self
