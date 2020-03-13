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
import sys

from debug_util import debug_msg
from messaging.Message import Message


class MessageReceiver(object):
    def __init__(self, connection):
        self._connection = connection

    def receive(self):
        header_size = self._read_size()
        payload_size = self._read_size()
        header = self._read_data(header_size).decode('utf-8')
        if payload_size > 0:
            payload = self._read_data(payload_size)
        else:
            payload = None
        message = Message.create(header, payload)
        debug_msg("Python - Received message: " + str(message))
        return message

    # reads 4 bytes from the input stream and interprets them as size
    def _read_size(self):
        data = bytearray()
        while len(data) < 4:
            data.extend(self._recv(4 - len(data)))
        return struct.unpack('>L', data)[0]

    # reads the next data from the input stream
    def _read_data(self, size=None):
        if size is None:
            size = self._read_size()
        data = bytearray()
        while len(data) < size:
            data.extend(self._recv(size - len(data)))
        return data

    def _recv(self, bufsize):
        data = self._connection.recv(bufsize)
        if len(data) < 1:
            raise RuntimeError("Connection closed unexpectedly.")
        return data
