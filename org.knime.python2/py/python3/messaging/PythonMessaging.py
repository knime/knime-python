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
@author Christian Dietz, KNIME GmbH, Konstanz, Germany
"""

import threading

from PythonUtils import invoke_safely
from debug_util import debug_msg
from messaging.MessageReceiver import MessageReceiver
from messaging.MessageSender import MessageSender
from messaging.PythonMessagingBase import PythonMessagingBase
from python3.messaging.MessageDistributorLoop import MessageDistributorLoop
from python3.messaging.MessageReceiverLoop import MessageReceiverLoop
from python3.messaging.MessageSenderLoop import MessageSenderLoop


class PythonMessaging(PythonMessagingBase):
    """
    The Python 3 messaging system.
    """

    _SEND_QUEUE_LENGTH = 10

    _RECEIVE_QUEUE_LENGTH = 10

    _TASK_RECEIVE_QUEUE_LENGTH = 10

    def __init__(self, connection, monitor):
        super(PythonMessaging, self).__init__()
        self._monitor = monitor
        self._is_running_lock = threading.Lock()
        self._message_id_lock = threading.Lock()

        self._send_loop = MessageSenderLoop(MessageSender(connection),
                                            monitor.create_message_queue(PythonMessaging._SEND_QUEUE_LENGTH), monitor)

        self._receive_queue = monitor.create_message_queue(PythonMessaging._RECEIVE_QUEUE_LENGTH)
        self._receive_loop = MessageReceiverLoop(MessageReceiver(connection), self._receive_queue, monitor)

        self._distribute_loop = MessageDistributorLoop(self._receive_loop, self._distributor,
                                                       monitor)

    @property
    def is_running(self):
        with self._is_running_lock:
            return super(PythonMessaging, self).is_running

    def create_receive_queue(self):
        return self._monitor.create_message_queue(PythonMessaging._TASK_RECEIVE_QUEUE_LENGTH)

    def create_next_message_id(self):
        with self._message_id_lock:
            return super(PythonMessaging, self).create_next_message_id()

    def register_message_handler(self, message_category, handler):
        return self._distribute_loop.register_message_handler(message_category, handler)

    def unregister_message_handler(self, message_category):
        return self._distribute_loop.unregister_message_handler(message_category)

    def start(self):
        with self._is_running_lock:
            super(PythonMessaging, self).start()

    def send(self, message):
        self._send_loop.send(message)

    def handle(self, message):
        if self._distribute_loop.can_handle(message.category):
            self._receive_queue.put(message)
            return True
        else:
            return False

    def close(self):
        with self._is_running_lock:
            super(PythonMessaging, self).close()

    def _start(self):
        # Order is intended (and differs from Java since we are the server).
        self._send_loop.start()
        self._distribute_loop.start()
        self._receive_loop.start()

    def _close(self):
        # Order is intended (and differs from Java since we are the server).
        loops = [self._receive_loop, self._distribute_loop, self._send_loop]
        invoke_safely(lambda msg, _: debug_msg(msg, exc_info=True), lambda l: l.close(), loops)
