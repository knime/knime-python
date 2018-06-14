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

from Queue import Empty

from messaging.MessageReceiver import MessageReceiver
from messaging.MessageSender import MessageSender
from messaging.PythonMessagingBase import PythonMessagingBase


class PythonMessaging(PythonMessagingBase):
    """
    The Python 2 messaging system.
    """

    def __init__(self, connection):
        super(PythonMessaging, self).__init__()
        self._sender = MessageSender(connection)
        self._receiver = MessageReceiver(connection)

    def create_receive_queue(self):
        return PythonMessaging._MessageFetchingQueue(self._receiver)

    def send(self, message):
        self._sender.send(message)

    def handle(self, message):
        if self._distributor.can_handle(message.category):
            self._distributor.handle(message)
            return True
        else:
            return False

    def _start(self):
        self._loop()

    def _loop(self):
        while self._is_running:
            message = self._receiver.receive()
            self._distributor.handle(message)

    def _close(self):
        pass  # no-op

    class _MessageFetchingQueue(object):
        """
        Queue that mimics a part of the interface of Python 2 Queue.Queue (Python 3 queue.Queue) without all the mutex
        overhead.
        Allows to fetch new messages via a message receiver if it is empty.
        """

        def __init__(self, message_receiver):
            self._queue = []
            self._message_receiver = message_receiver

        def empty(self):
            return not self._queue

        def full(self):
            return False

        def put(self, item, block=True, timeout=None):
            self._queue.append(item)

        def put_nowait(self, item):
            return self.put(item, block=False)

        def get(self, block=True, timeout=None):
            if self.empty():
                if not block:
                    raise Empty()
                else:
                    self.put(self._message_receiver.receive())
            return self._queue.pop(0)

        def get_nowait(self):
            return self.get(block=False)
