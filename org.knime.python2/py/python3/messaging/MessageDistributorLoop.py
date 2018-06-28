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
from python3.messaging.AbstractMessageLoop import AbstractMessageLoop


class MessageDistributorLoop(AbstractMessageLoop):
    def __init__(self, receiver, distributor, monitor):
        super(MessageDistributorLoop, self).__init__(monitor)
        self._receiver = receiver
        self._distributor = distributor
        self._message_handlers_lock = threading.RLock()
        self._message_handlers_closed = False

    def register_message_handler(self, message_category, handler):
        with self._message_handlers_lock:
            exception = None
            registered = False
            try:
                registered = self._distributor.register_message_handler(message_category, handler)
            except BaseException as ex:
                exception = ex
            if self._message_handlers_closed:
                self._close_message_handlers([handler])
            if exception is not None:
                raise exception
            else:
                return registered

    def unregister_message_handler(self, message_category):
        with self._message_handlers_lock:
            return self._distributor.unregister_message_handler(message_category)

    def can_handle(self, message_category):
        with self._message_handlers_lock:
            return self._distributor.can_handle(message_category)

    def _loop(self):
        while self.is_running:
            message = None
            try:
                message = self._receiver.receive()
                self._distributor.handle(message)
            except Exception as ex:
                debug_msg("Failed to distribute message " + (
                    "'" + str(message) + "' " if message is not None else "") + "from Java. Cause: " + str(ex))
                raise ex

    def _close(self):
        with self._message_handlers_lock:
            self._message_handlers_closed = True
            # Handlers may want to unregister upon closing. Copy to avoid concurrent modification.
            handlers = list(self._distributor._message_handlers.values())
            self._close_message_handlers(handlers)

    def _close_message_handlers(self, handlers):
        invoke_safely(None, lambda h: h.handle(self._monitor.poison_pill), handlers)
