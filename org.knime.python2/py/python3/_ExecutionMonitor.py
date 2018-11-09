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

from threading import Event

from debug_util import debug_msg
from messaging.Message import Message
from python3._MonitoredMessageQueue import _MonitoredMessageQueue


class _ExecutionMonitor(object):
    # TODO: We should encapsulate this poison pill in a special "killable" queue implementation.
    _POISON_PILL = Message(-1, "", None, None)

    def __init__(self):
        self._close = Event()
        self._exception = None

    @property
    def poison_pill(self):
        return _ExecutionMonitor._POISON_PILL

    # TODO: This should be moved out of here.
    def create_message_queue(self, length):
        return _MonitoredMessageQueue(length, self)

    def report_close(self):
        self._close.set()

    def report_exception(self, exception, message=None):
        if self._exception is None and exception is not None:
            self._exception = exception
            try:
                if message is not None:
                    error_message = message + " Cause: " + str(exception)
                else:
                    error_message = "An exception occurred: " + str(exception)
                debug_msg(error_message, exc_info=True)
            except BaseException:
                pass
            self.report_close()

    def check_exception(self):
        if self._exception is not None:
            raise self._exception

    def wait_for_close(self):
        self._close.wait()
        self.check_exception()
