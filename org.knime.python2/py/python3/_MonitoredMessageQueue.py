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

from queue import Empty
from queue import Full
from queue import Queue

from time import monotonic as time


class _MonitoredMessageQueue(Queue):
    _TIMEOUT_STEP_IN_SEC = 0.1

    def __init__(self, length, monitor):
        """
        :param length: The maximal length of the queue.
                       If length is less than or equal to zero, then the queue may be infinitely long.
        :param monitor: An instance of python3._ExecutionMonitor.
        """
        super(_MonitoredMessageQueue, self).__init__(length)
        self._monitor = monitor

    def put(self, item, block=True, timeout=None):
        if not block:
            return super(_MonitoredMessageQueue, self).put(item, block, timeout)
        else:
            timeout_step = _MonitoredMessageQueue._TIMEOUT_STEP_IN_SEC
            endtime = (time() + timeout) if timeout is not None else None
            while True:
                self._monitor.check_exception()
                if timeout is None:
                    remaining = timeout_step
                else:
                    remaining = endtime - time()
                try:
                    return super(_MonitoredMessageQueue, self).put(item, block,
                                                                   timeout_step
                                                                   if remaining > timeout_step
                                                                   else remaining)
                except Full:
                    continue

    def get(self, block=True, timeout=None):
        if not block:
            return super(_MonitoredMessageQueue, self).get(block, timeout)
        else:
            timeout_step = _MonitoredMessageQueue._TIMEOUT_STEP_IN_SEC
            endtime = (time() + timeout) if timeout is not None else None
            while True:
                self._monitor.check_exception()
                if timeout is None:
                    remaining = timeout_step
                else:
                    remaining = endtime - time()
                try:
                    return super(_MonitoredMessageQueue, self).get(block,
                                                                   timeout_step
                                                                   if remaining > timeout_step
                                                                   else remaining)
                except Empty:
                    continue
