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

import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from threading import RLock

from DBUtil import DBUtil
from PythonKernelBase import PythonKernelBase
from messaging.RequestHandlers import _builtin_request_handlers
from python3._ExecutionMonitor import _ExecutionMonitor
from python3._QueuedExecutor import _QueuedExecutor
from python3.messaging.PythonMessaging import PythonMessaging


class PythonKernel(PythonKernelBase):
    """
    The Python 3 kernel.
    """

    def __init__(self):
        try:
            super(PythonKernel, self).__init__()
            self._monitor = _ExecutionMonitor()
            self._execution_queue = self._monitor.create_message_queue(0)
            self._is_running_or_closed_lock = RLock()
            # SQL connections need to be closed by the same thread that opened them, which is the execute thread.
            self._execute_thread_cleanup_object_names = set()
        except BaseException as ex:
            self.close()
            raise

    def add_cleanup_object_name(self, variable_name):
        if isinstance(self.get_variable(variable_name), DBUtil):
            self._execute_thread_cleanup_object_names.add(variable_name)
        super(PythonKernel, self).add_cleanup_object_name(variable_name)

    def start(self):
        with self._is_running_or_closed_lock:
            super(PythonKernel, self).start()
        # Call to start is blocking (main execution loop).
        self.execute_thread_executor.start()
        self._monitor.wait_for_close()

    def close(self):
        # Note: This method may not be called from self._execute_thread_executor because some cleanup operations need
        # to be forwarded to that executor which would cause a deadlock.
        with self._is_running_or_closed_lock:
            super(PythonKernel, self).close()
            # Hold lock. Make all that closing atomic.
            self._monitor.report_close()

    def _setup_builtin_request_handlers(self):
        super(PythonKernel, self)._setup_builtin_request_handlers()
        for k in ['putFlowVariables',
                  'getFlowVariables',
                  'putObject',
                  'getObject',
                  'putSql',
                  'getSql',
                  'getImage',
                  'listVariables',
                  'hasAutoComplete',
                  'autoComplete',
                  'execute']:
            self.unregister_task_handler(k)
            self.register_task_handler(k, _builtin_request_handlers[k], executor=self.execute_thread_executor)

    def _create_execute_thread_executor(self):
        return _QueuedExecutor(self._execution_queue, self._monitor)

    def _create_executor(self):
        number_threads = multiprocessing.cpu_count() * 2 - 1
        return ThreadPoolExecutor(number_threads)

    def _create_messaging(self, connection):
        return PythonMessaging(connection, self._monitor)

    def _cleanup_object(self, obj, obj_name):
        if obj_name in self._execute_thread_cleanup_object_names:
            self._execute_thread_executor.submit(obj._cleanup)
        else:
            super(PythonKernel, self)._cleanup_object(obj, obj_name)
