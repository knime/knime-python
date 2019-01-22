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
from concurrent.futures._base import Future
from concurrent.futures.thread import _WorkItem
from queue import Full


class _QueuedExecutor(object):
    """
    Executor that enqueues submitted tasks in a waiting queue that is processed by the loop in its start method.
    Mimics a part of the interface of Python 3 futures.ThreadPoolExecutor.
    """

    def __init__(self, queue, monitor):
        """
        :param queue: Expected to derive from (or behave like) queue.Queue.
        :param monitor: An instance of python3._ExecutionMonitor.
        """
        self._monitor = monitor
        self._queue = queue
        self._shutdown = False
        self._shutdown_lock = threading.Lock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def start(self):
        """
        Starts the executor's processing loop and blocks until the executor is shut down or an exception occurs.
        The execution of submitted tasks is carried out in the calling thread.
        """
        while True:
            self._monitor.check_exception()
            work_item = self._queue.get(block=True)
            if work_item is not None:
                work_item.run()
                self._queue.task_done()
                del work_item
                continue
            else:
                # Poison pill. Shut down.
                self._queue.task_done()
                return

    def submit(self, fn, *args, **kwargs):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after shutdown')
            future = Future()
            work_item = _WorkItem(future, self._monitored_fn, (fn,) + args, kwargs)
            # Blocks until item is put in queue.
            self._queue.put(work_item)
            return future

    def _monitored_fn(self, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except BaseException as ex:
            self._monitor.report_exception(ex)
            raise

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
            # Poison pill.
            if wait:
                self._queue.put(None)
            else:
                queue = self._queue
                while True:
                    with queue.all_tasks_done:
                        queue.queue.clear()
                        queue.unfinished_tasks = 0
                    try:
                        queue.put_nowait(None)
                    except Full:
                        continue
                    else:
                        break
        if wait:
            self._queue.join()
