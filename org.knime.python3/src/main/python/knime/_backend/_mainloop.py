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
@author Adrian Nembach, KNIME GmbH, Konstanz, Germany
@author Carsten Haubold, KNIME GmbH, Konstanz, Germany
@author David Arnold, KNIME GmbH, Berlin, Germany
"""

from queue import Full, Queue
from threading import Event, Lock
from typing import Callable, List, Optional


class MainLoop:
    def __init__(self):
        super().__init__()
        self._main_loop_queue: Queue[Optional[_ExecuteTask]] = Queue()
        self._main_loop_lock = Lock()
        self._main_loop_stopped = False

    def enter(self) -> None:
        """
        Enter the main loop and process tasks from the queue.

        This method runs in an infinite loop, continuously retrieving tasks from the queue and executing them until a None task is encountered.

        """
        queue = self._main_loop_queue
        while True:
            task = queue.get()

            if task is not None:
                task.run()
                queue.task_done()
            else:
                # Poison pill, exit loop.
                queue.task_done()
                return

    def exit(self) -> None:
        """
        Exit the main loop and stop further execution.

        This method is used to gracefully exit the main loop and stop any further execution. It empties the queue and puts a `None` value to the queue to signal the completion of all tasks. If the queue is full, it will continue to try until it can successfully put the `None` value into the queue.

        """
        with self._main_loop_lock:
            self._main_loop_stopped = True
            queue = self._main_loop_queue
            # Aggressively try to clear queue and insert poison pill.
            while True:
                with queue.all_tasks_done:
                    queue.queue.clear()
                    queue.unfinished_tasks = 0
                try:
                    queue.put_nowait(None)  # Poison pill
                except Full:
                    continue
                else:
                    break

    def execute(self, func, *args, **kwargs):
        """
        Run the given function on the main thread, wait for its results and return those.
        """
        with self._main_loop_lock:
            if self._main_loop_stopped:
                raise RuntimeError(
                    "Cannot schedule executions on the main thread after the main loop stopped."
                )
            execute_task = _ExecuteTask(func, *args, **kwargs)
            self._main_loop_queue.put(execute_task)
            return execute_task.result()


class _ExecuteTask:
    """
    Executes the given function with provided arguments and provides access
    to the results.
    """

    def __init__(self, execute_func: Callable[[str], List[str]], *args, **kwargs):
        self._execute_func = execute_func
        self._args = args
        self._kwargs = kwargs
        self._execute_finished = Event()
        self._result = None
        self._exception = None

    def run(self):
        """
        Run the function and handle any exceptions.

        This method executes the stored function with the provided arguments and keyword arguments. If an exception occurs during execution, it is stored in the `_exception` attribute. The `_result` attribute is updated with the result of the function execution, or `None` if an exception occurs.

        Upon completion, the `_execute_finished` event is set.

        """
        try:
            self._result = self._execute_func(*self._args, **self._kwargs)
        except Exception as ex:
            self._exception = ex
        finally:
            self._execute_finished.set()

    def result(self) -> List[str]:
        """
        Get the result of a function execution.

        This method waits for the execution to finish and returns the result.
        If the execution raised an exception, it re-raises the exception.

        Returns
        -------
        List[str]
            The result of the function execution.

        Raises
        ------
        AnyException
            If the execution of the function raised an exception.
        """
        self._execute_finished.wait()
        if self._exception is not None:
            try:
                raise self._exception
            finally:
                # Copied from concurrent.futures._base.Future:
                # Break a reference cycle with the exception in self._exception
                self = None
        return self._result
