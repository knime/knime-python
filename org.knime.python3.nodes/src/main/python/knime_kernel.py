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
"""

import os
import pickle
import py4j.clientserver
import sys
import warnings
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from py4j.java_collections import JavaArray, ListConverter
from py4j.java_gateway import JavaClass
from queue import Full, Queue
from threading import Event, Lock, get_ident
from typing import Any, Callable, Dict, List, Optional, TextIO

import knime_arrow_table as kat
import knime_gateway as kg

import debugpy

#debugpy.listen(5678)
#debugpy.wait_for_client()

#class MainLoopStation -> org.knime.python3 -> knime_main_loop
# TODO update knime_kernel.py
lock = Lock()

class PythonKernel(kg.EntryPoint):
    def __init__(self):
        super().__init__()
        self._workspace: Dict[str, Any] = {}  # TODO: should we make this thread safe?
        self._main_loop_queue: Queue[Optional[_ExecuteTask]] = Queue()
        self._main_loop_lock = lock
        self._main_loop_stopped = False
        self._external_custom_path_initialized = False
        self._working_dir_initialized = False
        self._java_callback = None

    def initializeJavaCallback(self, java_callback: JavaClass) -> None:
        if self._java_callback is not None:
            raise RuntimeError(
                "Java callback has already been initialized. Calling this method again is an implementation error."
            )
        self._java_callback = java_callback

    @property
    def java_callback(self):
        #Provides access to functionality on the Java side. Used by e.g. knime_jupyter to resolve KNIME URLs.
        #:return: The callback on the Java side of Java type
        #org.knime.python3.scripting.Python3KernelBackendProxy.Callback.
        return self._java_callback

    def enter_main_loop(self) -> None:
        queue = self._main_loop_queue
        while True:
            task = queue.get()
            #debugpy.breakpoint()

            if task is not None:
                task.run()
                queue.task_done()
            else:
                # Poison pill, exit loop.
                queue.task_done()
                return

    def exit_main_loop(self) -> None:
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


    def executeOnMainThread(self, source_code: str, check_outputs: bool) -> List[str]:
        with self._main_loop_lock:
            if self._main_loop_stopped:
                raise RuntimeError(
                    "Cannot schedule executions on the main thread after the main loop stopped."
                )
            execute_task = _ExecuteTask(self.execute, source_code, check_outputs)
            self._main_loop_queue.put(execute_task)
            return execute_task.result()

class _ExecuteTask:
    """
    Executes
    """
    def __init__(self, execute_func: Callable[[str], List[str]], *args):
        self._execute_func = execute_func
        self._args = args
        self._execute_finished = Event()
        self._result = None
        self._exception = None

    def run(self):
        try:
            self._result = self._execute_func(*self._args)
        except Exception as ex:
            self._exception = ex
        finally:
            self._execute_finished.set()

    def result(self) -> List[str]:
        self._execute_finished.wait()
        if self._exception is not None:
            try:
                raise self._exception
            finally:
                # Copied from concurrent.futures._base.Future:
                # Break a reference cycle with the exception in self._exception
                self = None
        return self._result
