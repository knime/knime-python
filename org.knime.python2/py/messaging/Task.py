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

import sys
from threading import Condition
from threading import Lock

from debug_util import debug_msg


class TaskFactory(object):
    def __init__(self, task_handler, message_sender, message_handlers, receive_queue_factory, message_id_supplier,
                 workspace, executor):
        self._delegate_task_handler = task_handler
        self._message_sender = message_sender
        self._message_handlers = message_handlers
        self._message_id_supplier = message_id_supplier
        self._receive_queue_factory = receive_queue_factory
        self._workspace = workspace
        self._executor = executor

    def create_task(self):
        return Task(None, self._delegate_task_handler, self._message_sender, self._message_handlers,
                    self._receive_queue_factory(),
                    self._message_id_supplier, self._workspace, self._executor)

    def handle(self, message):
        return self.create_task().handle(message)


class Task(object):
    def __init__(self, message, task_handler, message_sender, message_handlers, receive_queue, message_id_supplier,
                 workspace, executor):
        """
        @param receive_queue  this differs from Java.
        """
        self._initiating_message = message
        self._delegate_task = Task.FutureTask(self._run_internal)
        self._delegate_task_handler = task_handler
        self._message_sender = message_sender
        self._message_handlers = message_handlers
        self._message_id_supplier = message_id_supplier
        self._workspace = workspace
        self._executor = executor
        self._received_messages = receive_queue
        self._registered_message_categories = []
        self._is_running_or_done = False
        self._is_running_or_done_lock = Lock()
        self._task_category = None

    def run(self):
        debug_msg("Python - Now inside 'run'.")
        with self._is_running_or_done_lock:
            if self._is_running_or_done:
                debug_msg("Python - Already running. Return.")
                return
            else:
                self._is_running_or_done = True
        debug_msg("Python - Start running task, initiating message: " + str(self._initiating_message))
        self._executor.submit(self._delegate_task.run)

    def get(self):
        self.run()  # Start task if not already running.
        return self._delegate_task.get()

    def handle(self, message):
        debug_msg(
            "Python - Enqueue message for task, message: " + str(message) + ", initiating message: " + str(
                self._initiating_message))
        self._received_messages.put(message)
        debug_msg("Python - Now calling 'run'.")
        self.run()  # Start task if not already running.
        return True

    def _run_internal(self):
        debug_msg("Python - Run task, initiating message: " + str(self._initiating_message))
        to_send = self._initiating_message
        while not self._delegate_task.is_done:
            if to_send is not None:
                message_category = str(to_send.id)
                if self._task_category is None:
                    self._task_category = message_category
                # TODO: Only register for first sent message (i.e. task category) and replace subsequent
                # registrations by reply-to pattern.
                if not self._message_handlers.register_message_handler(message_category, self):
                    raise RuntimeError("Message handler for category '" + message_category + "' is already registered.")
                else:
                    self._registered_message_categories.append(message_category)
                self._message_sender.send(to_send)
            debug_msg(
                "Python - Wait for message in task, initiating message: " + str(self._initiating_message))
            received = self._received_messages.get()
            debug_msg(
                "Python - Received message in task, message: " + str(received) + ", initiating message: " + str(
                    self._initiating_message))
            to_send = self._delegate_task_handler.handle(
                received, self._message_handlers, self._message_id_supplier, self._set_result, self._workspace)
        # Send pending message if any.
        # This may happen if the act of responding to a message also marks (successful) termination of the task.
        if to_send is not None:
            self._message_sender.send(to_send)

        # Unregister message handlers to remove references to this task instance.
        for message_category in self._registered_message_categories:
            self._message_handlers.unregister_message_handler(message_category)
        del self._registered_message_categories[:]

        # Message may contain heavy payload. Dereference to obviate memory leak.
        self._initiating_message = None

    def _set_result(self, result):
        self._delegate_task.set(result)

    class FutureTask:
        """
        A class that works like an implementation of Java's RunnableFuture interface. "get" blocks the executing thread
        until an answer message is set using "set".
        """

        def __init__(self, runnable):
            self._runnable = runnable
            self._is_done = False
            self._result = None
            self._exc_info = None
            self._condition = Condition()

        @property
        def is_done(self):
            return self._is_done

        def run(self):
            try:
                self._runnable()
            except BaseException as ex:
                self.set_exc_info(sys.exc_info())
                debug_msg("An exception occurred while running a task. Cause: " + str(ex), exc_info=True)
                raise

        def get(self):
            with self._condition:
                __counter = 0
                while not self.is_done:
                    __counter += 1
                    debug_msg("Python - Wait for result (" + str(__counter) + ").")
                    self._condition.wait()
                exc_info = self._exc_info
                result = self._result
            if exc_info is not None:
                Task.FutureTask._reraise_exception(exc_info)
            else:
                return result

        @staticmethod
        def _reraise_exception(exc_info):
            try:
                import six
            except ImportError:
                # six is not available, "reraise" exception ourselves.
                raise Exception(exc_info[1])
            six.reraise(exc_info[0], exc_info[1], exc_info[2])

        def set(self, result):
            with self._condition:
                debug_msg("Python - Set result: " + str(result))
                self._result = result
                self._is_done = True
                self._condition.notify()

        def set_exc_info(self, exc_info):
            with self._condition:
                if self._exc_info is None and exc_info is not None and exc_info[0] is not None:
                    debug_msg("Python - Set exception: " + str(exc_info[1]))
                    self._exc_info = exc_info
                    self._is_done = True
                    self._condition.notify()
