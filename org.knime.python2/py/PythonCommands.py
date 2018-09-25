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

from PythonUtils import invoke_safely
from messaging.AbstractTaskHandler import AbstractTaskHandler
from messaging.Message import Message
from messaging.Message import PayloadDecoder
from messaging.Message import PayloadEncoder
from messaging.Task import Task
from messaging.Task import TaskFactory


class PythonCommands(object):
    def __init__(self, messaging, workspace):
        self._messaging = messaging
        self._workspace = workspace

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def message_handlers(self):
        return self._messaging

    def create_task(self, task_handler, message, executor=None):
        return Task(message, task_handler, self._messaging, self._messaging, self._messaging.create_receive_queue(),
                    self._messaging.create_next_message_id,
                    self._workspace, executor if executor is not None else self._workspace.executor)

    def create_task_factory(self, task_handler, executor=None):
        return TaskFactory(task_handler, self._messaging, self._messaging, self._messaging.create_receive_queue,
                           self._messaging.create_next_message_id,
                           self._workspace, executor if executor is not None else self._workspace.executor)

    def request_serializer(self, type_or_id):
        payload = PayloadEncoder().put_string(type_or_id).payload
        return self.create_task(PythonCommands._SerializerRequestTaskHandler(),
                                Message(self._messaging.create_next_message_id(), "serializer_request", payload))

    def request_deserializer(self, id):
        payload = PayloadEncoder().put_string(id).payload
        return self.create_task(PythonCommands._DeserializerRequestTaskHandler(),
                                Message(self._messaging.create_next_message_id(), "deserializer_request", payload))

    def resolve_knime_url(self, url):
        """
        :param url: File must exist as Java side tries to download remote files to a temporary location.
        """
        payload = PayloadEncoder().put_string(url).payload
        return self.create_task(PythonCommands._ResolveKnimeUrlTaskHandler(),
                                Message(self._messaging.create_next_message_id(), "resolve_knime_url", payload))

    def start(self):
        self._messaging.start()

    def close(self):
        invoke_safely(None, lambda m: m.close(), self._messaging)

    # Task handlers:

    class _SerializerRequestTaskHandler(AbstractTaskHandler):
        def _handle_success_message(self, message):
            payload_decoder = PayloadDecoder(message.payload)
            values = list()
            values.append(payload_decoder.get_next_string())
            values.append(payload_decoder.get_next_string())
            values.append(payload_decoder.get_next_string())
            return values

    class _DeserializerRequestTaskHandler(AbstractTaskHandler):
        def _handle_success_message(self, message):
            payload_decoder = PayloadDecoder(message.payload)
            values = list()
            values.append(payload_decoder.get_next_string())
            values.append(payload_decoder.get_next_string())
            return values

    class _ResolveKnimeUrlTaskHandler(AbstractTaskHandler):
        def _handle_success_message(self, message):
            return PayloadDecoder(message.payload).get_next_string()
