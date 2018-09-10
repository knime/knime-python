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

from __future__ import print_function

import sys

from messaging.AbstractTaskHandler import AbstractTaskHandler
from messaging.Message import Message
from messaging.Message import PayloadDecoder
from messaging.Message import PayloadEncoder
from messaging.RequestHandlers import AbstractRequestHandler


def test_request_from_java_to_python(workspace):
    import debug_util
    debug_util.debug_msg("Entering test_request_from_java_to_python.")

    class _RequestHandler(AbstractRequestHandler):
        def _respond(self, request, response_message_id, workspace):
            workspace.unregister_task_handler("my-request-from-java")
            payload = PayloadEncoder().put_string("my-response-from-python").payload
            return AbstractRequestHandler._create_response(request, response_message_id, response_payload=payload)

    workspace.register_task_handler("my-request-from-java", _RequestHandler())


def test_request_from_python_to_java(workspace):
    import debug_util
    debug_util.debug_msg("Entering test_request_from_python_to_java.")

    class _TaskHandler(AbstractTaskHandler):
        def _handle_success_message(self, message):
            import debug_util
            debug_util.debug_msg("Inside handler of Java response.")
            return PayloadDecoder(message.payload).get_next_string()

    my_task = workspace._commands.create_task(_TaskHandler(), Message(
        workspace._commands._messaging.create_next_message_id(), "my-request-from-python"))

    print(my_task.get())  # 'flush' keyword argument is not supported by Python 2.
    sys.stdout.flush()


def test_nested_request_from_java_to_python(workspace):
    import debug_util
    debug_util.debug_msg("Entering test_nested_request_from_java_to_python.")

    class _TaskHandler(AbstractTaskHandler):

        def __init__(self):
            super(_TaskHandler, self).__init__()
            self._response_string_payload = ""

        def _handle_custom_message(self, message, response_message_id_supplier, response_consumer, result_consumer,
                                   workspace):
            if message.category == "my-nested-request-from-java":
                request_message_type = "first-request"
                response_payload = None
            else:
                message_type = message.get_header_field(AbstractTaskHandler.FIELD_KEY_MESSAGE_TYPE)
                if message_type == "first":
                    self._response_string_payload += PayloadDecoder(message.payload).get_next_string()
                    request_message_type = "second-request"
                    response_payload = None
                elif message_type == "second":
                    workspace.unregister_task_handler("my-nested-request-from-java")
                    result_consumer(None)
                    self._response_string_payload += PayloadDecoder(message.payload).get_next_string()
                    request_message_type = AbstractTaskHandler.MESSAGE_TYPE_SUCCESS
                    response_payload = PayloadEncoder().put_string(self._response_string_payload).payload
                else:
                    error_message = "Message type '" + message_type + "' is not recognized. Illegal state."
                    debug_util.debug_msg(error_message)
                    raise RuntimeError(error_message)
            response_consumer[0] = Message(workspace._commands._messaging.create_next_message_id(), str(message.id),
                                           payload=response_payload,
                                           additional_options={AbstractTaskHandler.FIELD_KEY_MESSAGE_TYPE:
                                                               request_message_type})
            return True

    workspace.register_task_handler("my-nested-request-from-java", _TaskHandler())


def test_request_from_java_that_causes_request_from_python(workspace):
    import debug_util
    debug_util.debug_msg("Entering test_request_from_java_that_causes_request_from_python.")

    class _RequestHandler(AbstractRequestHandler):
        def _respond(self, request, response_message_id, workspace):
            workspace.unregister_task_handler("my-request-from-java-that-causes-a-request-from-python")
            result_from_request = trigger_request_from_python(workspace)
            payload = PayloadEncoder().put_string(result_from_request + "-made-the-task-succeed").payload
            return AbstractRequestHandler._create_response(request, response_message_id, True, payload)

    workspace.register_task_handler("my-request-from-java-that-causes-a-request-from-python", _RequestHandler())


def trigger_request_from_python(workspace):
    class _TaskHandler(AbstractTaskHandler):
        def _handle_success_message(self, message):
            return PayloadDecoder(message.payload).get_next_string()

    my_task = workspace._commands.create_task(_TaskHandler(),
                                              Message(workspace._commands._messaging.create_next_message_id(),
                                                      "caused-request-from-python"))
    return my_task.get()
