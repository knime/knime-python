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

import abc

from debug_util import debug_msg
from messaging.Message import PayloadDecoder


class AbstractTaskHandler(object):
    __metaclass__ = abc.ABCMeta

    FIELD_KEY_MESSAGE_TYPE = "type"

    MESSAGE_TYPE_SUCCESS = "success"

    MESSAGE_TYPE_FAILURE = "failure"

    def handle(self, message, message_handlers, message_id_supplier, result_consumer, workspace):
        message_type = message.get_header_field(AbstractTaskHandler.FIELD_KEY_MESSAGE_TYPE)
        debug_msg("Python - Handle task, message: " + str(message))
        if message_type == AbstractTaskHandler.MESSAGE_TYPE_SUCCESS:
            result = self._handle_success_message(message)
            debug_msg("Python - Handled task, message: " + str(message) + ", result: " + str(result))
            result_consumer(result)
        elif message_type == AbstractTaskHandler.MESSAGE_TYPE_FAILURE:
            self._handle_failure_message(message)
        else:
            message_to_send = [None]
            if self._handle_custom_message(message, message_id_supplier, message_to_send, result_consumer, workspace):
                debug_msg(
                    "Python - Handled task, message: " + str(message) + ", follow-up: " + str(message_to_send[0]))
                return message_to_send[0]
            else:
                if not message_handlers.handle(message):
                    raise RuntimeError("Message '" + str(message) + "' could not be handled.")
        return None

    def _handle_success_message(self, message):
        return None

    def _handle_failure_message(self, message):
        if message.payload is not None:
            error_message = PayloadDecoder(message.payload).get_next_string()
        else:
            error_message = "Java task failed for unknown reasons."
        raise RuntimeError(error_message)

    def _handle_custom_message(self, message, response_message_id_supplier, response_consumer, result_consumer,
                               workspace):
        raise NotImplementedError("Custom message types are not supported by this task.")
