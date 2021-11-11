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
@author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
@author Patrick Winter, KNIME GmbH, Konstanz, Germany
@author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
@author Christian Dietz, KNIME GmbH, Konstanz, Germany
"""

# This should be the first statement in each module that makes specific demands on the Python environment.
import EnvironmentHelper

# Make sure the import stays at the top (when auto-formatting).
EnvironmentHelper.dummy_call()

import abc
import collections
import os
import pickle
import sys

import pandas

import PythonUtils

import traceback

from debug_util import debug_msg
from DBUtil import DBUtil
from messaging.AbstractTaskHandler import AbstractTaskHandler
from messaging.Message import Message
from messaging.Message import PayloadDecoder
from messaging.Message import PayloadEncoder


class AbstractRequestHandler(AbstractTaskHandler):
    @staticmethod
    def _create_response(request, response_message_id, success=True, response_payload=None,
                         response_additional_options=None):
        options = response_additional_options or {}
        options[AbstractTaskHandler.FIELD_KEY_MESSAGE_TYPE] = (
            AbstractTaskHandler.MESSAGE_TYPE_SUCCESS if success else AbstractTaskHandler.MESSAGE_TYPE_FAILURE)
        return Message(response_message_id, str(request.id), response_payload, options)

    @abc.abstractmethod
    def _respond(self, request, response_message_id, workspace):
        raise NotImplementedError()

    def _handle_custom_message(self, message, response_message_id_supplier, response_consumer, result_consumer,
                               workspace):
        response_message_id = response_message_id_supplier()
        try:
            debug_msg("Python - Respond to message: " + str(message))
            response = self._respond(message, response_message_id, workspace)
            debug_msg("Python - Responded to message: " + str(message) + ", response: " + str(response))
            response_consumer[0] = response
        except Exception as ex:
            error_message = str(ex)
            debug_msg(error_message, exc_info=True)
            # Message format: Error message, pretty traceback, number of traceback elements (frames), list of frames
            # where each frame is of the form: filename, line number, name, line.
            error_pretty_traceback = traceback.format_exc()
            error_traceback_frames = traceback.extract_tb(sys.exc_info()[2])
            error_payload = PayloadEncoder().put_string(error_message).put_string(error_pretty_traceback).put_int(
                len(error_traceback_frames))
            for frame in reversed(error_traceback_frames):
                error_payload.put_string(frame[0]).put_int(frame[1]).put_string(frame[2]).put_string(frame[3])
            error_payload = error_payload.payload
            # Inform Java that handling the request did not work.
            error_response = AbstractRequestHandler._create_response(message, response_message_id, success=False,
                                                                     response_payload=error_payload)
            response_consumer[0] = error_response
        result_consumer(None)  # We are done after the response (either success or failure) is sent.
        return True


# Implementations:

_PAYLOAD_NAME = "payload_name"


class GetPidRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        pid = os.getpid()

        return AbstractRequestHandler._create_response(request, response_message_id,
                                                       response_payload=_create_int_payload(pid))


class PutFlowVariablesRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        payload_decoder = PayloadDecoder(request.payload)
        data_bytes = payload_decoder.get_next_bytes()
        name = request.get_header_field(_PAYLOAD_NAME)

        flow_variables = collections.OrderedDict()
        data_frame = workspace.serializer.bytes_to_data_frame(data_bytes)
        workspace.serializer.fill_flow_variables_from_data_frame(flow_variables, data_frame)
        workspace.put_variable(name, flow_variables)

        return AbstractRequestHandler._create_response(request, response_message_id)


class GetFlowVariablesRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        name = PayloadDecoder(request.payload).get_next_string()

        variables = workspace.get_variable(name)
        data_frame = workspace.serializer.flow_variables_dict_to_data_frame(variables)
        data_bytes = workspace.serializer.data_frame_to_bytes(data_frame)

        return AbstractRequestHandler._create_response(request, response_message_id,
                                                       response_payload=_create_byte_array_payload(data_bytes))


class PutTableRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        payload_decoder = PayloadDecoder(request.payload)
        data_bytes = payload_decoder.get_next_bytes()
        name = request.get_header_field(_PAYLOAD_NAME)

        data_frame = workspace.serializer.bytes_to_data_frame(data_bytes)
        workspace.put_variable(name, data_frame)

        return AbstractRequestHandler._create_response(request, response_message_id)


class AppendToTableRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        payload_decoder = PayloadDecoder(request.payload)
        data_bytes = payload_decoder.get_next_bytes()
        name = request.get_header_field(_PAYLOAD_NAME)

        data_frame = workspace.serializer.bytes_to_data_frame(data_bytes)
        workspace.append_to_table(name, data_frame)

        return AbstractRequestHandler._create_response(request, response_message_id)


class GetTableSizeRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        name = PayloadDecoder(request.payload).get_next_string()

        data_frame = workspace.get_variable(name)

        return AbstractRequestHandler._create_response(request, response_message_id,
                                                       response_payload=_create_int_payload(len(data_frame)))


class GetTableRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        name = PayloadDecoder(request.payload).get_next_string()

        data_frame = workspace.get_variable(name)
        if type(data_frame) != pandas.core.frame.DataFrame:
            raise TypeError("Expected pandas.DataFrame, got: " + str(type(data_frame))
                            + "\nPlease make sure your output_table is a pandas.DataFrame.")
        data_bytes = workspace.serializer.data_frame_to_bytes(data_frame)

        return AbstractRequestHandler._create_response(request, response_message_id,
                                                       response_payload=_create_byte_array_payload(data_bytes))


class GetTableChunkRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        payload_decoder = PayloadDecoder(request.payload)
        name = payload_decoder.get_next_string()
        start = payload_decoder.get_next_int()
        end = payload_decoder.get_next_int()

        data_frame = workspace.get_variable(name)
        if type(data_frame) != pandas.core.frame.DataFrame:
            raise TypeError("Expected pandas.DataFrame, got: " + str(type(data_frame))
                            + "\nPlease make sure your output_table is a pandas.DataFrame.")
        data_frame_chunk = data_frame[start:end + 1]
        data_bytes = workspace.serializer.data_frame_to_bytes(data_frame_chunk, start)

        return AbstractRequestHandler._create_response(request, response_message_id,
                                                       response_payload=_create_byte_array_payload(data_bytes))


class PutObjectRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        payload_decoder = PayloadDecoder(request.payload)
        # data_bytes = payload_decoder.get_next_bytes()
        path = payload_decoder.get_next_string()
        name = request.get_header_field(_PAYLOAD_NAME)
        with open(path, "rb") as file:
            data_object = pickle.load(file)
            workspace.put_variable(name, data_object)

        return AbstractRequestHandler._create_response(request, response_message_id)


class GetObjectRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        payload_decoder = PayloadDecoder(request.payload)
        name = payload_decoder.get_next_string()
        path = payload_decoder.get_next_string()

        data_object = workspace.get_variable(name)
        o_type = type(data_object).__name__
        o_representation = PythonUtils.object_to_string(data_object)
        with open(path, "wb") as file:
            pickle.dump(obj=data_object, file=file)
        payload = PayloadEncoder().put_string(o_type).put_string(o_representation).payload

        return AbstractRequestHandler._create_response(request, response_message_id,
                                                       response_payload=payload)


class PutSqlRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        payload_decoder = PayloadDecoder(request.payload)
        data_bytes = payload_decoder.get_next_bytes()
        name = request.get_header_field(_PAYLOAD_NAME)

        data_frame = workspace.serializer.bytes_to_data_frame(data_bytes)
        db_util = DBUtil(data_frame)
        workspace.put_variable(name, db_util)
        workspace.add_cleanup_object_name(name)

        return AbstractRequestHandler._create_response(request, response_message_id)


class GetSqlRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        name = PayloadDecoder(request.payload).get_next_string()

        db_util = workspace.get_variable(name)
        db_util._writer.commit()
        query = db_util.get_output_query()

        return AbstractRequestHandler._create_response(request, response_message_id,
                                                       response_payload=_create_string_payload(query))


class GetImageRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        name = PayloadDecoder(request.payload).get_next_string()

        image = workspace.get_variable_or_default(name, None)
        if EnvironmentHelper.is_python3():
            if type(image) is bytes:
                data_bytes = image
            else:
                data_bytes = bytearray()
        else:
            if type(image) is str:
                data_bytes = image
            else:
                data_bytes = ''

        return AbstractRequestHandler._create_response(request, response_message_id,
                                                       response_payload=_create_byte_array_payload(data_bytes))


class ListVariablesRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        variables = workspace.list_variables()
        data_frame = pandas.DataFrame(variables)
        data_bytes = workspace.serializer.data_frame_to_bytes(data_frame)

        return AbstractRequestHandler._create_response(request, response_message_id,
                                                       response_payload=_create_byte_array_payload(data_bytes))


class HasAutoCompleteRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        value = 1 if workspace.has_auto_complete() else 0

        return AbstractRequestHandler._create_response(request, response_message_id,
                                                       response_payload=_create_int_payload(value))


class AutoCompleteRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        payload_decoder = PayloadDecoder(request.payload)
        source_code = payload_decoder.get_next_string()
        line = payload_decoder.get_next_int()
        column = payload_decoder.get_next_int()

        suggestions = workspace.auto_complete(source_code, line, column)
        data_frame = pandas.DataFrame(suggestions)
        data_bytes = workspace.serializer.data_frame_to_bytes(data_frame)

        return AbstractRequestHandler._create_response(request, response_message_id,
                                                       response_payload=_create_byte_array_payload(data_bytes))


class AddSerializerRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        payload_decoder = PayloadDecoder(request.payload)
        s_id = payload_decoder.get_next_string()
        s_type = payload_decoder.get_next_string()
        s_path = payload_decoder.get_next_string()

        workspace.type_extension_manager.add_serializer(s_id, s_type, s_path)

        return AbstractRequestHandler._create_response(request, response_message_id)


class AddDeserializerRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        payload_decoder = PayloadDecoder(request.payload)
        d_id = payload_decoder.get_next_string()
        d_path = payload_decoder.get_next_string()

        workspace.type_extension_manager.add_deserializer(d_id, d_path)

        return AbstractRequestHandler._create_response(request, response_message_id)


class SetSerializationLibraryRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        path_to_serialization_library_module = PayloadDecoder(request.payload).get_next_string()

        workspace.set_serialization_library(path_to_serialization_library_module)

        return AbstractRequestHandler._create_response(request, response_message_id)


class SetCustomModulePathsRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        path = PayloadDecoder(request.payload).get_next_string()

        sys.path.append(path)

        return AbstractRequestHandler._create_response(request, response_message_id)


class ExecuteRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        source_code = PayloadDecoder(request.payload).get_next_string()

        debug_msg('Executing:\n' + source_code)
        output, error = workspace.execute(source_code, request.id)
        debug_msg('Execution done.')

        response_payload = PayloadEncoder().put_string(output).put_string(error).payload
        return AbstractRequestHandler._create_response(request, response_message_id, response_payload=response_payload)


class ResetRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        workspace.reset()
        return AbstractRequestHandler._create_response(request, response_message_id)


class CleanupRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        workspace._cleanup()
        return AbstractRequestHandler._create_response(request, response_message_id)


class ShutdownRequestHandler(AbstractRequestHandler):
    def _respond(self, request, response_message_id, workspace):
        # TODO: Send success message?
        workspace.close()


# Note that all builtin request handlers need to be stateless since a single instance is used per message category.
_builtin_request_handlers = {'getpid': GetPidRequestHandler(),
                             'putFlowVariables': PutFlowVariablesRequestHandler(),
                             'getFlowVariables': GetFlowVariablesRequestHandler(),
                             'putTable': PutTableRequestHandler(),
                             'appendToTable': AppendToTableRequestHandler(),
                             'getTableSize': GetTableSizeRequestHandler(),
                             'getTable': GetTableRequestHandler(),
                             'getTableChunk': GetTableChunkRequestHandler(),
                             'putObject': PutObjectRequestHandler(),
                             'getObject': GetObjectRequestHandler(),
                             'putSql': PutSqlRequestHandler(),
                             'getSql': GetSqlRequestHandler(),
                             'getImage': GetImageRequestHandler(),
                             'listVariables': ListVariablesRequestHandler(),
                             'hasAutoComplete': HasAutoCompleteRequestHandler(),
                             'autoComplete': AutoCompleteRequestHandler(),
                             'addSerializer': AddSerializerRequestHandler(),
                             'addDeserializer': AddDeserializerRequestHandler(),
                             'setSerializationLibrary': SetSerializationLibraryRequestHandler(),
                             'setCustomModulePaths': SetCustomModulePathsRequestHandler(),
                             'execute': ExecuteRequestHandler(),
                             'execute_async': ExecuteRequestHandler(),
                             'cleanup': CleanupRequestHandler(),
                             'shutdown': ShutdownRequestHandler()}


def get_builtin_request_handlers():
    return _builtin_request_handlers.copy()


def _create_byte_array_payload(value):
    return PayloadEncoder().put_bytes(value).payload


def _create_int_payload(value):
    return PayloadEncoder().put_int(value).payload


def _create_string_payload(value):
    return PayloadEncoder().put_string(value).payload
