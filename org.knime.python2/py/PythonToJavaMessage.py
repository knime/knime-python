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
#  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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

# Used for defining responses that can be sent back to Java
class PythonToJavaMessage(object):
    # @param cmd    a string command to trigger a certain Java response action
    # @param val    the value to process in Java, will be converted to string
    # @param requestsData true - the message requests data from java, false otherwise
    def __init__(self, cmd, val, requestsData):
        self._cmd = cmd
        self._val = str(val)
        self._requestsData = requestsData
    
    # Get the string representation of this message that can be sent over the
    # socket connection to Java    
    def to_string(self):
        reqstr = "r" if self._requestsData else "s"
        return reqstr + ":" + self._cmd + ":" + self._val
    
    def is_data_request(self):
        return self._requestsData
    
    # Parse the response coming back from Java. Returns None for messages
    # that are not requests.
    def parse_response_string(self, val):
        return None

# Used for indicating the successful termination of a command        
class SuccessResponse(PythonToJavaMessage):
    def __init__(self):
        PythonToJavaMessage.__init__(self, 'success', '0', False)

# Used for requesting a serializer from java. The value may either be the
# python type that is to be serialized or the extension id
class SerializerRequest(PythonToJavaMessage):
    def __init__(self, val):
        PythonToJavaMessage.__init__(self, 'serializer_request', val, True)
    
    def parse_response_string(self, val):
        try:
            res = val.split(';')
            if(res[0] != ''):
                return res
        except:
            pass
        return None

# Used for requesting a deserializer from java. The value should be the extension id.       
class DeserializerRequest(PythonToJavaMessage):
    def __init__(self, val):
        PythonToJavaMessage.__init__(self, 'deserializer_request', val, True)
    
    def parse_response_string(self, val):
        try:
            res = val.split(';')
            if(res[0] != ''):
                return res
        except:
            pass
        return None