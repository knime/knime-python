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

if EnvironmentHelper.is_python3():
    import importlib
else:
    import imp

import os
import types

from Borg import Borg


class TypeExtensionManager(Borg):
    """
    Used for managing all registered serializers and deserializers.
    Serializers and deserializers can be accessed using the identifier, which is the id of the java extension point or
    the type_string corresponding to the python type. This type string is set in the extension point's specification in
    plugin.xml.
    """

    def __init__(self, commands):
        Borg.__init__(self)
        self._commands = commands
        self._serializer_id_to_index = {}
        self._serializer_type_to_id = {}
        self._serializers = []
        self._deserializer_id_to_index = {}
        self._deserializers = []

    def add_serializer(self, identifier, type_string, path):
        """
        Add a serializer for a python type.
        @param identifier the java extension point id (string)
        @param type_string a python type
        @param path the path to the file containing the serializer module
        """
        index = len(self._serializers)
        self._serializers.append(path)
        self._serializer_id_to_index[identifier] = index
        self._serializer_type_to_id[type_string] = identifier

    def add_deserializer(self, identifier, path):
        """
        Add a deserializer for a python type.
        @param identifier the java extension point id (string)
        @param path the path to the file containing the deserializer module
        """
        index = len(self._deserializers)
        self._deserializers.append(path)
        self._deserializer_id_to_index[identifier] = index

    def get_serializer_by_id(self, identifier):
        """
        Get the serializer associated with the given id.
        @param identifier the java extension point id (string)
        @return serializer module (implementing the serialize(object) method) or None on miss
        """
        if identifier not in self._serializer_id_to_index:
            return self._get_extension_by_index(self._request_serializer(identifier), self._serializers)
        return self._get_extension_by_index(self._serializer_id_to_index[identifier], self._serializers)

    def get_serializer_by_type(self, type_string):
        """
        Get the serializer associated with the given type.
        @param type_string a python type
        @return serializer module (implementing the serialize(object) method) or None on miss
        """
        if type_string not in self._serializer_type_to_id:
            return self._get_extension_by_index(self._request_serializer(type_string), self._serializers)
        return self.get_serializer_by_id(self._serializer_type_to_id[type_string])

    def get_serializer_id_by_type(self, type_string):
        """
        Get the java extension point id associated with the given python type.
        @param type_string a python type
        @return java extension point id (string)
        """
        if type_string not in self._serializer_type_to_id:
            if self._request_serializer(type_string) is None:
                return None
        return self._serializer_type_to_id[type_string]

    def get_deserializer_by_id(self, identifier):
        """
        Get the deserializer associated with the given id
        @param identifier the java extension point id (string)
        @return deserializer module (implementing the deserialize(bytes) method) or None on miss
        """
        if identifier not in self._deserializer_id_to_index:
            return self._get_extension_by_index(self._request_deserializer(identifier), self._deserializers)
        return self._get_extension_by_index(self._deserializer_id_to_index[identifier], self._deserializers)

    def _request_serializer(self, type_or_id):
        """
        Request a serializer for the requested python type or extension id from the extension manager on java side.
        @param type_or_id either the python type string or the extension id
        """
        res = self._commands.request_serializer(type_or_id).get()
        if res[0] == '' or res[1] == '':
            # No serializer was found for request.
            raise LookupError('No serializer extension having the id or processing python type "' + type_or_id
                              + '" could be found.')
        self.add_serializer(res[0], res[1], res[2])
        return len(self._serializers) - 1

    def _request_deserializer(self, id):
        """
        Request a deserializer for the requested extension id from the extension manager on java side
        @param id the extension id
        """
        res = self._commands.request_deserializer(id).get()
        if res[0] == '' or res[1] == '':
            # No serializer was found for request.
            raise LookupError('No deserializer extension having the id "' + id + '" could be found.')
        self.add_deserializer(res[0], res[1])
        return len(self._deserializers) - 1

    @staticmethod
    def _get_extension_by_index(index, extensions):
        """
        Get the path to a module at position index in the passed lists of paths (extensions) and load it.
        Return the loaded module.
        @param index a position in the extensions array
        @param extensions a list of paths to python modules (usually self._serializers or self._deserializers)
        @return the module loaded from the specified path
        """
        if index >= len(extensions):
            return None
        type_extension = extensions[index]
        if not isinstance(type_extension, types.ModuleType):
            path = type_extension
            last_separator = path.rfind(os.sep)
            file_extension_start = path.rfind('.')
            module_name = path[last_separator + 1:file_extension_start]
            try:
                if EnvironmentHelper.is_python3():
                    type_extension = importlib.machinery.SourceFileLoader(module_name, path).load_module()
                else:
                    type_extension = imp.load_source(module_name, path)
            except ImportError as error:
                raise ImportError('Error while loading python type extension ' + module_name + '\nCause: ' + str(error))
            extensions[index] = type_extension
        return type_extension
