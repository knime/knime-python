from PythonToJavaMessage import *
import types
import os
import sys
import debug_util

# check if we are running python 2 or python 3
_python3 = sys.version_info >= (3, 0)

if _python3:
    import importlib
else:
    import imp

# Singleton/BorgSingleton.py
# Alex Martelli's 'Borg' singleton implementation

class Borg:
    _shared_state = {}
    def __init__(self):
        self.__dict__ = self._shared_state

# Used for managing all registered serializers and deserializers.
# Serializers and deserializers can be accessed using the identifier,
# which is the id of the java extension point or the type_string corresponding
# to the python type. This type string is set in the extension point's specification
# in plugin.xml.
class TypeExtensionManager(Borg):
    def __init__(self, writeResponseFn):
        Borg.__init__(self)
        self._writeResponseFn = writeResponseFn
        self._serializer_id_to_index = {}
        self._serializer_type_to_id = {}
        self._serializers = []
        self._deserializer_id_to_index = {}
        self._deserializers = []

    # Get the deserializer associated with the given id
    # @param identifier    the java extension point id (string)
    # @return deserializer module (implementing the deserialize(bytes) method) or None on miss
    def get_deserializer_by_id(self, identifier):
        if identifier not in self._deserializer_id_to_index:
            return self.get_extension_by_index(self.request_deserializer(identifier), self._deserializers)
        return self.get_extension_by_index(self._deserializer_id_to_index[identifier], self._deserializers)

    # Get the serializer associated with the given id
    # @param identifier    the java extension point id (string)
    # @return serializer module (implementing the serialize(object) method) or None on miss
    def get_serializer_by_id(self, identifier):
        if identifier not in self._serializer_id_to_index:
            return self.get_extension_by_index(self.request_serializer(identifier), self._serializers)
        return self.get_extension_by_index(self._serializer_id_to_index[identifier], self._serializers)

    # Get the serializer associated with the given type
    # @param identifier    a python type
    # @return serializer module (implementing the serialize(object) method) or None on miss
    def get_serializer_by_type(self, type_string):
        if type_string not in self._serializer_type_to_id:
            return self.get_extension_by_index(self.request_serializer(type_string), self._serializers)
        return self.get_serializer_by_id(self._serializer_type_to_id[type_string])

    # Get the java extension point id associated with the given python type
    # @param type_string    a python type
    # @return java extension point id (string)
    def get_serializer_id_by_type(self, type_string):
        if type_string not in self._serializer_type_to_id:
            if self.request_serializer(type_string) == None:
                return None
        return self._serializer_type_to_id[type_string]

    # Get the path to a module at position index in the passed lists of paths 
    # (extensions) and load it. Return the loaded module.
    # @param index         a position in the extensions array
    # @param extensions    a list of paths to python modules (usually self._serializers or self._deserializers)
    # @return the module loaded from the specified path
    @staticmethod
    def get_extension_by_index(index, extensions):
        if index >= len(extensions):
            return None
        type_extension = extensions[index]
        if not isinstance(type_extension, types.ModuleType):
            path = type_extension
            last_separator = path.rfind(os.sep)
            file_extension_start = path.rfind('.')
            module_name = path[last_separator + 1:file_extension_start]
            try:
                if _python3:
                    type_extension = importlib.machinery.SourceFileLoader(module_name, path).load_module()
                else:
                    type_extension = imp.load_source(module_name, path)
            except ImportError as error:
                raise ImportError('Error while loading python type extension ' + module_name + '\nCause: ' + str(error))
            extensions[index] = type_extension
        return type_extension
    
    # Request a serializer for the requested python type or extension id from the
    # extension manager on java side.
    # @param typeOrId either the python type string or the extension id
    def request_serializer(self, typeOrId):
        res = self._writeResponseFn(SerializerRequest(typeOrId))
        # No serializer was found for request
        if res == None:
            raise LookupError('No serializer extension having the id or processing python type "' + typeOrId + '" could be found.')
        self.add_serializer(res[0], res[1], res[2])
        return len(self._serializers) - 1
    
    # Request a deserializer for the requested extension id from the
    # extension manager on java side
    # @param id the extension id
    def request_deserializer(self, id):
        res = self._writeResponseFn(DeserializerRequest(id))
        # No serializer was found for request
        if res == None:
            raise LookupError('No deserializer extension having the id "' + typeOrId + '" could be found.')
        #debug_util.breakpoint()
        self.add_deserializer(res[0], res[1])
        return len(self._deserializers) - 1

    # Add a serializer for a python type.
    # @param identifier     the java extension point id (string)
    # @param type_string    a python type
    # @param path           the path to the file containing the serializer module
    def add_serializer(self, identifier, type_string, path):
        index = len(self._serializers)
        self._serializers.append(path)
        self._serializer_id_to_index[identifier] = index
        self._serializer_type_to_id[type_string] = identifier

    # Add a deserializer for a python type.
    # @param identifier     the java extension point id (string)
    # @param path           the path to the file containing the deserializer module
    def add_deserializer(self, identifier, path):
        index = len(self._deserializers)
        self._deserializers.append(path)
        self._deserializer_id_to_index[identifier] = index