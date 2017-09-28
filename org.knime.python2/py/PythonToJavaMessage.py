# Used for defining responses that can be sent back to Java
class PythonToJavaMessage(object):
    # @param cmd    a string command to trigger a certain Java response action
    # @param val    the value to process in Java
    # @param requestsData true - the message requests data from java, false otherwise
    def __init__(self, cmd, val, requestsData):
        self._cmd = cmd
        self._val = val
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