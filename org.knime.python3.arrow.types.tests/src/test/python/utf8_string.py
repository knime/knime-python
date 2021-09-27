import knime_types as kt


class Utf8EncodedString:

    def __init__(self, value):
        self.value = value


class Utf8EncodedStringFactory(kt.PythonValueFactory):

    def __init__(self):
        kt.PythonValueFactory.__init__(self, Utf8EncodedString)

    def decode(self, storage):
        return Utf8EncodedString(storage.decode("utf-8", "strict"))

    def encode(self, value):
        return value.value.encode("utf-8")
