import knime_node as kn

def create_node(node_id: str) -> kn.PythonNode:
    if node_id == "first":
        return MyPythonNode()
    else:
        raise ValueError(f"Unsupported node id {node_id} encountered.")

class MyPythonNode(kn.PythonNode):

    def __init__(self) -> None:
        super().__init__()
        self._string_parameter = "foobar"
        self._numerical_parameter = 2

    @kn.parameter
    def string_parameter(self):
        return self._string_parameter

    @string_parameter.setter
    def string_parameter(self, value):
        self._string_parameter = value

    @kn.parameter
    def numerical_parameter(self):
        return self._numerical_parameter

    @numerical_parameter.setter
    def numerical_parameter(self, value):
        if value < 0:
            raise ValueError("The value of numerical_parameter must be non-negative.")
        self._numerical_parameter = value

    def configure(self):
        return super().configure()

    def execute(self):
        return super().execute()
