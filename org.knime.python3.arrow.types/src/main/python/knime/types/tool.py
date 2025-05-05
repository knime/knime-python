from dataclasses import dataclass
import knime.api.types as kt


@dataclass
class WorkflowTool:
    """
    A class representing a tool value in KNIME.
    """

    name: str
    description: str
    parameter_schema: dict

    # TODO extend with data in- and outputs
    def execute(self, parameters: dict) -> str: ...


class WorkflowToolValueFactory(kt.PythonValueFactory):
    """
    Python equivalent of the WorkflowToolValueFactory class in KNIME, that can read
    the Arrow representation of a KNIME workflow tool value and convert it to a Python object.
    """

    def __init__(self):
        kt.PythonValueFactory.__init__(self, WorkflowTool)

    def decode(self, storage):
        if storage is None:
            return None
        return WorkflowTool(
            name=storage["0"], description=storage["1"], parameter_schema=storage["2"]
        )

    def encode(self, value: WorkflowTool):
        if value is None:
            return None
        return {
            "0": value.name,
            "1": value.description,
            "2": value.parameter_schema,
        }
