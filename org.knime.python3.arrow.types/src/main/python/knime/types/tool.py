from dataclasses import dataclass
from typing import List, Optional
import knime.api.types as kt


class ToolPort:
    name: str
    description: str
    type: str
    spec: Optional[str]

    def _to_arrow_dict(self):
        return {
            "0": self.name,
            "1": self.description,
            "2": self.type,
            "3": self.spec,
        }


@dataclass
class WorkflowTool:
    """
    A class representing a tool value in KNIME.
    """

    name: str
    description: str
    parameter_schema: dict
    tool_bytes: bytes
    input_ports: Optional[List[ToolPort]] = None
    output_ports: Optional[List[ToolPort]] = None

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
        import json

        return WorkflowTool(
            name=storage["0"],
            description=storage["1"],
            parameter_schema=json.loads(storage["2"]),
            tool_bytes=storage["3"],
        )

    def encode(self, value: WorkflowTool):
        if value is None:
            return None
        return {
            "0": value.name,
            "1": value.description,
            "2": value.parameter_schema,
            "3": value.tool_bytes,
            "4": [port._to_arrow_dict() for port in value.input_ports],
            "5": [port._to_arrow_dict() for port in value.output_ports],
        }
