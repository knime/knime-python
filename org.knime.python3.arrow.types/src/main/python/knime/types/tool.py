from dataclasses import dataclass
from typing import List, Optional
import knime.api.types as kt


@dataclass
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

    @classmethod
    def _from_arrow_dict(cls, storage):
        if storage is None:
            return None
        return cls(
            name=storage["0"],
            description=storage["1"],
            type=storage["2"],
            spec=storage["3"],
        )


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
            input_ports=[ToolPort._from_arrow_dict(port) for port in storage["4"]],
            output_ports=[ToolPort._from_arrow_dict(port) for port in storage["5"]],
        )

    def encode(self, value: WorkflowTool):
        import json

        if value is None:
            return None
        return {
            "0": value.name,
            "1": value.description,
            "2": json.dumps(value.parameter_schema),
            "3": value.tool_bytes,
            "4": [port._to_arrow_dict() for port in value.input_ports],
            "5": [port._to_arrow_dict() for port in value.output_ports],
        }
