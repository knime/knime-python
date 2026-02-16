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
    message_output_port_index: int
    input_ports: Optional[List[ToolPort]] = None
    output_ports: Optional[List[ToolPort]] = None
    _filestore_keys: Optional[List[str]] = None


class WorkflowToolValueFactory(kt.FileStorePythonValueFactory):
    """
    Python equivalent of the WorkflowToolValueFactory class in KNIME, that can read
    the Arrow representation of a KNIME workflow tool value and convert it to a Python object.
    """

    def __init__(self):
        kt.FileStorePythonValueFactory.__init__(self, WorkflowTool)

    def read(self, file_paths, table_data):
        # file_paths is not accessed because we only pass on the filestore keys
        import json

        param_schema_json = table_data["2"]
        param_schema = json.loads(param_schema_json) if param_schema_json else {}

        return WorkflowTool(
            name=table_data["0"],
            description=table_data["1"],
            parameter_schema=param_schema,
            message_output_port_index=table_data["5"],
            input_ports=[ToolPort._from_arrow_dict(port) for port in table_data["3"]],
            output_ports=[ToolPort._from_arrow_dict(port) for port in table_data["4"]],
        )

    def decode(self, storage):
        # calls the read method and constructs the Python readable part of the tool
        tool = super().decode(storage)
        # the filestore is not accessed in Python but we need it to execute the tool
        tool._filestore_keys = storage.get("0")
        return tool

    def write(self, file_store_creator, value):
        # we don't create filestores here. The encode method takes care of passing the filestore keys along
        import json

        if value is None:
            return None
        return {
            "0": value.name,
            "1": value.description,
            "2": json.dumps(value.parameter_schema),
            "3": [port._to_arrow_dict() for port in value.input_ports],
            "4": [port._to_arrow_dict() for port in value.output_ports],
            "5": value.message_output_port_index,
        }

    def encode(self, value: WorkflowTool):
        encoded = super().encode(value)
        return {
            "0": value._filestore_keys,  # the filestore keys we read in the decode method
            "1": encoded["1"],  # the rest of the encoded data
        }


@dataclass
class MCPTool:
    """
    A class representing an MCP (Model Context Protocol) tool value in KNIME.
    """

    name: str
    description: str
    parameter_schema: dict
    server_uri: str
    tool_name: str
    input_schema: Optional[dict] = None
    output_type: Optional[str] = None


class MCPToolValueFactory(kt.PythonValueFactory):
    """
    Python equivalent of MCPToolValueFactory that can read and write
    the Arrow representation of an MCP tool value.
    """

    def __init__(self):
        kt.PythonValueFactory.__init__(self, MCPTool)

    def decode(self, storage):
        import json

        if storage is None:
            return None

        param_schema_json = storage.get("2")
        param_schema = json.loads(param_schema_json) if param_schema_json else {}

        input_schema_json = storage.get("5")
        input_schema = json.loads(input_schema_json) if input_schema_json else None

        return MCPTool(
            name=storage["0"],
            description=storage["1"],
            parameter_schema=param_schema,
            server_uri=storage["3"],
            tool_name=storage["4"],
            input_schema=input_schema,
            output_type=storage.get("6"),
        )

    def encode(self, value: MCPTool):
        import json

        if value is None:
            return None

        return {
            "0": value.name,
            "1": value.description,
            "2": json.dumps(value.parameter_schema),
            "3": value.server_uri,
            "4": value.tool_name,
            "5": json.dumps(value.input_schema) if value.input_schema else None,
            "6": value.output_type,
        }
