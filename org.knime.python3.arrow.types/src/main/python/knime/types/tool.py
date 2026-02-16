from dataclasses import dataclass
from enum import IntEnum
from typing import List, Optional, Union
import knime.api.types as kt
import warnings


class ToolType(IntEnum):
    """Tool type enumeration matching Java enum indices."""
    WORKFLOW = 0
    MCP = 1


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
    A class representing a workflow-based tool in KNIME.
    """

    name: str
    description: str
    parameter_schema: dict
    message_output_port_index: int
    input_ports: Optional[List[ToolPort]] = None
    output_ports: Optional[List[ToolPort]] = None
    _filestore_keys: Optional[List[str]] = None

    @property
    def tool_type(self) -> ToolType:
        return ToolType.WORKFLOW


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

    @property
    def tool_type(self) -> ToolType:
        return ToolType.MCP


# Type alias for unified tool handling
Tool = Union[WorkflowTool, MCPTool]


class ToolValueFactory(kt.PythonValueFactory):
    """
    Unified value factory for both workflow and MCP tools.
    Uses byte-indexed enum for tool type discrimination.
    """

    def __init__(self):
        kt.PythonValueFactory.__init__(self, Tool)

    def decode(self, storage):
        import json

        if storage is None:
            return None

        tool_type_index = storage.get("0")
        tool_type = ToolType(tool_type_index)

        # Common fields
        name = storage["1"]
        description = storage["2"]
        param_schema_json = storage.get("3")
        param_schema = json.loads(param_schema_json) if param_schema_json else {}

        if tool_type == ToolType.WORKFLOW:
            # Decode WorkflowTool from fields 4-7
            input_spec_json = storage.get("4")
            output_spec_json = storage.get("5")

            tool = WorkflowTool(
                name=name,
                description=description,
                parameter_schema=param_schema,
                message_output_port_index=storage.get("6", -1),
                input_ports=[
                    ToolPort._from_arrow_dict(p) for p in json.loads(input_spec_json)
                ]
                if input_spec_json
                else [],
                output_ports=[
                    ToolPort._from_arrow_dict(p) for p in json.loads(output_spec_json)
                ]
                if output_spec_json
                else [],
            )
            # Filestore keys from field 7
            tool._filestore_keys = storage.get("7")
            return tool

        elif tool_type == ToolType.MCP:
            # Decode MCPTool from field 8
            return MCPTool(
                name=name,
                description=description,
                parameter_schema=param_schema,
                server_uri=storage["8"],
                tool_name=name,  # Use name as tool_name
            )
        else:
            raise ValueError(f"Unknown tool type index: {tool_type_index}")

    def encode(self, value: Tool):
        import json

        if value is None:
            return None

        # Common fields (0-3)
        encoded = {
            "0": value.tool_type.value,  # Byte: enum index
            "1": value.name,
            "2": value.description,
            "3": json.dumps(value.parameter_schema),
        }

        if isinstance(value, WorkflowTool):
            # Workflow-specific fields (4-7)
            encoded.update(
                {
                    "4": json.dumps(
                        [p._to_arrow_dict() for p in value.input_ports]
                    )
                    if value.input_ports
                    else None,  # input_spec
                    "5": json.dumps(
                        [p._to_arrow_dict() for p in value.output_ports]
                    )
                    if value.output_ports
                    else None,  # output_spec
                    "6": value.message_output_port_index,
                    "7": value._filestore_keys,  # workflow filestore
                    "8": None,  # server_uri (MCP only)
                }
            )
        elif isinstance(value, MCPTool):
            # MCP-specific fields (8), nulls for Workflow fields (4-7)
            encoded.update(
                {
                    "4": None,  # input_spec (Workflow only)
                    "5": None,  # output_spec (Workflow only)
                    "6": -1,  # message_output_port_index
                    "7": None,  # workflow_filestore (Workflow only)
                    "8": value.server_uri,
                }
            )
        else:
            raise TypeError(f"Unknown tool type: {type(value)}")

        return encoded


# Deprecated: Old value factories kept for backward compatibility
class MCPToolValueFactory(ToolValueFactory):
    """
    @deprecated Use ToolValueFactory instead.
    Kept for backward compatibility during migration period.
    """

    def __init__(self):
        super().__init__()
        warnings.warn(
            "MCPToolValueFactory is deprecated, use ToolValueFactory instead",
            DeprecationWarning,
            stacklevel=2,
        )
