from dataclasses import dataclass
from enum import IntEnum
from typing import List, Optional
import knime.api.types as kt


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
    @deprecated Use Tool class instead.
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
            "1": encoded["1"] if encoded else "",  # the rest of the encoded data
        }


@dataclass
class Tool:
    """
    Unified tool class representing either a workflow-based tool or an MCP tool.

    Mirrors the Java ToolCell architecture with internal type discrimination.
    Use tool_type property to check which type, then access type-specific fields.
    """

    tool_type: ToolType
    name: str
    description: str
    parameter_schema: dict

    # Workflow-specific fields (None for MCP tools)
    message_output_port_index: Optional[int] = None
    input_ports: Optional[List[ToolPort]] = None
    output_ports: Optional[List[ToolPort]] = None
    _filestore_keys: Optional[List[str]] = None

    # MCP-specific fields (None for workflow tools)
    server_uri: Optional[str] = None
    tool_name: Optional[str] = None

    # Credential reference for authenticated MCP servers (None if unauthenticated)
    credential_name: Optional[str] = None

    @classmethod
    def create_workflow_tool(
        cls,
        name: str,
        description: str,
        parameter_schema: dict,
        message_output_port_index: int,
        input_ports: Optional[List[ToolPort]] = None,
        output_ports: Optional[List[ToolPort]] = None,
        filestore_keys: Optional[List[str]] = None,
    ) -> "Tool":
        """Create a workflow-based tool."""
        return cls(
            tool_type=ToolType.WORKFLOW,
            name=name,
            description=description,
            parameter_schema=parameter_schema,
            message_output_port_index=message_output_port_index,
            input_ports=input_ports,
            output_ports=output_ports,
            _filestore_keys=filestore_keys,
            server_uri=None,
            tool_name=None,
        )

    @classmethod
    def create_mcp_tool(
        cls,
        name: str,
        description: str,
        parameter_schema: dict,
        server_uri: str,
        tool_name: str,
        credential_name: Optional[str] = None,
    ) -> "Tool":
        """Create an MCP tool.

        Parameters
        ----------
        credential_name : str, optional
            Name of the KNIME credentials flow variable to use for
            authenticating with the MCP server. ``None`` if no auth required.
        """
        return cls(
            tool_type=ToolType.MCP,
            name=name,
            description=description,
            parameter_schema=parameter_schema,
            message_output_port_index=None,
            input_ports=None,
            output_ports=None,
            _filestore_keys=None,
            server_uri=server_uri,
            tool_name=tool_name,
            credential_name=credential_name,
        )


debugger_attached = False


class ToolValueFactory(kt.FileStorePythonValueFactory):
    """
    Unified value factory for both workflow and MCP tools.
    Uses byte-indexed enum for tool type discrimination.
    """

    def __init__(self):
        kt.FileStorePythonValueFactory.__init__(self, Tool)

    def decode(self, storage):
        # calls the read method and constructs the Python readable part of the tool
        tool = super().decode(storage)
        # the filestore is not accessed in Python but we need it to execute the tool
        tool._filestore_keys = storage.get("0")
        return tool

    def encode(self, value: Tool):
        encoded = super().encode(value)
        return {
            # the filestore keys we read in the decode method, if present
            "0": value._filestore_keys or "",
            "1": encoded["1"] if encoded else "",  # the rest of the encoded data
        }

    def read(self, file_paths, table_data):
        # file_paths is not accessed because we only pass on the filestore keys
        import json

        if table_data is None:
            return None

        tool_type_index = table_data.get("0")
        tool_type = ToolType(tool_type_index)

        # Common fields
        name = table_data["1"]
        description = table_data["2"]
        param_schema_json = table_data.get("3")
        param_schema = json.loads(param_schema_json) if param_schema_json else {}

        if tool_type == ToolType.WORKFLOW:
            # Decode WorkflowTool from fields 4-7
            input_spec_json = table_data.get("4")
            output_spec_json = table_data.get("5")

            tool = Tool.create_workflow_tool(
                name=name,
                description=description,
                parameter_schema=param_schema,
                message_output_port_index=table_data.get("6", -1),
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
                filestore_keys=table_data.get("7"),
            )
            return tool

        elif tool_type == ToolType.MCP:
            # Decode MCPTool from fields 8-9
            credential_name_raw = table_data.get("9")
            return Tool.create_mcp_tool(
                name=name,
                description=description,
                parameter_schema=param_schema,
                server_uri=table_data["8"],
                tool_name=name,  # Use name as tool_name
                credential_name=credential_name_raw if credential_name_raw else None,
            )
        else:
            raise ValueError(f"Unknown tool type index: {tool_type_index}")

    def write(self, file_store_creator, value: Tool):
        # we don't create filestores here. The encode method takes care of passing the filestore keys along
        import json

        if value is None:
            return None

        # Common fields (0-3)
        encoded = {
            "0": value.tool_type.value,  # Int: enum index
            "1": value.name,
            "2": value.description,
            "3": json.dumps(value.parameter_schema),
        }

        if value.tool_type == ToolType.WORKFLOW:
            # Workflow-specific fields (4-7)
            encoded.update(
                {
                    "4": json.dumps([p._to_arrow_dict() for p in value.input_ports])
                    if value.input_ports
                    else None,  # input_spec
                    "5": json.dumps([p._to_arrow_dict() for p in value.output_ports])
                    if value.output_ports
                    else None,  # output_spec
                    "6": value.message_output_port_index,
                    "7": value._filestore_keys,  # workflow filestore
                    "8": None,  # server_uri (MCP only)
                    "9": None,  # credential_name (MCP only)
                }
            )
        elif value.tool_type == ToolType.MCP:
            # MCP-specific fields (8-9), nulls for Workflow fields (4-7)
            encoded.update(
                {
                    "4": None,  # input_spec (Workflow only)
                    "5": None,  # output_spec (Workflow only)
                    "6": -1,  # message_output_port_index
                    "7": None,  # workflow_filestore (Workflow only)
                    "8": value.server_uri,
                    "9": value.credential_name,  # credential reference
                }
            )
        else:
            raise TypeError(f"Unknown tool type: {value.tool_type}")

        return encoded
