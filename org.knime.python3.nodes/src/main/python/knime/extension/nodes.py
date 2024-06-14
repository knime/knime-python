# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------
#  Copyright by KNIME AG, Zurich, Switzerland
#  Website: http://www.knime.com; Email: contact@knime.com
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License, Version 3, as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, see <http://www.gnu.org/licenses>.
#
#  Additional permission under GNU GPL version 3 section 7:
#
#  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
#  Hence, KNIME and ECLIPSE are both independent programs and are not
#  derived from each other. Should, however, the interpretation of the
#  GNU GPL Version 3 ("License") under any applicable laws result in
#  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
#  you the additional permission to use and propagate KNIME together with
#  ECLIPSE with only the license terms in place for ECLIPSE applying to
#  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
#  license terms of ECLIPSE themselves allow for the respective use and
#  propagation of ECLIPSE together with KNIME.
#
#  Additional permission relating to nodes for KNIME that extend the Node
#  Extension (and in particular that are based on subclasses of NodeModel,
#  NodeDialog, and NodeView) and that only interoperate with KNIME through
#  standard APIs ("Nodes"):
#  Nodes are deemed to be separate and independent programs and to not be
#  covered works.  Notwithstanding anything to the contrary in the
#  License, the License does not apply to Nodes, you are not required to
#  license Nodes under the License, and you are granted a license to
#  prepare and propagate Nodes, in each case even if such Nodes are
#  propagated with or for interoperation with KNIME.  The owner of a Node
#  may freely choose the license terms applicable to such Node, including
#  when such Node is propagated with or for interoperation with KNIME.
# ------------------------------------------------------------------------

"""
Provides base implementations and utilities for the development of KNIME nodes in Python.

@author Adrian Nembach, KNIME GmbH, Konstanz, Germany
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Tuple, Type, Union
import os.path

import knime.extension.parameter as kp
import knime.api.table as kt
from knime.api.schema import PortObjectSpec, _ColumnarView


class PortObject(ABC):
    """
    Base class for custom port objects. They must have a corresponding
    `PortObjectSpec` and support serialization from and to bytes.
    """

    def __init__(self, spec: PortObjectSpec) -> None:
        self._spec = spec

    @property
    def spec(self) -> PortObjectSpec:
        """
        Provides access to the spec of the PortObject.
        """
        return self._spec

    @abstractmethod
    def serialize(self) -> bytes:
        """
        Serialize the object to bytes.
        """
        pass

    @classmethod
    @abstractmethod
    def deserialize(cls, spec: PortObjectSpec, storage: bytes) -> "PortObject":
        """
        Creates the port object from its spec and storage.
        """
        pass


class ConnectionPortObject(PortObject):
    """
    Connection port objects are a special type of port objects which
    support dealing with non-serializable objects such as database connections
    or web sessions.

    Connection port objects are passed downstream by ensuring that the same Python
    process is used to execute subsequent nodes. ConnectionPortObjects must provide the
    data in the `to_connection_data` and create new instances from the same data in
    `from_connection_data`. A reference to the data Python object is maintained and
    handed to downstream nodes. So the data does not need to be serializable/picklable.
    """

    def __init__(self, spec: PortObjectSpec) -> None:
        super().__init__(spec)

    def serialize(self):
        raise NotImplementedError("A connection port object cannot be serialized")

    @classmethod
    def deserialize(cls, spec, storage):
        raise NotImplementedError("A connection port object cannot be deserialized")

    @abstractmethod
    def to_connection_data(self) -> Any:
        """
        Provide the data that makes up this ConnectionPortObject such that it can be used
        by downstream nodes in the `from_connection_data` method.
        """
        pass

    @classmethod
    @abstractmethod
    def from_connection_data(
        cls, spec: PortObjectSpec, data: Any
    ) -> "ConnectionPortObject":
        """
        Construct a ConnectionPortObject from spec and data. The data is the data that has
        been returned by the `to_connection_data` method of the ConnectionPortObject
        by the upstream node.

        The data should not be tempered with, as it is a Python object that is handed to
        all nodes using this ConnectionPortObject.
        """
        pass


class FilestorePortObject(PortObject):
    """
    FilestorePortObjects are a special kind of PortObject whose serialization methods get
    direct access to files instead of bytes objects.

    This is experimental internal API that may change arbitrarily between releases.
    """

    def serialize(self) -> bytes:
        raise RuntimeError("Serialize should not be called for FilestorePortObjects.")

    @classmethod
    def deserialize(cls, spec: PortObjectSpec, storage: bytes) -> "PortObject":
        raise RuntimeError("Deserialize should not be called for FilestorePortObjects.")

    @abstractmethod
    def write_to(self, file_path: str) -> None:
        """
        Write the object into the given file_path.

        Parameters
        ----------
        file_path : str
            The path to the file where the object will be written.
        """

    @classmethod
    @abstractmethod
    def read_from(cls, spec: PortObjectSpec, file_path: str) -> "FilestorePortObject":
        """
        Read the object from the given file_path.
        """


def load_port_object(
    port_object_type: Type[PortObject], spec: PortObjectSpec, file_path: str
) -> PortObject:
    """
    Loads a port object from the given file.

    Experimental API for internal use only. May change in future releases without notice.
    """
    if issubclass(port_object_type, FilestorePortObject):
        return port_object_type.read_from(spec, file_path)
    else:
        with open(file_path, "rb") as f:
            return port_object_type.deserialize(spec, f.read())


def save_port_object(port_object: PortObject, file_path: str) -> None:
    """
    Saves a port object into a file.

    Experimental API for internal use only. May change in future releases without notice.
    """
    if issubclass(type(port_object), FilestorePortObject):
        port_object.write_to(file_path)
    else:
        with open(file_path, "wb") as f:
            f.write(port_object.serialize())


@dataclass
class PortType:
    id: str
    name: str
    object_class: type
    spec_class: type

    def is_super_type_of(self, port_type: "PortType") -> bool:
        """
        Returns True if this PortType is the same or a super-type of the provided PortType.
        One PortType is a supertype of another PortType if both its spec_class and object_class
        are a super-classes of the other PortTypes spec_class and object_class.

        Parameters
        ----------
        other : PortType
            The PortType to compare with.

        Returns
        -------
        bool
            True if this PortType is the same or a super-type of the provided PortType, False otherwise.
        """
        return issubclass(port_type.spec_class, self.spec_class) and issubclass(
            port_type.object_class, self.object_class
        )


# special PortTypes that are treated separately
PortType.BINARY = "PortType.BINARY"
PortType.CONNECTION = "PortType.CONNECTION"
PortType.TABLE = "PortType.TABLE"
PortType.IMAGE = "PortType.IMAGE"
PortType.CREDENTIAL = "PortType.CREDENTIAL"
PortType.WORKFLOW = "PortType.WORKFLOW"
PortType.HUB_AUTHENTICATION = "PortType.HUB_AUTHENTICATION"

input_port_type_to_java_name_map = {
    PortType.TABLE: "Table",
    PortType.BINARY: "Binary",
    PortType.IMAGE: "Image",
}


class ImageFormat(Enum):
    """
    The image formats available for image ports.
    """

    PNG = "png"
    """The PNG format."""
    SVG = "svg"
    """The SVG format."""

    @classmethod
    def available_options(cls):
        return ", ".join(option for option in cls)


class _KnimeNodeBackend(ABC):
    @abstractmethod
    def register_port_type(
        self, name: str, object_class: type, spec_class: type, id: Optional[str] = None
    ):
        raise RuntimeError("Not implemented")

    @abstractmethod
    def get_port_type_for_spec_type(self, spec_type: Type[PortObjectSpec]) -> PortType:
        """
        Returns the port type that corresponds to a particular type of PortObjectSpec.

        Returns
        -------
        PortType
            The port type that corresponds to the given PortObjectSpec.
        """

    @abstractmethod
    def get_port_type_for_id(self, id: str) -> PortType:
        """
        Returns the port type that corresponds to a particular id.

        Note
        ----
        This is an experimental API and might change without warning in future releases.

        Returns
        -------
        PortType
            The port type that corresponds to the given ID.
        """


_backend: _KnimeNodeBackend = None


def port_type(
    name: str,
    object_class: Type[PortObject],
    spec_class: Type[PortObjectSpec],
    id: Optional[str] = None,
) -> PortType:
    """
    Use this decorator to annotate a PortObject class that should correspond to a PortType in KNIME.

    Parameters
    ----------
    name : str
        A human readable name for the PortType
    spec_class : Type[PortObjectSpec]
        The class of PortObjectSpec used by the PortType
    id : str, optional
        A unique id for the PortType. If None is provided an id is generated by concatenating the module and class name separated by a '.'.
    """
    return _backend.register_port_type(name, object_class, spec_class, id)


def get_port_type_for_spec_type(spec_type: Type[PortObjectSpec]) -> PortType:
    """
    Returns the port type that corresponds to a particular type of PortObjectSpec.

    Experimental API: Might change without warning in future releases.
    """
    return _backend.get_port_type_for_spec_type(spec_type)


def get_port_type_for_id(id: str) -> PortType:
    """
    Returns the port type that corresponds to a particular id.

    Experimental API. Might change without warning in future releases.
    """
    return _backend.get_port_type_for_id(id)


@dataclass
class Port:
    """
    A class representing a port.

    Parameters
    ----------
    type : PortType
        The type of the port.
    name : str
        The name of the port.
    description : str
        The description of the port.
    id : Optional[str], default=None
        The unique identifier for the port. Required for ports of type BINARY or CONNECTION.

    Raises
    ------
    TypeError
        If the port type is BINARY or CONNECTION and the 'id' is not provided.

    """

    type: PortType
    name: str
    description: str
    id: Optional[str] = (
        None  # can be used by BINARY and CONNECTION ports to only allow linking ports with matching IDs
    )

    def __post_init__(self):
        """
        Perform validation after ``__init__``
        """
        if self.type in [PortType.BINARY, PortType.CONNECTION] and self.id is None:
            raise TypeError(
                f"{type(self)}s of type BINARY or CONNECTION must have a unique 'id' set"
            )


@dataclass
class PortGroup:
    """
    A class representing a port group.

    Parameters
    ----------
    type : PortType
        The type of the port group.
    name : str
        The name of the port group.
    description : str
        The description of the port group.
    """

    type: PortType
    name: str
    description: str
    id: Optional[str] = (
        None  # can be used by BINARY and CONNECTION ports to only allow linking ports with matching IDs
    )

    def __post_init__(self):
        """
        Perform validation after ``__init__``
        """
        if not isinstance(self.name, str):
            raise TypeError(f"name must be of type str. Got {type(self.name)}.")

        if not isinstance(self.description, str):
            raise TypeError(
                f"description must be of type str. Got {type(self.description)}."
            )


@dataclass
class ViewDeclaration:
    """
    A data class representing a view declaration.

    Parameters
    ----------
    name : str
        The name of the view.
    description : str
        The description of the view.
    static_resources : str, optional
        The static resources associated with the view.

    Attributes
    ----------
    name : str
        The name of the view.
    description : str
        The description of the view.
    static_resources : str or None
        The static resources associated with the view, or None if not specified.
    """

    name: str
    description: str
    static_resources: Optional[str] = None


@dataclass(frozen=True)
class Credential:
    """
    A class representing a credential.

    Parameters
    ----------
    username : str
        The username for the credential.
    password : str
        The password for the credential.
    credential_name : str
        The name of the credential.
    """

    username: str
    password: str
    credential_name: str


# re-exporting symbols so that "import knime_node" will include the most needed features
IntParameter = kp.IntParameter
DoubleParameter = kp.DoubleParameter
BoolParameter = kp.BoolParameter
StringParameter = kp.StringParameter
MultilineStringParameter = kp.MultilineStringParameter
parameter_group = kp.parameter_group
ColumnParameter = kp.ColumnParameter
MultiColumnParameter = kp.MultiColumnParameter
ColumnFilterParameter = kp.ColumnFilterParameter
Table = kt.Table
BatchOutputTable = kt.BatchOutputTable


class _BaseContext:
    def __init__(self, java_ctx, flow_variables) -> None:
        self._flow_variables = flow_variables
        self._java_ctx = java_ctx

    @property
    def flow_variables(self) -> Dict[str, Any]:
        """
        The flow variables coming in from KNIME as a dictionary with string keys.

        Notes
        -----
        The dictionary can be edited and supports flow variables of the following types:

        - bool
        - list[bool]
        - float
        - list[float]
        - int
        - list[int]
        - str
        - list[str]
        """
        return self._flow_variables

    def get_credential_names(self):
        """
        Returns the identifier (flow variable name) for each credential.

        Returns
        -------
        list
            A list of credential names.
        """
        credential_names = list(self._java_ctx.get_credential_names())
        return credential_names

    def get_credentials(self, identifier: str) -> Credential:
        """
        Returns the credentials dataclass for the given identifier.

        Parameters
        ----------
            identifier : str
                The identifier of the credentials to retrieve.

        Returns
        -------
        Credential
            A dataclass containing the credentials.
        """
        from py4j.protocol import Py4JJavaError

        try:
            credentials = list(self._java_ctx.get_credentials(identifier))
        except Py4JJavaError:
            raise KeyError(f"Error retrieving credentials for identifier: {identifier}")
        # create dataclass from credential list
        return Credential(credentials[0], credentials[1], credentials[2])


class DialogCreationContext(_BaseContext):
    """
    The DialogCreationContext provides utilities to communicate with KNIME during the dialog creation phase.
    It enables access to the flow variables, the specs of the input tables and the credentials. These can be used to
    create the dialog elements, by passing the respective method as lambda function to the constructor of the
    string parameter class. The lambdas will receive the dialog creation context as parameter which should be passed
    as first parameter to the fully qualified method calls of `DialogCreationContext` as below:


    Examples
    -------
    >>> class ExampleNode:
    ...     # This dialog element displays a dropdown with all available credentials
    ...     string_param = knext.StringParameter(label="Credential parameter", description="Choices is a callable",
    ...                                 choices=lambda a: knext.DialogCreationContext.get_credential_names(a))

    """

    def __init__(self, java_ctx, flow_variables, specs_to_python_converter) -> None:
        """
        Initialize the object.

        Parameters
        ----------
        java_ctx : JavaContext
            The JavaContext object.
        flow_variables : list
            The list of flow variables.
        specs_to_python_converter : function
            The function to convert Java specifications to Python.

        """
        super().__init__(java_ctx, flow_variables)
        specs = self._java_ctx.get_input_specs()
        portmap = self._java_ctx.get_input_port_map()
        # cast portmap to dict
        portmap = {k: list(v) for k, v in portmap.items()}
        self._python_specs = specs_to_python_converter(specs, portmap)

    def get_input_specs(self) -> List[PortObjectSpec]:
        """
        Returns the specs for all input ports of the node.

        Returns
        -------
        List
            A list of specs for all input ports.
        """
        return self._python_specs

    def get_flow_variables(self):
        """
        Returns the flow variables coming in from KNIME as a dictionary with string keys.
        The dictionary cannot be edited.

        Returns
        -------
        dict
            The flow variables dictionary with string keys.

        Notes
        -----
        The supported flow variable types are:
        - bool
        - list(bool)
        - float
        - list(float)
        - int
        - list(int)
        - str
        - list(str)
        """
        return self._flow_variables


class ConfigurationContext(_BaseContext):
    """
    The ConfigurationContext provides utilities to communicate with KNIME
    during a node's configure() method.
    """

    def __init__(
        self, java_ctx, flow_variables, input_ports=None, output_ports=None
    ) -> None:
        super().__init__(java_ctx, flow_variables)
        self._input_ports = input_ports
        self._output_ports = output_ports

    def get_connected_input_port_numbers(self) -> List[int]:
        """Gets the number of connected input ports for each port.

        This method can be used to know how many input ports are connected to the node for each port.

        Returns
        -------
        list of int
            A list of the number of connected input ports for each port type.
        """
        assert self._input_ports is not None
        port_map = dict(self._java_ctx.get_input_port_map())
        port_list = [
            len(port_map.get(port.name, [])) if isinstance(port, PortGroup) else 1
            for port in self._input_ports
        ]
        return port_list

    def get_connected_output_port_numbers(self) -> List[int]:
        """Gets the number of connected output ports for each port.

        This method can be used to know how many output ports are connected to the node for each port. This
        is relevant when using PortGroups to determine which ports have to be populated with data.

        Examples
        --------
        >>> @node(name="Example Node with Group Ports")
        ... @input_table(name="Input Data", description="The data to process in my node")
        ... @output_table_group(name="Output Data", description="Multiple outputs from my node")
        ... class ExampleNode(PythonNode):
        ...     # ...
        ...     def execute(self, exec_context, table)->List[knext.Schema]:
        ...         output_table = table
        ...         connected_output_ports  = exec_context.get_connected_output_port_numbers()
        ...         # When 2 output ports are connected, this will return [2].
        ...         output_tables = [output_table] * connected_output_ports[0]
        ...         return output_tables # Thus, one list with two tables has to be returned.


        Returns
        -------
        list of int
            A list of the number of connected output ports for each port type.
        """
        assert self._output_ports is not None
        port_map = dict(self._java_ctx.get_output_port_map())
        port_list = [
            len(port_map.get(port.name, [])) if isinstance(port, PortGroup) else 1
            for port in self._output_ports
        ]

        return port_list

    def set_warning(self, message: str) -> None:
        """
        Sets a warning on the node.

        Parameters
        ----------
        message : str
            The warning message to display on the node.
        """
        self._java_ctx.set_warning(str(message))


class ExecutionContext(ConfigurationContext):
    """
    The `ExecutionContext` provides utilities to communicate with KNIME during a
    node's `execute()` method.
    """

    def set_progress(self, progress: float, message: str = None):
        """
        Set the progress of the execution.

        Note that the progress that can be set here is 80% of the total progress
        of a node execution. The first and last 10% are reserved for data
        transfer and will be set by the framework.

        Parameters
        ----------
        progress : float
            A floating point number between 0.0 and 1.0.
        message : str, optional
            An optional message to display in KNIME with the progress.
        """
        if isinstance(progress, int):
            progress = float(progress)
        if not isinstance(progress, float):
            raise TypeError(f"progress must be of type float. Got {type(progress)}.")
        if progress < -0.0001 or progress > 1.0001:
            # NOTE: We are less strict as we say we are because of possible floating point imprecision
            raise ValueError("progress must be between 0.0 and 1.0.")
        progress = min(1.0, max(0.0, progress))

        if message is None:
            self._java_ctx.set_progress(progress)
        else:
            if not isinstance(message, str):
                raise TypeError("message must be a str or None.")
            self._java_ctx.set_progress(progress, message)

    def is_canceled(self) -> bool:
        """
        Returns true if this node's execution has been canceled from KNIME.
        Nodes can check for this property and return early if the execution does
        not need to finish. Raising a RuntimeError in that case is encouraged.
        """
        return self._java_ctx.is_canceled()

    def get_workflow_temp_dir(self) -> str:
        """
        Returns the local absolute path where temporary files for this workflow
        should be stored. Files created in this folder are not automatically deleted
        by KNIME.

        By default, this folder is located in the operating system's
        temporary folder. In that case, the contents will be cleaned by the OS.
        """
        return self._java_ctx.get_workflow_temp_dir()

    def get_workflow_data_area_dir(self) -> str:
        """
        Returns the local absolute path to the current workflow's data area folder.
        This folder is meant to be part of the workflow, so its contents are included
        whenever the workflow is shared.
        """
        return os.path.join(self._java_ctx.get_workflow_dir(), "data")

    def get_knime_home_dir(self) -> str:
        """
        Returns the local absolute path to the directory in which KNIME stores its
        configuration as well as log files.

        Returns
        -------
        str
            The local absolute path to the KNIME directory.
        """
        return self._java_ctx.get_knime_home_dir()


class PythonNode(ABC):
    """
    Extend this class to provide a pure Python based node extension to KNIME Analytics Platform.

    Users can either use the decorators ``@knext.input_table``, ``@knext.input_binary``,
    ``@knext.output_table``, ``@knext.output_binary``, and ``@knext.output_view``, or
    populate the ``input_ports``, ``output_ports``, and ``output_view`` attributes.

    Use the Python logging facilities and its ``.warning`` and ``.error`` methods to
    write warnings and errors to the KNIME console. ``.info`` and ``.debug`` will only
    show up in the KNIME console if the log level in KNIME is configured to show these.

    Examples
    --------

    >>> import logging
    ... import knime.extension as knext
    ...
    ... LOGGER = logging.getLogger(__name__)
    ...
    ... category = knext.category("/community", "mycategory", "My Category", "My category described", icon="icons/category.png")
    ...
    ... @knext.node(name="Pure Python Node", node_type=knext.NodeType.LEARNER, icon_path="icons/icon.png", category=category)
    ... @knext.input_table(name="Input Data", description="We read data from here")
    ... @knext.output_table(name="Output Data", description="Whatever the node has produced")
    ... class TemplateNode(knext.PythonNode):
    ...     # A Python node has a description.
    ...
    ...     def configure(self, configure_context, table_schema):
    ...         LOGGER.info(f"Configuring node")
    ...         return table_schema
    ...
    ...     def execute(self, exec_context, table):
    ...         return table

    """

    input_ports: List[Port] = None
    output_ports: List[Port] = None
    output_view: ViewDeclaration = None

    @abstractmethod
    def configure(self, config_context: ConfigurationContext, *inputs):
        """
        Configure this Python node.

        Parameters
        ----------
        config_context : ConfigurationContext
            The ConfigurationContext providing KNIME utilities during execution
        *inputs :
            Each input table spec or binary port spec will be added as parameter,
            in the same order that the ports were defined.

        Returns
        -------
        Union[Spec, List[Spec], Tuple[Spec, ...], Column]
            Either a single spec, or a tuple or list of specs. The number of specs
            must match the number of defined output ports, and they must be returned in this order.
            Alternatively, instead of a spec, a knext.Column can be returned (if the spec shall
            only consist of one column).

        Raises
        ------
        InvalidParametersError
            If the current input parameters do not satisfy this node's requirements.
        """
        pass

    @abstractmethod
    def execute(self, exec_context: ExecutionContext, *inputs):
        """
        Execute this Python node.

        Parameters
        ----------
        exec_context : ExecutionContext
            The ExecutionContext providing KNIME utilities during execution.
        *inputs : tuple
            Each input table or binary port object will be added as a parameter, in the same order that the ports were defined.
            Tables will be provided as a `kn.Table`, while binary data will be a plain Python `bytes` object.

        Returns
        -------
        object or tuple/list of objects
            Either a single output object (table or binary), or a tuple or list of objects.
            The number of output objects must match the number of defined output ports,
            and they must be returned in this order.
            Tables must be provided as a `kn.Table` or `kn.BatchOutputTable`, while binary data
            should be returned as plain Python `bytes` object.
        """
        pass


class _Category:
    def __init__(
        self,
        path: str,
        level_id: str,
        name: str,
        description: str,
        icon: str,
        after: str = "",
        locked: bool = True,
    ) -> None:
        self._path = path
        self._level_id = level_id
        self._name = name
        self._description = description
        self._icon = icon
        self._after = after
        self._locked = locked

    def to_dict(self):
        return {
            "path": self._path,
            "level_id": self._level_id,
            "name": self._name,
            "description": self._description,
            "icon": self._icon,
            "after": self._after,
            "locked": self._locked,
        }


_categories = []


def category(
    path: str,
    level_id: str,
    name: str,
    description: str,
    icon: str,
    after: str = "",
    locked: bool = True,
):
    """Register a new node category.

    A node category must be created only once. Use the string that represents the
    absolute category path to add nodes to an existing category.

    Examples
    --------
    >>> node_category = knext.category(
    ...     "/testing", "python-nodes", "Python Nodes", "Python testing nodes", "icon.png"
    ... )
    ...
    ... @knext.node(
    ...     name="Simple Python Node",
    ...     node_type=knext.NodeType.MANIPULATOR,
    ...     icon_path="icon.png",
    ...     category=node_category
    ... )
    ... class SimpleNode(knext.PythonNode):
    ...     pass

    Parameters
    ----------
    path : str
        The absolute path that leads to this category, e.g., "/io/read". The segments are the category level-IDs, separated by a slash ("/"). Categories that contain community nodes should be placed in the "/community" category.
    level_id : str
        The identifier of the level which is used as a path-segment and must be unique at the level specified by "path".
    name : str
        The name of this category, e.g., "File readers".
    description : str
        A short description of the category.
    icon : str
        File path to a 16x16 pixel PNG icon for this category. The path must be relative to the root of the extension.
    after : str, optional
        Specifies the level-id of the category after which this category should be sorted. Defaults to "".
    locked : bool, optional
        Set this to False to allow extensions from other vendors to add sub-categories or nodes to this category. Defaults to True.

    Returns
    -------
    str
        The full path of the category, which can be used to create nodes inside this category.
    """

    _categories.append(
        _Category(path, level_id, name, description, icon, after, locked)
    )
    if path.endswith("/"):
        return path + level_id
    return f"{path}/{level_id}"


def _port_specifier_list_from_port_list(
    port_list: List[Union["Port", "PortGroup"]],
) -> List["_PortSpecifier"]:
    """Convert a list of Port objects to a list of PortSpecifier objects.

    Also increments the description index for each Port object in the list.

    Args:
        port_list:

    Returns:

    """
    description_index = 0
    port_specifier_list = []

    for port in port_list:
        port_specifier_list.append(
            asdict(_PortSpecifier.from_port(port, description_index))
        )
        # we only increment the description index if the port is a Port object, as PortGroups
        # are inserted in between two Ports.
        if isinstance(port, Port):
            description_index += 1
    return port_specifier_list


class _Node:
    """
    Class representing an actual node in KNIME AP.
    """

    node_factory: Callable
    id: str
    name: str
    is_deprecated: bool
    is_hidden: bool
    node_type: str
    icon_path: str
    category: str
    after: str
    keywords: Optional[List[str]]
    input_ports: List[Port]
    output_ports: List[Port]
    views: List[
        ViewDeclaration
    ]  # for the moment we only allow one view, but we use a list to potentially allow multiple

    def __init__(
        self,
        node_factory,
        id: str,
        name: str,
        is_deprecated: bool,
        is_hidden: bool,
        node_type: str,
        icon_path: str,
        category: str,
        after: str,
        keywords: Optional[List[str]],
    ) -> None:
        self.id = id
        self.name = name
        self.is_deprecated = is_deprecated
        self.is_hidden = is_hidden
        self.node_type = node_type
        self.icon_path = icon_path
        self.category = category
        self.after = after
        self.keywords = keywords
        self.input_ports = _get_ports(node_factory, "input_ports")
        self.output_ports = _get_ports(node_factory, "output_ports")
        self.views = [_get_view(node_factory)]

        def port_injector(*args, **kwargs):
            """
            This method is called whenever a node is instanciated through the node_factory
            which was modified by the @kn.node decorator.

            We inject the found ports/views into the instance after creation so that they are available
            to the users.
            """
            node = node_factory(*args, **kwargs)
            self.assert_no_composed_parameters(node)
            node.input_ports = self.input_ports
            node.output_ports = self.output_ports
            node.output_view = self.views[0]
            node.execute = _unwrap_results(node.execute)
            node.configure = _unwrap_results(node.configure)

            return node

        self.node_factory = port_injector

    def assert_no_composed_parameters(self, node_instance):
        """
        Node-level parameter composition is not supported, hence we check that none of the
        instance attributes are parameter objects. We don't check for the `._name` attribute
        since an instance-level parameter is by definition not a descriptor.
        """
        if any(
            [
                isinstance(attr, kp._BaseParameter)
                for attr in node_instance.__dict__.values()
            ]
        ):
            raise AttributeError(
                """Only parameter group composition is allowed at the node-level.
                Individual parameters need to be moved out of the node's `__init__` method and be either defined at the class-level or encapsulated in a parameter group."""
            )

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "node_type": self.node_type,
            "is_deprecated": self.is_deprecated,
            "is_hidden": self.is_hidden,
            "icon_path": self.icon_path,
            "category": self.category,
            "after": self.after,
            "keywords": self.keywords if self.keywords is not None else [],
            "input_port_specifier": _port_specifier_list_from_port_list(
                self.input_ports
            ),
            "output_port_specifier": _port_specifier_list_from_port_list(
                self.output_ports
            ),
            "views": [asdict(v) for v in self.views if v is not None],
        }


_nodes = {}


class NodeType(Enum):
    """
    Defines the different node types that are available for Python based nodes.
    """

    SOURCE = "Source"
    """A node producing data."""
    SINK = "Sink"
    """A node consuming data."""
    LEARNER = "Learner"
    """A node learning a model that is typically consumed by a PREDICTOR."""
    PREDICTOR = "Predictor"
    """A node that predicts something typically using a model provided by a LEARNER."""
    MANIPULATOR = "Manipulator"
    """A node that manipulates data."""
    VISUALIZER = "Visualizer"
    """A node that visualizes data."""
    OTHER = "Other"
    """A node that doesn't fit one of the other node types."""


# TODO allow to pass in other nodes as after?
def node(
    name: str,
    node_type: NodeType,
    icon_path: str,
    category: str,
    after: str = None,
    keywords: Optional[List[str]] = None,
    id: str = None,
    is_deprecated: bool = False,
    is_hidden: bool = False,
) -> Callable:
    """
    Use this decorator to annotate a PythonNode class or function that creates a PythonNode
    instance that should correspond to a node in KNIME.

    Parameters
    ----------
    name: str
        The name of the node
    node_type: NodeType
        Type can be Source, Sink, Learner, Predictor, Manipulator, Visualizer or Other.
    icon_path: str
        String to icon if no path is given it has no icon.
    category: str
        Category to which the node will belongs to. The node will appear under this category. E.g. `community`.
    after: Optional[str]
        If given the node will be listed after the specified node.
    keywords: Optional[List[str]]
        Keywords describing your node which will help finding the node during search.
    id: str
        Id of your node.
    is_deprecated: bool
        Default is false.
    is_hidden: bool
        Default is false. If true your node will not shown during search and not be listed in it's category.

    Returns
    -------
    Callable
        Returns a function which will decorate your node implementation with the set parameters.
    """

    def register(node_factory):
        node_id = id if id is not None else node_factory.__name__

        if isinstance(node_type, NodeType):
            nt = node_type.value
        else:
            nt = "Unknown"

        n = _Node(
            node_factory=node_factory,
            id=node_id,
            name=name,
            node_type=nt,
            is_deprecated=is_deprecated,
            is_hidden=is_hidden,
            icon_path=icon_path,
            category=category,
            after=after,
            keywords=keywords,
        )
        _nodes[node_id] = n

        # Return n.node_factory here because this is a modified variant where we're injecting ports
        return n.node_factory

    return register


class InvalidParametersError(Exception):
    """
    Error that should be raised if the values of the configured parameters are invalid.
    """

    def __init__(self, message) -> None:
        super().__init__(message)


def _unwrap_results(func: Callable) -> Callable:
    """
    Configure and view can both return _ColumnarView or _TabularView respectively, which have a virtual
    stack of operations that haven't been executed yet. But when we use the results, we always want the
    executed version, so we're calling .get() on each result of func() whenever needed.
    """

    def wrapper(*args, **kwargs):
        results = func(*args, **kwargs)

        if results is None:
            return None
        elif isinstance(results, list) or isinstance(results, tuple):
            unwrapped_results = []
            for r in results:
                if isinstance(r, _ColumnarView):
                    unwrapped_results.append(r.get())
                else:
                    unwrapped_results.append(r)
            return unwrapped_results
        elif isinstance(results, _ColumnarView):
            return results.get()
        else:
            return results

    return wrapper


def _add_port(node_factory, port_slot: str, port: Union[Port, PortGroup]):
    """
    Add a port to a node factory object.

    Parameters
    ----------
    node_factory : object
        The node factory object to add the port to.
    port_slot : str
        The name of the attribute in the node factory object to add the port to.
    port : Union[Port, PortGroup]
        The port to be added to the node factory.

    Returns
    -------
    object
        The updated node factory object.

    Raises
    ------
    ValueError
        If the attribute specified by `port_slot` already exists in the node factory object and is not decorated.
    """
    if not hasattr(node_factory, port_slot) or getattr(node_factory, port_slot) is None:
        setattr(node_factory, port_slot, [])
        # We insert a tiny marker to know whether the port slot was created by us,
        # or whether it already existed in the class because we do NOT want to
        # insert ports into an attrib usin the decorator if it was previously
        # filled by the user. The reason is that we wouldn't know the Port order
        # and whether overriding or appending was desired.
        setattr(node_factory, "__knime_added_" + port_slot, True)
    else:
        if not hasattr(node_factory, "__knime_added_" + port_slot):
            raise ValueError(
                f"Cannot use '{port_slot}' decorator on object which has an attribute '{port_slot}' already"
            )

    port_list = getattr(node_factory, port_slot)
    if isinstance(port, PortGroup) and any(
        isinstance(existing_port, PortGroup) and existing_port.name == port.name
        for existing_port in port_list
    ):
        raise ValueError(
            f"A PortGroup named '{port.name}' already exists in the '{port_slot}' list. "
            f"For Input and Output PortGroups, the name of a PortGroup must be unique. "
            f"For example, if you have an Input PortGroup named 'data', you cannot have another Input PortGroup named 'data'. "
            f"But you can have an Output PortGroup named 'data'."
        )

    port_list.insert(0, port)

    return node_factory


def _get_attr_from_instance_or_factory(node_factory, attr) -> List[Port]:
    # first try an instance of the node whether it has the respective port set
    if hasattr((n := node_factory()), attr) and (ps := getattr(n, attr)) is not None:
        return ps

    # then look at the node factory
    if hasattr(node_factory, attr) and (ps := getattr(node_factory, attr)) is not None:
        return ps

    return None


def _get_ports(node_factory, port_slot) -> List[Port]:
    ps = _get_attr_from_instance_or_factory(node_factory, port_slot)

    if ps is None:
        return []
    else:
        return ps


def _get_view(node_factory) -> Optional[ViewDeclaration]:
    return _get_attr_from_instance_or_factory(node_factory, "output_view")


def input_binary(name: str, description: str, id: str):
    """
    Use this decorator to define a bytes-serialized port object input of a node.

    Parameters
    ----------
    name: str
        The name of the input port.
    description: str
        A description of the input port.
    id: str
        A unique ID identifying the type of the Port. Only Ports with equal ID can be connected in KNIME.
    """
    return lambda node_factory: _add_port(
        node_factory, "input_ports", Port(PortType.BINARY, name, description, id)
    )


def input_table(name: str, description: str):
    """
    Use this decorator to define an input port of type "Table" of a node.

    Parameters
    ----------
        name : str
            The name of the input port.
        description : str
            A description of the input port.
    """
    return lambda node_factory: _add_port(
        node_factory, "input_ports", Port(PortType.TABLE, name, description)
    )


def input_port(name: str, description: str, port_type: PortType):
    """
    Use this decorator to add an input port of the provided type to a node.

    Parameters
    ----------
        name : str
            The name of the input port.
        description : str
            A description of the input port.
        port_type : PortType
            The type of the input port.
    """
    return lambda node_factory: _add_port(
        node_factory, "input_ports", Port(port_type, name, description)
    )


def output_binary(name: str, description: str, id: str):
    """
    Use this decorator to define a bytes-serialized port object output of a node.

    Parameters
    ----------
    name : str
        The name of the port.
    description : str
        The description of the port.
    id : str
        A unique ID identifying the type of the Port. Only Ports with equal ID can be connected in KNIME.
    """
    return lambda node_factory: _add_port(
        node_factory, "output_ports", Port(PortType.BINARY, name, description, id)
    )


def output_table(name: str, description: str):
    """
    Use this decorator to define an output port of type "Table" of a node.

    Parameters
    ----------
    name : str
        The name of the port.
    description : str
        Description of what the port is used for.
    """
    return lambda node_factory: _add_port(
        node_factory, "output_ports", Port(PortType.TABLE, name, description)
    )


def output_port(name: str, description: str, port_type: PortType):
    """
    Use this decorator to add an output port of the provided type to a node.

    Parameters
    ----------
    name : str
        The name of the port.
    description : str
        Description of what the port is used for.
    port_type : type
        The type of the port to add.
    """
    return lambda node_factory: _add_port(
        node_factory, "output_ports", Port(port_type, name, description)
    )


def output_view(name: str, description: str, static_resources: Optional[str] = None):
    """
    Use this decorator to specify that this node produces a view.

    Parameters
    ----------
    name : str
        The name of the view.
    description : str
        Description of the view.
    static_resources : str
        The path to a folder of resources that will be available to the HTML page. The
        path given here must be relative to the root of the extension. The resources
        can be accessed by the same relative file path (e.g. "{static_resources}/{filename}").
    """

    def add_view(node_factory):
        setattr(
            node_factory,
            "output_view",
            ViewDeclaration(name, description, static_resources),
        )
        return node_factory

    return add_view


def output_image(name: str, description: str):
    """
    Use this decorator to define an output port of the type "Image" of a node.

    The `configure` method must return specs of the type `ImagePortObjectSpec`. The
    `execute` method must return a `bytes` object containing the image data. Note
    that the image data must be valid for the format defined in `configure`.

    Parameters
    ----------
        name : str
            The name of the image output port
        description : str
            Description of the image output port

    Examples
    --------

    >>> @knext.node(...)
    ... @knext.output_image(
    ...     name="PNG Output Image",
    ...     description="An example PNG output image")
    ... @knext.output_image(
    ...     name="SVG Output Image",
    ...     description="An example SVG output image")
    ... class ImageNode:
    ...     def configure(self, config_context):
    ...         return (
    ...             knext.ImagePortObjectSpec(knext.ImageFormat.PNG),
    ...             knext.ImagePortObjectSpec(knext.ImageFormat.SVG),
    ...         )
    ...
    ...     def execute(self, exec_context):
    ...         # create a plot ...
    ...         buffer_png = io.BytesIO()
    ...         plt.savefig(buffer_png, format="png")
    ...
    ...         buffer_svg = io.BytesIO()
    ...         plt.savefig(buffer_svg, format="svg")
    ...
    ...         return (
    ...             buffer_png.getvalue(),
    ...             buffer_svg.getvalue(),
    ...         )
    """
    return lambda node_factory: _add_port(
        node_factory,
        "output_ports",
        Port(PortType.IMAGE, name, description),
    )


def input_port_group(name: str, description: str, port_type: PortType):
    """
    Use this decorator to add an input port group of the provided type to a node.

    Parameters
    ----------
        name : str
            The name of the input port. Must be unique for all input port groups.
        description : str
            A description of the input port.
        port_type : PortType
            The type of the input port.
    """
    return lambda node_factory: _add_port(
        node_factory, "input_ports", PortGroup(port_type, name, description)
    )


def input_table_group(name: str, description: str):
    """
    Use this decorator to define an input port group of type "Table" of a node.

    Parameters
    ----------
    name : str
        The name of the input port group. Must be unique for all input port groups.
    description : str
        A description of the input port group.

    Examples
    --------
    >>> @knext.node(
    ...     name="Example Node with Multiple Input Table Groups",
    ...     node_type=knext.NodeType.MANIPULATOR,
    ...     icon_path="icon.png",
    ...     category="community/example"
    ... )
    ... @knext.input_table_group(
    ...     name="First Input Tables",
    ...     description="The first group of input tables for processing"
    ... )
    ... @knext.input_table_group(
    ...     name="Second Input Tables",
    ...     description="The second group of input tables for processing"
    ... )
    ... @knext.output_table_group(
    ...     name="Output Tables",
    ...     description="Combined results of the input tables"
    ... )
    ... class ConcatenateNode(knext.PythonNode):
    ...     def configure(self, config_context, first_table_specs: List[knext.Schema], second_table_specs: List[knext.Schema]) -> Iterable[List[knext.Schema]]:
    ...         return first_table_specs + second_table_specs
    ...
    ...     def execute(self, exec_context, first_tables: List[knext.Table], second_tables: List[knext.Table]) -> Iterable[List[knext.Table]]:
    ...         return first_tables + second_tables
    """
    return lambda node_factory: _add_port(
        node_factory,
        "input_ports",
        PortGroup(PortType.TABLE, name, description),
    )


def input_binary_group(name: str, description: str, id: Optional[str] = None):
    """
    Use this decorator to define an input port group of type "Binary" for a node.

    Parameters
    ----------
    name : str
        The name of the input port group. Must be unique for all input port groups.
    description : str
        A description of the input port group.
    id : Optional[str]
        A unique ID identifying the type of the PortGroup. Only PortGroups with equal ID can be connected in KNIME.
    """
    return lambda node_factory: _add_port(
        node_factory,
        "input_ports",
        PortGroup(PortType.BINARY, name, description, id=id),
    )


def output_table_group(name: str, description: str):
    """
    Use this decorator to define an output port group of type "Table" of a node.

    Parameters
    ----------
        name : str
            The name of the output port. Must be unique for all output port groups.
        description : str
            A description of the output port.
    """
    return lambda node_factory: _add_port(
        node_factory,
        "output_ports",
        PortGroup(PortType.TABLE, name, description),
    )


def output_binary_group(name: str, description: str, id: Optional[str] = None):
    """
    Use this decorator to define an output port group of type "Binary" for a node.

    Parameters
    ----------
    name : str
        The name of the output port group. Must be unique for all output port groups.
    description : str
        A description of the output port group.
    id : Optional[str]
        A unique ID identifying the type of the PortGroup. Only PortGroups with equal ID can be connected in KNIME.
    """
    return lambda node_factory: _add_port(
        node_factory,
        "output_ports",
        PortGroup(PortType.BINARY, name, description, id=id),
    )


def output_image_group(name: str, description: str):
    """
    Use this decorator to define an output port group of the type "Image" of a node.

    The `configure` method must return specs of the type `ImagePortObjectSpec`. The
    `execute` method must return a `bytes` object containing the image data. Note
    that the image data must be valid for the format defined in `configure`.

    Parameters
    ----------
        name : str
            The name of the image output port. Must be unique for all output port groups.
        description : str
            Description of the image output port

    Examples
    --------

    >>> @knext.node(...)
    ... @knext.output_image_group(
    ...     name="PNG Output Image Group",
    ...     description="An example PNG output image")
    ... @knext.output_image_group(
    ...     name="SVG Output Image Group",
    ...     description="An example SVG output image")
    ... class ImageNode:
    ...     def configure(self, config_context):
    ...         # if 2 ports are connected per group, we will have 2 PNG and 2 SVG outputs
    ...         port_numbers = config_context.get_connected_output_port_numbers()
    ...         return (
    ...             [knext.ImagePortObjectSpec(knext.ImageFormat.PNG]*port_numbers[0],
    ...             [knext.ImagePortObjectSpec(knext.ImageFormat.SVG)]*port_numbers[1],
    ...         )
    ...
    ...     def execute(self, exec_context):
    ...         # create a plot ...
    ...         buffer_png = io.BytesIO()
    ...         plt.savefig(buffer_png, format="png")
    ...
    ...         buffer_svg = io.BytesIO()
    ...         plt.savefig(buffer_svg, format="svg")
    ...
    ...         return (
    ...             [buffer_png.getvalue()]*2,
    ...             [buffer_svg.getvalue()]*2,
    ...         )
    """
    return lambda node_factory: _add_port(
        node_factory,
        "output_ports",
        PortGroup(PortType.IMAGE, name, description),
    )


def output_port_group(name: str, description: str, port_type: PortType):
    """
    Use this decorator to add an output port group of the provided type to a node.

    Parameters
    ----------
    name : str
        The name of the port group. Must be unique for all output port groups.
    description : str
        Description of what the port is used for.
    port_type : type
        The type of the port to add.
    """
    return lambda node_factory: _add_port(
        node_factory, "output_ports", PortGroup(port_type, name, description)
    )


@dataclass
class _PortSpecifier:
    """
    A class representing a port specifier.

    Used for the Java python communication

    Parameters
    ----------
    name : str
        The name of the port specifier.
    type_string : str
        The type string of the port specifier.
    description : str
        The description of the port specifier.
    description_index: int
        The description index of the port specifier. Where to insert in the description.
    group : bool = False
        Whether the port is a group or not.
    """

    name: str
    type_string: str
    description: str
    description_index: int
    group: bool = False

    @classmethod
    def from_port(cls, port: Port, description_index: int) -> "_PortSpecifier":
        return cls(
            name=port.name,
            type_string=_port_to_str(port),
            description=port.description,
            description_index=description_index,
            group=isinstance(port, PortGroup),
        )

    def __post_init__(self):
        """
        Perform validation after ``__init__``
        """
        if not isinstance(self.name, str):
            raise TypeError(f"name must be of type str. Got {type(self.name)}.")

        if not isinstance(self.description, str):
            raise TypeError(
                f"description must be of type str. Got {type(self.description)}."
            )
        if not isinstance(self.group, bool):
            raise TypeError(f"group must be of type bool. Got {type(self.group)}.")


def _port_to_str(port):
    if port.type == PortType.BINARY:
        return f"{port.type}"
    elif hasattr(port.type, "object_class") and issubclass(
        port.type.object_class, ConnectionPortObject
    ):
        return "Connection" + str(port.type)
    else:
        return str(port.type)
