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
from abc import ABC, abstractmethod, abstractproperty
from dataclasses import dataclass, asdict
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Callable
import knime_parameter as kp
import knime_node_table as kt

# TODO currently not part of our dependencies but maybe worth adding instead of reimplementing here
from packaging.version import Version


class PortType(Enum):
    # TODO: make this extensible
    BINARY = (
        auto()
    )  # could have an ID -> PortObjectSpec, could be checked in configure?
    TABLE = auto()


@dataclass
class Port:
    type: PortType
    name: str
    description: str
    id: Optional[
        str
    ] = None  # can be used by BINARY ports to only allow linking ports with matching IDs

    def __post_init__(self):
        """
        Perform validation after __init__
        """
        if self.type == PortType.BINARY and self.id is None:
            raise TypeError(
                f"{type(self)}s of type {self.type} must have a unique 'id' set"
            )


@dataclass
class ViewDeclaration:
    name: str
    description: str


# re-exporting symbols so that "import knime_node" will include the most needed features
IntParameter = kp.IntParameter
DoubleParameter = kp.DoubleParameter
BoolParameter = kp.BoolParameter
StringParameter = kp.StringParameter
parameter_group = kp.parameter_group
ColumnParameter = kp.ColumnParameter
MultiColumnParameter = kp.MultiColumnParameter
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
        The dictionary can be edited and supports flow variables of the following types:

        * bool
        * list(bool)
        * float
        * list(float)
        * int
        * list(int)
        * str
        * list(str)

        """
        return self._flow_variables

    def set_warning(self, message: str) -> None:
        """
        Sets a warning on the node.

        Args:
            message: the warning message to display on the node
        """
        self._java_ctx.set_warning(str(message))


class ConfigurationContext(_BaseContext):
    """
    The ConfigurationContext provides utilities to communicate with KNIME
    during a node's configure() method.
    """

    def __init__(self, java_config_ctx, flow_variables) -> None:
        super().__init__(java_config_ctx, flow_variables)


class ExecutionContext(_BaseContext):
    """
    The ExecutionContext provides utilities to communicate with KNIME during a
    node's execute() method.
    """

    def __init__(self, java_exec_ctx, flow_variables) -> None:
        super().__init__(java_exec_ctx, flow_variables)

    def set_progress(self, progress: float, message: str = None):
        """Set the progress of the execution.

        Args:
            progress: a floating point number between 0 and 1
            message: an optional message to display in KNIME with the progress
        """
        if not isinstance(progress, float):
            raise ValueError(f"progress must be of type float. Got {type(progress)}.")
        if progress < 0 or progress > 1:
            raise ValueError("progress must be between 0 and 1.")

        if message is None:
            self._java_exec_ctx.set_progress(progress)
        else:
            if not isinstance(message, str):
                raise ValueError("message must be a str or None.")
            self._java_exec_ctx.set_progress(progress, message)

    def is_canceled(self) -> bool:
        """
        Returns true if this node's execution has been canceled from KNIME.
        Nodes can check for this property and return early if the execution does
        not need to finish. Raising a RuntimeError in that case is encouraged.
        """
        return self._java_ctx.is_canceled()


class PythonNode(ABC):
    """
    Extend this class to provide a pure Python based node extension to KNIME Analytics Platform.

    Users can either use the decorators @kn.input_table, @kn.input_binary, @kn.output_table, @kn.output_binary, 
    and @kn.view, or populate the input_ports, output_ports, and view attributes.

    Use the Python logging facilities and its `.warn` and `.error` methods to write warnings
    and errors to the KNIME console.

    **Example**::

        @kn.node("My Predictor", node_type="Predictor", icon_path="icon.png", category="/")
        @kn.input_binary("Trained Model", "Trained fancy machine learning model", id="org.example.my.model")
        @kn.input_table("Data", "The data on which to predict")
        @kn.input_table("Output Data", "The input table with appended double column which holds the predictions")
        class MyPredictor():
            def configure(self, binary_spec, table_spec):
                # append a "double" column to the table_spec
                return table_spec.append(kn.Column(ks.double(), "Predictions"))

            def execute(self, table, binary):
                model = self._load_model_from_bytes(binary)
                df = table.to_pandas()
                new_col = model.predict(df)
                df["Predictions"] = new_col
                return [table.append(kn.Table.from_pandas(df))]

            def _load_model_from_bytes(self, data):
                return pickle.loads(data)
    
    """

    input_ports: List[Port] = None
    output_ports: List[Port] = None
    view: ViewDeclaration = None

    @abstractmethod
    def configure(self, config_context: ConfigurationContext, *inputs):
        """
        Configure this Python node.

        Args:
            config_context: The ConfigurationContext providing KNIME utilities during execution
            *inputs:
                Each input table spec or binary port spec will be added as parameter,
                in the same order that the ports were defined.
        
        Returns:
            Either a single spec, or a tuple or list of specs. The number of specs
            must match the number of defined output ports, and they must be returned in this order.
        
        Raise:
            InvalidConfigurationError:
                If the input configuration does not satisfy this node's requirements.
        """
        pass

    @abstractmethod
    def execute(self, exec_context: ExecutionContext, *inputs):
        """
        Execute this Python node.

        Args:
            exec_context: The ExecutionContext providing KNIME utilities during execution
            *inputs:
                Each input table or binary port object will be added as parameter,
                in the same order that the ports were defined.
                Tables will be provided as a `kn.Table`, while binary data will be a plain
                Python `bytes` object.
        
        Returns:
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

    A node cateogry must only be created once. Use a string encoding the
    absolute category path to add nodes to an existing category.

    Args:
        path (Union[str, Category]): The absolute "path" that lead to this
            category e.g. "/io/read". The segments are the category
            level-IDs, separated by a slash ("/").
        level_id (str): The identifier of the level which is used as a
            path-segment and must be unique at the level specified by
            "path".
        name (str): The name of this category e.g. "File readers".
        description (str): A short description of the category.
        icon (str): File path to 16x16 pixel PNG icon for this category. The
            path must be relative to the root of the extension.
        after (str, optional): Specifies the level-id of the category after
            which this category should be sorted in. Defaults to "".
        locked (bool, optional): Set this to False to allow extensions from
            other vendors to add sub-categories or nodes to this category.
            Defaults to True.

    Returns:
        str: The full path of the category which can be used to create nodes
            inside this category.
    """
    _categories.append(
        _Category(path, level_id, name, description, icon, after, locked)
    )
    if path.endswith("/"):
        return path + level_id
    return f"{path}/{level_id}"


class _Node:
    """Class representing an actual node in KNIME AP."""

    node_factory: Callable
    id: str
    name: str
    node_type: str
    icon_path: str
    category: str
    after: str
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
        node_type: str,
        icon_path: str,
        category: str,
        after: str,
    ) -> None:
        self.node_factory = node_factory
        self.id = id
        self.name = name
        self.node_type = node_type
        self.icon_path = icon_path
        self.category = category
        self.after = after
        self.input_ports = _get_ports(node_factory, "input_ports")
        self.output_ports = _get_ports(node_factory, "output_ports")
        self.views = [_get_view(node_factory)]

    def to_dict(self):
        def port_to_str(port):
            if port.type == PortType.BINARY:
                return f"{port.type}={port.id}"
            else:
                return str(port.type)

        return {
            "id": self.id,
            "name": self.name,
            "node_type": self.node_type,
            "icon_path": self.icon_path,
            "category": self.category,
            "after": self.after,
            "input_port_types": [port_to_str(p) for p in self.input_ports],
            "output_port_types": [port_to_str(p) for p in self.output_ports],
            "input_ports": [
                {"name": p.name, "description": p.description} for p in self.input_ports
            ],
            "output_ports": [
                {"name": p.name, "description": p.description}
                for p in self.output_ports
            ],
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


# TODO allow to pass in other nodes as after?
# TODO allow to define categories
def node(
    name: str, node_type: NodeType, icon_path: str, category: str, after: str = None
) -> Callable:
    """
    Use this decorator to annotate a PythonNode class or function that creates a PythonNode instance that should correspond to a node in KNIME.
    """

    def register(node_factory):
        id = f"{category}/{name}"
        if isinstance(node_type, NodeType):
            nt = node_type.value
        else:
            nt = "Unknown" 
        n = _Node(
            node_factory=node_factory,
            id=id,
            name=name,
            node_type=nt,
            icon_path=icon_path,
            category=category,
            after=after,
        )
        _nodes[id] = n

        def port_injector(*args, **kwargs):
            """
            This method is called whenever a node is instanciated through the node_factory
            which was modified by the @kn.node decorator.

            We inject the found ports/views into the instance after creation so that they are available
            to the users.
            """
            node = node_factory(*args, **kwargs)
            node.input_ports = n.input_ports
            node.output_ports = n.output_ports
            node.view = n.views[0]
            return node

        return port_injector

    return register


class InvalidParametersError(Exception):
    """Error that should be raised if the values of the configured parameters are invalid."""

    def __init__(self, message) -> None:
        super().__init__(message)


def _add_port(node_factory, port_slot: str, port: Port):
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

    getattr(node_factory, port_slot).insert(0, port)

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
    return _get_attr_from_instance_or_factory(node_factory, "view")


def input_binary(name: str, description: str, id: str):
    """
    Use this decorator to define a bytes-serialized port object input of a node.

    Args:
        name:
        description:
        id:
            A unique ID identifying the type of the Port. Only Ports with equal ID
            can be connected in KNIME
    """
    return lambda node_factory: _add_port(
        node_factory, "input_ports", Port(PortType.BINARY, name, description, id)
    )


def input_table(name: str, description: str):
    """
    Use this decorator to define an input port of type "Table" of a node.
    """
    return lambda node_factory: _add_port(
        node_factory, "input_ports", Port(PortType.TABLE, name, description)
    )


def output_binary(name: str, description: str, id: str):
    """
    Use this decorator to define a bytes-serialized port object output of a node.

    Args:
        name:
        description:
        id:
            A unique ID identifying the type of the Port. Only Ports with equal ID
            can be connected in KNIME
    """
    return lambda node_factory: _add_port(
        node_factory, "output_ports", Port(PortType.BINARY, name, description, id)
    )


def output_table(name: str, description: str):
    """
    Use this decorator to define an output port of type "Table" of a node.
    """
    return lambda node_factory: _add_port(
        node_factory, "output_ports", Port(PortType.TABLE, name, description)
    )


def view(name: str, description: str):
    """
    Use this decorator to specify that this node has a view
    """

    def add_view(node_factory):
        setattr(node_factory, "view", ViewDeclaration(name, description))
        return node_factory

    return add_view
