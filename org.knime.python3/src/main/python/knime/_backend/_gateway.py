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
Basic utilities for communicating with the KNIME instance which started the Python process.

@author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
"""

import os
import sys
import importlib
from collections.abc import Iterable, Sequence
from contextlib import ExitStack, AbstractContextManager

from py4j.clientserver import ClientServer, JavaParameters, PythonParameters

# Import this before we start with threading
# https://bugs.python.org/issue42647
import concurrent.futures.thread

client_server = None

DATA_SOURCES = {}
DATA_SINKS = {}


class EntryPoint:
    """
    The base class for all entry points.

    Notes
    -----
    Methods in this class and subclasses are called by a Java process and are therefore
    entry points into the Python process.
    """

    def getPid(self):  # NOSONAR: Method name defined in Java interface
        """
        Get the process identifier of this Python process.

        Returns
        -------
        int
            The process identifier of this Python process.
        """
        return os.getpid()

    def enableDebugging(
        self,
        enable_breakpoints: bool = True,
        enable_debug_log: bool = True,
        debug_log_to_stderr: bool = False,
        port: int = 5678,
    ):
        """
        Enable debugging with specified options.

        Parameters
        ----------
        enable_breakpoints : bool, optional
            Whether to enable breakpoints. Default is True.
        enable_debug_log : bool, optional
            Whether to enable debug logging. Default is True.
        debug_log_to_stderr : bool, optional
            Whether to log debug messages to stderr. Default is False.
        port : int, optional
            The port number for debugging. Default is 5678.

        """
        import debug_util

        debug_util.init_debug(
            enable_breakpoints, enable_debug_log, debug_log_to_stderr, port
        )

    def registerExtensions(  # NOSONAR: Method name defined in Java interface
        self, extensions
    ):
        """
        Register a list of extensions that are imported into the process.

        Parameters
        ----------
        extensions : iterable
            An iterable of strings. The strings must be module names that can will imported
            via `importlib.import_module(s)`. Modules are allowed to execute initialization/
            registration code on import but should keep import time to a minimum.
        """
        # Note: Import errors are given back to the Java caller
        for ext in extensions:
            importlib.import_module(ext)

    def registerPythonValueFactory(
        self,
        python_module,
        python_value_factory_name,
        data_spec,
        data_traits,
        python_value_type_name,
        is_default_python_representation,
    ):
        """
        Register a Python value factory in KNIME.

        Parameters
        ----------
        self : object
            Reference to the current object.

        python_module : str
            The name of the Python module to register.

        python_value_factory_name : str
            The name of the Python value factory.

        data_spec : object
            The data specification of the factory.

        data_traits : object
            The data traits of the factory.

        python_value_type_name : str
            The name of the Python value type.

        is_default_python_representation : bool
            Whether the Python representation is the default.

        """
        import knime.api.types as types

        types.register_python_value_factory(
            python_module,
            python_value_factory_name,
            data_spec,
            data_traits,
            python_value_type_name,
            is_default_python_representation,
        )

    def registerToPandasColumnConverter(
        self,
        python_module,
        python_class_name,
        python_value_type_name,
    ):
        """
        Register a converter for a Python value type to a Pandas column.

        Parameters
        ----------
        python_module : str
            The Python module where the converter is defined.
        python_class_name : str
            The name of the Python class that implements the converter.
        python_value_type_name : str
            The name of the Python value type that the converter converts.

        Side Effects
        ------------
        This function modifies the `_to_pandas_column_converters` dictionary in the `types` module of the KNIME API.

        """
        import knime.api.types as types

        types._to_pandas_column_converters[python_value_type_name] = (
            python_module,
            python_class_name,
        )

    def registerFromPandasColumnConverter(
        self,
        python_module,
        python_class_name,
        python_value_type_name,
    ):
        """
        Register a converter function for converting a Pandas column to a specific data type.

        Parameters
        ----------
        self : object
            The class object.
        python_module : str
            The name of the Python module containing the converter function.
        python_class_name : str
            The name of the Python class containing the converter function.
        python_value_type_name : str
            The name of the data type to convert the Pandas column to.


        Notes
        -----
        This function is used to register custom converter functions for converting specific Pandas column types to Knime data types.

        Examples
        --------

        >>> registerFromPandasColumnConverter(Object, "pandas_module", "pandas_class", "data_type")

        This example registers a converter function to convert a Pandas column of type "data_type" using the "pandas_module" and "pandas_class".
        """
        import knime.api.types as types

        types._from_pandas_column_converters[python_value_type_name] = (
            python_module,
            python_class_name,
        )

    class Java:
        implements = ["org.knime.python3.PythonEntryPoint"]


def connect_to_knime(entry_point: EntryPoint):
    """
    Connect to the KNIME instance that started this Python process.

    This function expects the Python process to have been created by the `PythonGateway`
    Java class. After this function returns `knime._backend._gateway.client_server` will be populated
    and can be used to communicate with the JVM.

    Parameters
    ----------
    entry_point : class
        A class implementing methods that can be called from Java.
    """
    java_port = int(sys.argv[1])
    java_params = JavaParameters(port=java_port, auto_convert=True)
    python_params = PythonParameters(
        port=0, propagate_java_exceptions=True
    )  # Dynamically determine port.

    # Create the client server
    global client_server
    client_server = ClientServer(
        java_parameters=java_params,
        python_parameters=python_params,
        python_server_entry_point=entry_point,
    )
    # Let Java reconnect to the dynamically determined Python port. This is necessary to be able to have several py4j
    # connections established (i.e. several Python nodes running) at the same time.
    python_address = (
        client_server.java_gateway_server.getCallbackClient().getAddress()
    )  # Has not changed.
    python_port = (
        client_server.get_callback_server().get_listening_port()
    )  # Has changed.
    client_server.java_gateway_server.resetCallbackClient(python_address, python_port)


def data_source(identifier: str):
    """
    Creates a function registering a function as a data source mapper.

    If `data_source_mapper(obj)` will be called with an obj with the given identifier the
    function will be called with `obj`.

    Examples
    --------

    obj is a Java data source with the method `get(int): string`.
    >>> obj.getIdentifier()
    'foo.bar.MySource'
    >>> @kg.data_source("foo.bar.MySource")
    ... def f(o):
    ...     print("Mapped MySource")
    ...     return [o.get(i) for i in range(10)]  # Map to a Python list
    >>> l = kg.data_source_mapper(obj)
    Mapped MySource
    >>> l
    ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    >>> type(l)
    <class 'list'>

    Parameters
    ----------
    identifier : str
        A string identifier that uniquely identifies the data source type.
        The same identifier as returned by the Java object `getIdentifier()`.
    """

    def f(mapper):
        """
        Set a mapper function for the DATA_SOURCES dictionary.

        Parameters
        ----------
        mapper : function
            The mapping function to be set for the DATA_SOURCES dictionary.

        Returns
        -------
        function
            The input mapper function.

        Notes
        -----
        - The DATA_SOURCES dictionary is modified with the new mapper function.
        """
        DATA_SOURCES[identifier] = mapper
        return mapper

    return f


def data_sink(identifier: str):
    """
    Creates a function registering a function as a data sink mapper.

    If `data_sink_mapper(obj)` will be called with an obj with the given identifier the
    function will be called with `obj`.

    Parameters
    ----------
    identifier : str
        A string identifier that uniquely identifies the data sink type.
        The same identifier as returned by the Java object `getIdentifier()`.
    """

    def f(mapper):
        """
        Set a mapper function for a data sink.

        Parameters
        ----------
        mapper : function
            The mapper function to be set for the data sink.

        Returns
        -------
        function
            The mapper function that was set.

        Notes
        -----
        - The DATA_SINKS dictionary is modified with the new mapper function.
        """
        DATA_SINKS[identifier] = mapper
        return mapper

    return f


def data_source_mapper(java_data_source):
    """
    Map a Java object which provides data to a Python object which gives access to the data
    using a Pythonic API.

    The mapper for a type of data must be decorated with the decorator `@knime._backend._gateway.data_sink(identifier)`.

    Parameters
    ----------
    java_data_source : object
        The Java object providing data. The object must implement the method `getIdentifier`
        which must return the identifier of the decorated mapper.

    Returns
    -------
    object
        A Python object which provides the data.

    Raises
    ------
    ValueError
        If no mapper is registered for the type of data source.
    """
    identifier = java_data_source.getIdentifier()
    if identifier not in DATA_SOURCES:
        raise ValueError(
            f"No mapper registerd for identifier {identifier}. "
            "Are you missing a KNIME Extension? "
            "If this is your own extension make sure to register a mapper by "
            "decorating a function with `@knime._backend._gateway.data_source(identifier)`."
        )
    return DATA_SOURCES[identifier](java_data_source)


def data_sink_mapper(java_data_sink):
    """
    Map a Java object which collects data to a Python object with a Pythonic API.

    Parameters
    ----------
    java_data_sink : object
        The Java object collecting data. The object must implement the method `getIdentifier`
        which must return the identifier of the decorated mapper.

    Returns
    -------
    python_data_sink : object
        A Python object which collects the data.

    Raises
    ------
    ValueError
        If no mapper is registered for the type of data sink.
    """
    identifier = java_data_sink.getIdentifier()
    if identifier not in DATA_SINKS:
        raise ValueError(
            f"No mapper registerd for identifier {identifier}. "
            "Are you missing a KNIME Extension? "
            "If this is your own extension make sure to register a mapper by "
            "decorating a function with `@knime._backend._gateway.data_sink(identifier)`."
        )
    return DATA_SINKS[identifier](java_data_sink)


class SequenceContextManager(Sequence):
    """
    A sequence of context managers.

    When a SequenceContextManager enters a context all values enter a context and when it leaves
    the context all values will leave the context. To access the values the context
    must be entered.

    Parameters
    ----------
    context_managers : list
        A list of context managers.

    Example
    -------
    >>> from contextlib import contextmanager
    ... @contextmanager
    ... def resource(name):
    ...     print(f"Entering {name}")
    ...     try:
    ...             yield None
    ...     finally:
    ...             print(f"Exiting {name}")
    ... context_managers = [resource(i) for i in range(3)]
    ... with kg.SequenceContextManager(context_managers) as context_list:
    ...     # Do something with the resources
    >>>     print("Doing something inside of the context")
    Entering 0
    Entering 1
    Entering 2
    Doing something inside of the context
    Exiting 2
    Exiting 1
    Exiting 0

    Notes
    --------
    When a `SequenceContextManager` enters a context, all values enter a context, and
    when it leaves the context, all values will leave the context. To access the values,
    the context must be entered.
    """

    def __init__(self, context_managers):
        """
        Create a new SequenceContextManager.

        Parameters
        ----------
        context_managers : list
            A list of context managers.
        """
        # In the docstring we ask for a list (because this is easy to understand)
        # but a Sequence is alright
        if not isinstance(context_managers, Sequence):
            raise ValueError("context_managers must be a Sequence")
        if any([not isinstance(cm, AbstractContextManager) for cm in context_managers]):
            raise ValueError("all elements in the sequence must be context managers")
        self.values = context_managers
        self.entered = None

    def __getitem__(self, index):
        if self.entered is None:
            raise RuntimeError("must enter the context before accessing values")
        # Error handling for the index is done by the list 'entered'
        return self.entered[index]

    def __len__(self) -> int:
        return len(self.values)

    def __enter__(self):
        """
        Enter the context manager and initialize necessary resources.

        Returns
        -------
        self
            The instance of the context manager.

        Raises
        ------
        Any exception raised by the context managers.
        """
        with ExitStack() as stack:
            # Enter the context of all values
            self.entered = [stack.enter_context(v) for v in self.values]
            # pop_all creates a new ExitStack and we remember close to close it later
            self.close = stack.pop_all().close
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context and close the object.

        Parameters
        ----------
        exc_type : type
            Type of the exception that occurred.
        exc_val : exception
            The actual exception that occurred.
        exc_tb : traceback
            The traceback object associated with the exception.

        Returns
        -------
        bool
            Always returns False.

        Notes
        -----
        This method is used as a context manager exit method.

        """
        self.close()
        return False
