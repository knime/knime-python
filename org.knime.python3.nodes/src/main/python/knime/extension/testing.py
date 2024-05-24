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
Experimental unit testing support for Python based KNIME nodes.

This module provides mock implementations for some KNIME internals as
well as an TestingExecutionContext and a TestingConfigureContext which you
can instantiate, e.g. set some flow variables, or overwrite the behavior
of some methods if needed.

With that, you can build unit tests that instanciate a node and run configure
or execute, using Pandas DataFrames as inputs and outputs (Arrow is not implemented
for testing).

If you need any extension types like e.g. Chemistry or Geospatial types, we provide
a method ``register_extension(plugin_xml)`` that you can point to the source of the
plugin providing these extensions. It will load load the Python type definition of
this plugin so that it will be available in the test as usual (e.g. ``import knime.types.chemistry``).

Examples
--------

>>> import knime.extension as knext
... import knime.extension.testing as ktest
...
... # set up test input
... input_df = pd.DataFrame({"Test": [1, 2, 3, 4]})
... input = knext.Table.from_pandas(input_df)
...
... # assume TestNode is your Python node with one input and one output table
... node = TestNode()
... node.column = "Test" # configure parameter "column" to the value "Test"
...
... # create testing execution context
... exec_context = ktest.TestingExecutionContext()
...
... # execute node
... output = node.execute(exec_context, input)
... output_df = output.to_pandas()
...
... # TODO: check that the output DataFrame matches your expectations
"""

from typing import Optional, Union, Dict, Any

import knime.extension as knext
import knime.api.table as ktab
import knime.api.schema as ks
import knime.extension.nodes as knodes

_NOT_AVAILABLE = "Not implemented for testing"
_PYARROW_NOT_AVAILABLE = "PyArrow tables not supported for testing"


class _TestingTable(ktab.Table):
    def __init__(self, data: "pandas.DataFrame"):
        self._df = data.copy()

    def batches(self):
        return iter([_TestingTable(self._df)])

    @property
    def num_batches(self):
        return 1

    @property
    def num_rows(self):
        return len(self._df)

    @property
    def num_columns(self):
        return len(self._df.columns)

    @property
    def column_names(self):
        return self._df.columns

    def to_pandas(self, sentinel: Optional[Union[str, int]] = None):
        return self._df

    def to_pyarrow(self, sentinel: Optional[Union[str, int]] = None) -> "pyarrow.Table":
        raise RuntimeError(_PYARROW_NOT_AVAILABLE)

    def _select_columns(self, selection):
        raise RuntimeError("Column Selection not implemented for testing")

    def _append(self, other: ks._Columnar) -> ks._Columnar:
        raise RuntimeError("Appending tables is not implemented for testing")

    def _select_rows(self, selection):
        raise RuntimeError("Row selection is not implemented for testing")

    @property
    def schema(self) -> ks.Schema:
        return _extract_knime_schema_from_df(self._df)


class _TestingBatchOutputTable(ktab.BatchOutputTable):
    def __init__(self):
        self._test_batches = []

    def append(self, batch):
        import pandas as pd

        if isinstance(batch, pd.DataFrame):
            batch = knext.Table.from_pandas(batch)
        elif isinstance(batch, ktab.Table):
            pass
        else:
            raise RuntimeError("Creating Batches from PyArrow not supported in testing")

        self._test_batches.append(batch)

    def batches(self):
        return iter(self._test_batches)

    @property
    def num_rows(self):
        return sum(batch.num_rows for batch in self._test_batches)

    @property
    def num_columns(self):
        return len(self.column_names)

    @property
    def column_names(self):
        if len(self._test_batches) > 0:
            return self._test_batches[0].column_names
        else:
            return []

    @property
    def num_batches(self):
        return len(self._test_batches)

    @property
    def schema(self) -> ks.Schema:
        if len(self._test_batches) > 0:
            return self._test_batches[0].column_names
        else:
            return ks.Schema.from_columns([])

    def to_pandas(self, sentinel: Optional[Union[str, int]] = None):
        import pandas as pd

        return pd.concat([batch.to_pandas() for batch in self._test_batches])


def _extract_knime_schema_from_df(df: "pandas.DataFrame") -> knext.Schema:
    """This method extracts a dict schema from a dataframe.

    It finds the correct logical type for 'object' columns by using the
    first type of the first non-empty element in that column

    Args:
        df: dataframe to parse

    Returns: knime Schema

    """

    columns = []

    # extract schema
    for col_name, col_type in zip(df.columns, df.dtypes):
        col_type = _extract_knime_type_from_series(df[col_name])
        columns.append(knext.Column(col_type, col_name))

    return knext.Schema.from_columns(columns)


def _extract_knime_type_from_series(col: "pandas.Series"):
    from pandas.api.types import is_object_dtype

    try:
        if not is_object_dtype(col.dtype) or col.size == 0:
            return _pandas_to_knime_type(col)

        cleaned = col.dropna()
        if (
            cleaned.size == 0
        ):  # if the column only contains empty elements we keep object type
            return _pandas_to_knime_type(col)

        dtype = type(cleaned.iloc[0])
        if _check_if_local_dt(cleaned, dtype):
            # as we map all pandas ts and dt objects on the ZonedDT ValFac
            # we have to manually determine if it is a local dt object,
            # but that logic is not included in the test framework
            raise RuntimeError("LocalDateTime not supported yet during testing")
        else:
            return knext.logical(dtype)
    except TypeError:
        return _pandas_to_knime_type(col)


def _pandas_to_knime_type(col: "pandas.Series"):
    pd_type = col.dtype
    import numpy as np
    import pandas as pd

    type_map = {
        np.dtype("int32"): knext.int32(),
        np.dtype("int64"): knext.int64(),
        np.dtype("double"): knext.double(),
        pd.StringDtype: knext.string(),
        pd.Int32Dtype: knext.int32(),
        pd.Int64Dtype: knext.int64(),
    }

    try:
        return type_map[pd_type]
    except KeyError:
        # last check: did we have strings?
        if pd_type == np.object_ and isinstance(col.dropna().iloc[0], str):
            return knext.string()
        raise RuntimeError(f"Testing doesn't support type: {pd_type} ({type(pd_type)})")


def _check_if_local_dt(column: "pandas.Series", dtype):
    """Checks if the column contains a dt parse-able type

    Args:
        column: column to check
        dtype: type of the first element in the column

    Returns:true if the column contains a dt parse-able type

    """
    if not (
        str(dtype) == "<class 'pandas._libs.tslibs.timestamps.Timestamp'>"
        or str(dtype) == "<class 'datetime.datetime'>"
    ):
        return False
    # parse timezone
    for elem in column:
        if hasattr(elem, "tzinfo") and elem.tzinfo is not None:
            return False
    return True


class _TestingBackend(ktab._Backend):
    def create_table_from_pandas(self, data, sentinel=None, row_ids: str = "auto"):
        if sentinel is not None or row_ids != "auto":
            raise RuntimeError("Sentinels and RowId modes not supported for testing")

        return _TestingTable(data)

    def create_table_from_pyarrow(self, data, sentinel=None, row_ids: str = "auto"):
        raise RuntimeError(_PYARROW_NOT_AVAILABLE)

    def create_batch_output_table(self, row_ids: str = "keep"):
        return _TestingBatchOutputTable()

    def close(self) -> None:
        # Nothing to do
        pass


class _TestingNodeBackend(knodes._KnimeNodeBackend):
    def register_port_type(
        self, name: str, object_class: type, spec_class: type, id: Optional[str] = None
    ):
        # no need to do anything
        pass

    def get_port_type_for_spec_type(self, spec_type):
        raise RuntimeError("Port type retrieval not implemented for testing")

    def get_port_type_for_id(self, id: str):
        raise RuntimeError("Port type retrieval not implemented for testing")


class TestingBaseContest:
    def __init__(self) -> None:
        self._flow_variables = {}

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
        raise RuntimeError(_NOT_AVAILABLE)

    def get_credentials(self, identifier: str):
        raise RuntimeError(_NOT_AVAILABLE)

    def set_warning(self, message: str) -> None:
        pass  # noop


class TestingConfigurationContext(TestingBaseContest, knext.ConfigurationContext):
    pass


class TestingExecutionContext(TestingBaseContest, knext.ExecutionContext):
    def set_warning(self, message: str) -> None:
        pass  # noop

    def set_progress(self, progress: float, message: str = None):
        pass  # noop

    def is_canceled(self) -> bool:
        return False

    def get_workflow_temp_dir(self) -> str:
        raise RuntimeError(_NOT_AVAILABLE)

    def get_workflow_data_area_dir(self) -> str:
        raise RuntimeError(_NOT_AVAILABLE)

    def get_knime_home_dir(self) -> str:
        raise RuntimeError(_NOT_AVAILABLE)


def register_extension(plugin_xml: str):
    """
    Registers the Python extension types found in the given plugin.xml file
    and puts all modules with extension types on the Pythonpath.

    Parameters
    ----------
    plugin_xml : str
        Provide the path to a plugin.xml that registers Python types at the extension point.
    """
    import xml.etree.ElementTree as ET
    import sys
    import os
    import knime.api.types as kt

    with open(plugin_xml, "r") as xml_source:
        extension_xml = ET.parse(xml_source)

    for extension in extension_xml.findall("extension"):
        if extension.get("point") != "org.knime.python3.types.PythonValueFactory":
            continue

        for module in extension.findall("Module"):
            module_path = module.get("modulePath")
            sys_path = os.path.join(os.path.dirname(plugin_xml), module_path)
            sys.path.append(sys_path)
            module_name = module.get("moduleName")

            for python_value_factory in module.findall("PythonValueFactory"):
                value_factory_str = (
                    '{\\"value_factory_class\\":\\"'
                    + python_value_factory.get("ValueTypeName")
                    + '\\"}'
                )
                kt.register_python_value_factory(
                    module_name,
                    python_value_factory.get("PythonClassName"),
                    '{"type": "struct", "inner_types": ["string", "variable_width_binary"]}',
                    "{"  #
                    + '"type": "struct",'  #
                    + '"traits": {"logical_type": "'
                    + value_factory_str
                    + '"},'  #
                    + '"inner": ['  #
                    + '        {"type": "simple", "traits": {}},'  #
                    + '        {"type": "simple", "traits": {}}'  #
                    + " ]"  #
                    + "}",
                    python_value_factory.get("ValueTypeName"),
                    python_value_factory.get("isDefaultPythonRepresentation", True),
                )


# We set the testing backend as table backend if no "real" (=Arrow) backend has been loaded yet.
# If an arrow backend is imported after the testing backend, it will overwrite the table backend.
if ktab._backend is None:
    ktab._backend = _TestingBackend()

if knodes._backend is None:
    knodes._backend = _TestingNodeBackend()
