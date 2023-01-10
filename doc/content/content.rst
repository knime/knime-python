Contents
========

Python Script API
------------------------------------

This section lists the API of the module ``knime.scripting.io`` that functions as the main contact point between KNIME
and Python in the `KNIME Python Script node <https://docs.knime.com/latest/python_installation_guide/index.html#_introduction_2>`_.
Please refer to the `KNIME Python Integration Guide <https://docs.knime.com/latest/python_installation_guide/index.html>`_ for more details on how to set up and use the node.

.. note::
   Before KNIME AP 4.7, the module used to interact with KNIME from Python was called ``knime_io`` and provided a slightly
   different API. Since KNIME AP 4.7 the new Python Script node is no longer in `Labs` status and uses the ``knime.scripting.io``
   module for interaction between KNIME and Python. It uses the same Table and Batch classes as can be used in KNIME Python Extensions.
   The previous API is described in :ref:`Deprecated Python Script API`


Inputs and outputs
^^^^^^^^^^^^^^^^^^

These properties can be used to retrieve data from or pass data back to KNIME Analytics Platform.
The length of the input and output lists depends on the number of input and output ports of the node.

**Example:**
If you have a Python Script node configured with two input tables and one input object, you can
access the two tables via ``knime.scripting.io.input_tables[0]`` and ``knime.scripting.io.input_tables[1]``, and the input object
via ``knime.scripting.io.input_objects[0]``.

.. automodule:: knime.scripting.io
   :members: input_tables, input_objects, output_tables, output_objects, output_images, output_view, flow_variables
   :noindex:


Classes
^^^^^^^^^^^^^^^^^^

.. autoclass:: knime.scripting.io.Table
   :members:
   :noindex:
   :inherited-members:
   :special-members: __getitem__, __len__


.. autoclass:: knime.scripting.io.BatchOutputTable
   :members:
   :noindex:
   :inherited-members:


Views
^^^^^^^^^^^^^^^^^^

.. autofunction:: knime.scripting.io.view
   :noindex:

.. autofunction:: knime.scripting.io.view_matplotlib
   :noindex:

.. autofunction:: knime.scripting.io.view_seaborn
   :noindex:

.. autofunction:: knime.scripting.io.view_plotly
   :noindex:

.. autofunction:: knime.scripting.io.view_html
   :noindex:

.. autofunction:: knime.scripting.io.view_svg
   :noindex:

.. autofunction:: knime.scripting.io.view_png
   :noindex:

.. autofunction:: knime.scripting.io.view_jpeg
   :noindex:

.. autofunction:: knime.scripting.io.view_ipy_repr
   :noindex:

.. autoclass:: knime.scripting.io.NodeView
   :members:
   :noindex:


Python Extension Development (Labs)
-------------------------------------

These classes can be used by developers to implement their own Python nodes for KNIME. 
For a more detailed description see the `Pure Python Node Extensions Guide <https://docs.knime.com/latest/pure_python_node_extensions_guide/index.html#introduction>`_

.. note::
   Before KNIME AP 4.7, the module used to access KNIME functionality was called ``knime_extension``. This module has been renamed 
   to ``knime.extension``.

Nodes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: knime.extension.PythonNode
   :members:
   :noindex:
   :inherited-members:

A node is part of a category:

.. autofunction:: knime.extension.category
   :noindex:

A node has a type:

.. autoclass:: knime.extension.NodeType
   :members:
   :noindex:
   :inherited-members:

A node's configure method receives a configuration context that lets you interact with KNIME

.. autoclass:: knime.extension.ConfigurationContext
   :members:
   :noindex:
   :inherited-members:

A node's execute method receives an execution context that lets you interact with KNIME and 
e.g. check whether the user has cancelled the execution of your Python node.

.. autoclass:: knime.extension.ExecutionContext
   :members:
   :noindex:
   :inherited-members:

Decorators
++++++++++++++++++++++++++++++
These decorators can be used to easily configure your Python node.

.. autofunction:: knime.extension.node
   :noindex:

.. autofunction:: knime.extension.input_binary
   :noindex:

.. autofunction:: knime.extension.input_table
   :noindex:

.. autofunction:: knime.extension.output_binary
   :noindex:

.. autofunction:: knime.extension.output_table
   :noindex:

.. autofunction:: knime.extension.output_view
   :noindex:

Parameters
++++++++++++++++++++++++++++++
To add parameterization to your nodes, the configuration dialog can be defined and customized. Each parameter can be
used in the nodes execution by accessing ``self.param_name``. These parameters can be set up by using
the following parameter types. For a more detailed description see
`Defining the node's configuration dialog <https://docs.knime.com/latest/pure_python_node_extensions_guide/index.html#_defining_the_nodes_configuration_dialog>`_.

.. autoclass:: knime.extension.IntParameter
   :members:
   :noindex:
   :inherited-members:
   :exclude-members: validator

.. autoclass:: knime.extension.DoubleParameter
   :members:
   :noindex:
   :inherited-members:
   :exclude-members: validator


.. autoclass:: knime.extension.BoolParameter
   :members:
   :noindex:
   :inherited-members:
   :exclude-members: validator


.. autoclass:: knime.extension.StringParameter
   :members:
   :noindex:
   :inherited-members:
   :exclude-members: validator


.. autoclass:: knime.extension.ColumnParameter
   :members:
   :noindex:
   :inherited-members:
   :exclude-members: validator


.. autoclass:: knime.extension.MultiColumnParameter
   :members:
   :noindex:
   :inherited-members:
   :exclude-members: validator


.. autoclass:: knime.extension.EnumParameter
   :members:
   :noindex:
   :inherited-members:
   :exclude-members: validator


.. autoclass:: knime.extension.EnumParameterOptions
   :members:
   :noindex:
   :inherited-members:

**Validation**

While each parameter type listed above has default type validation (eg checking if the IntParameter contains only Integers),
they also support custom validation via a property-like decorator notation. For instance, this can be used to verify that
the parameter value matches a certain criteria (see example below). The validator should be placed below the definition of
the corresponding parameter.

.. autoclass:: knime.extension.IntParameter
   :members:
   :noindex:
   :inherited-members:

**Parameter Groups**

Additionally these parameters can be combined in ``parameter_groups``. These groups are visualized as sections in the
configuration dialog. Another benefit of defining parameter groups is the ability to provide group validation.
As opposed to only being able to validate a single value when attaching a validator to a parameter, group validators
have access to the values of all parameters contained in the group, allowing for more complex validation routines.

.. autofunction:: knime.extension.parameter_group
   :noindex:


Tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``Table`` and ``Schema`` are the two classes that are used to communicate tabular data (Table) during execute,
or the table structure (Schema) in configure between Python and KNIME.

.. autoclass:: knime.extension.Table
   :members:
   :noindex:
   :inherited-members:
   :special-members: __getitem__


.. autoclass:: knime.extension.BatchOutputTable
   :members:
   :noindex:
   :inherited-members:
   :special-members: __getitem__


.. autoclass:: knime.extension.Schema
   :members:
   :noindex:
   :inherited-members:
   :special-members: __getitem__


.. autoclass:: knime.extension.Column
   :members:
   :noindex:
   :inherited-members:
   :special-members: __init__

Data Types
++++++++++++++++++++++++++++++
These are helper functions to create KNIME compatible datatypes. For instance, if a new column is created.


.. autofunction:: knime.extension.int32
   :noindex:

.. autofunction:: knime.extension.int64
   :noindex:

.. autofunction:: knime.extension.double
   :noindex:

.. autofunction:: knime.extension.bool_
   :noindex:

.. autofunction:: knime.extension.string
   :noindex:

.. autofunction:: knime.extension.blob
   :noindex:

.. autofunction:: knime.extension.list_
   :noindex:

.. autofunction:: knime.extension.struct
   :noindex:

.. autofunction:: knime.extension.logical
   :noindex:


Deprecated Python Script API
----------------------------

This section lists the API of the module ``knime_io`` that functioned as the main contact point between KNIME
and Python in the `KNIME Python Script node <https://docs.knime.com/latest/python_installation_guide/index.html#_introduction_2>`_ 
in KNIME AP before version 4.7, when the Python Script node was moved out of Labs.
Please refer to the `KNIME Python Integration Guide <https://docs.knime.com/latest/python_installation_guide/index.html>`_ 
for more details on how to set up and use the node.

.. warning::
   This API is deprecated since KNIME AP 4.7, please use the current API as described in :ref:`Python Script API`

Inputs and outputs
^^^^^^^^^^^^^^^^^^

These properties can be used to retrieve data from or pass data back to KNIME Analytics Platform.
The length of the input and output lists depends on the number of input and output ports of the node.

**Example:**
If you have a Python Script node configured with two input tables and one input object, you can
access the two tables via ``knime_io.input_tables[0]`` and ``knime_io.input_tables[1]``, and the input object
via ``knime_io.input_objects[0]``.

.. automodule:: knime_io
   :members: input_tables, input_objects, output_tables, output_objects, output_images, flow_variables
   :noindex:

Factory methods
^^^^^^^^^^^^^^^^^^

Use these methods to fill the ``knime_io.output_tables``.

.. automodule:: knime_io
   :members: write_table, batch_write_table
   :noindex:

Classes
^^^^^^^^^^^^^^^^^^
.. autoclass:: knime.scripting._deprecated._table.Batch
   :members:
   :noindex:
   :inherited-members:
   :special-members: __getitem__

.. autoclass:: knime.scripting._deprecated._table.ReadTable
   :members:
   :noindex:
   :inherited-members:
   :special-members: __getitem__, __len__

.. autoclass:: knime.scripting._deprecated._table.WriteTable
   :members:
   :noindex:
   :inherited-members:

.. autoclass:: knime.scripting._deprecated._table.BatchWriteTable
   :members:
   :noindex:
   :inherited-members:
