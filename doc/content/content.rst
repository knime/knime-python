Contents
========

Inputs and outputs
------------------

These properties can be used to retrieve data from or pass data back to KNIME Analytics Platform.
The length of the input and output lists depends on the number of input and output ports of the node.

**Example:**
If you have a Python Script (Labs) node configured with two input tables and one input object, you can
access the two tables via ``knime_io.input_tables[0]`` and ``knime_io.input_tables[1]``, and the input object
via ``knime_io.input_objects[0]``.


.. automodule:: knime_io
   :members: input_tables, input_objects, output_tables, output_objects, output_images, flow_variables
   :noindex:

Factory methods
----------------

Use these methods to fill the ``knime_io.output_tables``.

.. automodule:: knime_io
   :members: write_table, batch_write_table
   :noindex:

Classes
--------
.. autoclass:: knime_table.Batch
   :members:
   :noindex:
   :inherited-members:
   :special-members: __getitem__

.. autoclass:: knime_table.ReadTable
   :members:
   :noindex:
   :inherited-members:
   :special-members: __getitem__, __len__

.. autoclass:: knime_table.WriteTable
   :members:
   :noindex:
   :inherited-members:

.. autoclass:: knime_table.BatchWriteTable
   :members:
   :noindex:
   :inherited-members:


Python Extension Development
-------------------------------------

Nodes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


These classes can be used by developers to implement their own Python nodes for KNIME. 
For a more detailed description see the `Pure Python Node Extensions Guide <https://docs.knime.com/latest/pure_python_node_extensions_guide/index.html#introduction>`_

.. autoclass:: knime_node.PythonNode
   :members:
   :noindex:
   :inherited-members:

A node has a type:

.. autoclass:: knime_node.NodeType
   :members:
   :noindex:
   :inherited-members:

A node's configure method receives a configuration context that lets you interact with KNIME

.. autoclass:: knime_node.ConfigurationContext
   :members:
   :noindex:
   :inherited-members:

A node's execute method receives an execution context that lets you interact with KNIME and 
e.g. check whether the user has cancelled the execution of your Python node.

.. autoclass:: knime_node.ExecutionContext
   :members:
   :noindex:
   :inherited-members:

Decorators
++++++++++++++++++++++++++++++
These decorators can be used to easily configure your Python node.

.. autofunction:: knime_node.node
   :noindex:

.. autofunction:: knime_node.input_binary
   :noindex:

.. autofunction:: knime_node.input_table
   :noindex:

.. autofunction:: knime_node.output_binary
   :noindex:

.. autofunction:: knime_node.output_table
   :noindex:

.. autofunction:: knime_node.output_view
   :noindex:

Parameters
++++++++++++++++++++++++++++++
To add parameterization to your nodes, the configuration dialog can be defined and customized. Each parameter can be
used in the nodes execution by accessing ``self.param_name``. These parameters can be set up by using
the following parameter types. For a more detailed description see
`Defining the node's configuration dialog <https://docs.knime.com/latest/pure_python_node_extensions_guide/index.html#_defining_the_nodes_configuration_dialog>`_.

.. autoclass:: knime_parameter.IntParameter
   :members:
   :noindex:
   :inherited-members:
   :exclude-members: validator

.. autoclass:: knime_parameter.DoubleParameter
   :members:
   :noindex:
   :inherited-members:
   :exclude-members: validator


.. autoclass:: knime_parameter.BoolParameter
   :members:
   :noindex:
   :inherited-members:
   :exclude-members: validator


.. autoclass:: knime_parameter.StringParameter
   :members:
   :noindex:
   :inherited-members:
   :exclude-members: validator


.. autoclass:: knime_parameter.ColumnParameter
   :members:
   :noindex:
   :inherited-members:
   :exclude-members: validator


.. autoclass:: knime_parameter.MultiColumnParameter
   :members:
   :noindex:
   :inherited-members:
   :exclude-members: validator

**Validation**

While each parameter type listed above has default type validation (eg checking if the IntParameter contains only Integers),
they also support custom validation via a property-like decorator notation. For instance, this can be used to verify that
the parameter value matches a certain criteria (see example below). The validator should be placed below the definition of
the corresponding parameter.

.. autoclass:: knime_parameter.IntParameter
   :members:
   :noindex:
   :inherited-members:

**Parameter Groups**

Additionally these parameters can be combined in ``parameter_groups``. These groups are visualized as sections in the
configuration dialog. Another benefit of defining parameter groups is the ability to provide group validation.
As opposed to only being able to validate a single value when attaching a validator to a parameter, group validators
have access to the values of all parameters contained in the group, allowing for more complex validation routines.

.. autofunction:: knime_parameter.parameter_group
   :noindex:


Tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``Table`` and ``Schema`` are the two classes that are used to communicate tabular data (Table) during execute,
or the table structure (Schema) in configure between Python and KNIME.

.. autoclass:: knime_node.Table
   :members:
   :noindex:
   :inherited-members:
   :special-members: __getitem__


.. autoclass:: knime_node.BatchOutputTable
   :members:
   :noindex:
   :inherited-members:
   :special-members: __getitem__


.. autoclass:: knime_schema.Schema
   :members:
   :noindex:
   :inherited-members:
   :special-members: __getitem__


.. autoclass:: knime_schema.Column
   :members:
   :noindex:
   :inherited-members:
   :special-members: __init__

Data Types
++++++++++++++++++++++++++++++
These are helper functions to create KNIME compatible datatypes. For instance, if a new column is created.


.. autofunction:: knime_schema.int32
   :noindex:

.. autofunction:: knime_schema.int64
   :noindex:

.. autofunction:: knime_schema.double
   :noindex:

.. autofunction:: knime_schema.bool_
   :noindex:

.. autofunction:: knime_schema.blob
   :noindex:

.. autofunction:: knime_schema.list_
   :noindex:

.. autofunction:: knime_schema.struct
   :noindex:

.. autofunction:: knime_schema.logical
   :noindex:

