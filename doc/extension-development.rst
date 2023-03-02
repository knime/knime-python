Python Extension Development (Labs)
===================================

These classes can be used by developers to implement their own Python nodes for KNIME. 
For a more detailed description see the `Pure Python Node Extensions Guide <https://docs.knime.com/latest/pure_python_node_extensions_guide/index.html#introduction>`_

.. note::
   Before KNIME AP 4.7, the module used to access KNIME functionality was called ``knime_extension``. This module has been renamed 
   to ``knime.extension``.

Nodes
-----

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
^^^^^^^^^^
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
^^^^^^^^^^
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

Validation
++++++++++

While each parameter type listed above has default type validation (eg checking if the IntParameter contains only Integers),
they also support custom validation via a property-like decorator notation. For instance, this can be used to verify that
the parameter value matches a certain criteria (see example below). The validator should be placed below the definition of
the corresponding parameter.

.. autoclass:: knime.extension.IntParameter
   :members:
   :noindex:
   :inherited-members:

Parameter Groups
++++++++++++++++

Additionally these parameters can be combined in ``parameter_groups``. These groups are visualized as sections in the
configuration dialog. Another benefit of defining parameter groups is the ability to provide group validation.
As opposed to only being able to validate a single value when attaching a validator to a parameter, group validators
have access to the values of all parameters contained in the group, allowing for more complex validation routines.

.. autofunction:: knime.extension.parameter_group
   :noindex:


Tables
------

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
^^^^^^^^^^
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


Views
-----

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
