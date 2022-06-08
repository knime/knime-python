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


These classes can be used by developers to implement their own python nodes for KNIME. For a more detailed description see the nodes documentation at:
`Python Extension Tutorials <https://bitbucket.org/KNIME/knime-python-extension-examples/src/master/tutorials/basic/>`_

.. autoclass:: knime_node.PythonNode
   :members:
   :noindex:
   :inherited-members:

A node has a type:

.. autoclass:: knime_node.NodeType
   :members:
   :noindex:
   :inherited-members:



Decorators
++++++++++++++++++++++++++++++
These decorators can be used to easily configure your python node.

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

Tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``Table`` and ``Schema`` are the two classes that are used to communicate tabular data (Table) during execute,
or the table structure (Schema) in configure between Python and KNIME.

.. autoclass:: knime_node.Table
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

