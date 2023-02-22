Deprecated Python Script API
============================

This section lists the API of the module ``knime_io`` that functioned as the main contact point between KNIME
and Python in the `KNIME Python Script node <https://docs.knime.com/latest/python_installation_guide/index.html#_introduction_2>`_ 
in KNIME AP before version 4.7, when the Python Script node was moved out of Labs.
Please refer to the `KNIME Python Integration Guide <https://docs.knime.com/latest/python_installation_guide/index.html>`_ 
for more details on how to set up and use the node.

.. warning::
   This API is deprecated since KNIME AP 4.7, please use the current API as described in :ref:`Python Script API`

Inputs and outputs
------------------

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
---------------

Use these methods to fill the ``knime_io.output_tables``.

.. automodule:: knime_io
   :members: write_table, batch_write_table
   :noindex:

Classes
-------

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
