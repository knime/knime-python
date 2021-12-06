KNIME Python Script (Labs) API
==============================

This document lists the API of the module ``knime_io`` that functions as main contact point between KNIME
and Python in the KNIME Python Script (Labs) node. 
Please refer to the 
`KNIME Python Integration Guide <https://docs.knime.com/latest/python_installation_guide/index.html>`_
for more details on how to set up and use the node.

Inputs and outputs
------------------

These properties can be used to retrieve data from KNIME, or pass data back to KNIME.
The length of the input and output lists depend on the number of input and output ports of the node.

**Example:**
If you have a Python Script (Labs) node configured with two input tables and one input object, you can
access the two tables via ``knime_io.input_tables[0]`` and ``knime_io.input_tables[1]``, and the input object
via ``knime_io.input_objects[0]``.


.. automodule:: knime_io
   :members: input_tables, input_objects, output_tables, output_objects, output_images, flow_variables

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
   :inherited-members:
   :special-members: __getitem__

.. autoclass:: knime_table.ReadTable
   :members:
   :inherited-members:
   :special-members: __getitem__, __len__

.. autoclass:: knime_table.WriteTable
   :members:
   :inherited-members:

.. autoclass:: knime_table.BatchWriteTable
   :members:
   :inherited-members:

.. toctree::
   :maxdepth: 2
   :caption: Contents:
