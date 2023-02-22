Python Script API
=================

This section lists the API of the module ``knime.scripting.io`` that functions as the main contact point between KNIME
and Python in the `KNIME Python Script node <https://docs.knime.com/latest/python_installation_guide/index.html#_introduction_2>`_.
Please refer to the `KNIME Python Integration Guide <https://docs.knime.com/latest/python_installation_guide/index.html>`_ for more details on how to set up and use the node.

.. note::
   Before KNIME AP 4.7, the module used to interact with KNIME from Python was called ``knime_io`` and provided a slightly
   different API. Since KNIME AP 4.7 the new Python Script node is no longer in `Labs` status and uses the ``knime.scripting.io``
   module for interaction between KNIME and Python. It uses the same Table and Batch classes as can be used in KNIME Python Extensions.
   The previous API is described in :ref:`Deprecated Python Script API`


Inputs and outputs
------------------

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
-------

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
