# Overview

This repository contains:

* KNIME Python Integration (major version 2)
* KNIME Python Integration (major versions 2&3)
* KNIME Jython Integration

The KNIME Python Integration (major version 2) is the current default Python Integration providing nodes to connect to python in the "Scripting" category of the node browser. Data transfer is based on google-protobuf. This Python Integration is no longer developed actively.
The KNIME Python Integration (major versions 2&3) is a new Python Integration providing more memory-efficient and processing-time-efficient serialization. For this purpose the (de)serialization module is realized as a pluggable component using the Eclipse extension point mechanism on the Java side.
The "KNIME Jython Integration" includes three Snippet nodes providing capabilities to use Jython in KNIME.

# Details

## KNIME Python Integration (major versions 2&3)

### Contained projects

* *org.knime.features.python2*: contains build.properties and feature.xml (only relevant for the KNIME build system)
* *org.knime.python2*: all controller classes managing the communication between KNIME and Python
* *org.knime.python2.nodes*: KNIME node implementations
* *org.knime.python2.serde.arrow*: a serialization library based on Apache Arrow (see detailed explanation)
* *org.knime.python2.serde.csv*: a serialization library using .csv files (see detailed explanation)
* *org.knime.python2.serde.flatbuffers*: : a serialization library based on google-flatbuffers (see detailed explanation)
* *org.knime.python.typeextensions* (shared with "KNIME Python Integration (major version 2)"): custom (de)serializers for more complex KNIME-types

### Explanation

![Configure dialog](https://bitbucket.org/KNIME/knime-python/raw/master/res/python_node_configure.png)

The KNIME Python Integration (major versions 2&3) provides a variety of nodes for executing python code.  With them, inputs and outputs can be accessed through "magic variables" inside a python script. The available variables can be checked in the table on the left side of the configure dialog (see image above). KNIME tables are translated into pandas.DataFrame objects
on the python side and vice versa. Flow variables can be accessed via a dictionary. Custom serialization methods for a variety of complex data-types allow transferring them between KNIME and the Python Kernel. The so-called typeextensions are defined in the org.knime.python.typeextensions project. At the moment built-in extensions exist for .png images, .svg images, date&time types, XML cells and bytevector cells. Further typeextensions may be defined using the Eclipse extension point mechanism.
Furthermore, python general options, such as the path to the python executables in major version 2 and the serialization library to use in major version 3, can be configured via the python preference page found in the menu under "Preferences -> KNIME -> Python ". Serialization libraries define methods for (de)serializing a KNIME table to a byte representation and vice versa on the Java side, and methods for (de)serializing a byte representation into a pandas.DataFrame and vice versa on the Python side. Serialization libraries are implemented as interchangeable modules using the Eclipse Extension point mechanism. Currently three different serialization libraries are implemented in their respective projects org.knime.python2.serde.arrow (based on the Apache Arrow technology; see: [https://arrow.apache.org/](https://arrow.apache.org/)), org.knime.python2.serde.csv (exchanges data using .csv files), and org.knime.python2.serde.flatbuffers (based on the google-flatbuffers technology; see: [https://google.github.io/flatbuffers/](https://google.github.io/flatbuffers/)).
In the node configure dialog window, the python major version to use can be selected in the options tab. Furthermore, missing value handling can be customized for Int- and Long-Columns, as those are converted to double columns by default as soon as they contain missing values. With the options tab, missing values in these columns can be converted to a sentinel-value (an arbitrary replacement value).

![The Python Integration Nodes in action](https://bitbucket.org/KNIME/knime-python/raw/master/res/python_example_workflow.png)

The following nodes are available in the "KNIME Python Integration (major versions 2&3)" plugin:

* **Python Edit Variable :** edit or append KNIME flow variables
* **Python Source :** run a python script, build a pandas.DataFrame, and transfer it back to KNIME
* **Python Script (1⇒1) :** run a python script processing a single KNIME table, build a pandas.DataFrame, and transfer it back to KNIME
* **Python Script (1⇒2) :** run a python script processing a single KNIME table, build two separate pandas.DataFrames, and transfer them back to KNIME
* **Python Script (2⇒1) :** run a python script processing two KNIME tables, build a pandas.DataFrame, and transfer it back to KNIME
* **Python Script (2⇒2) :** run a python script processing two KNIME tables, build two separate pandas.DataFrames, and transfer them back to KNIME
* **Python View :** run a python script that creates a view, e.g. a diagram
* **Python Object Reader :** read a python object that was written using the Python Object Writer . Creates a special python object at the output that may be processed by the Python Predictor  node
* **Python Object Writer :** write a python object as a pickle-file. Python objects can be created using the Python Learner  node
* **Python Learner :** use python to train a model. The model is returned as a special python object that may be processed by the Python Predictor .
* **Python Predictor :** use python to make predictions on the basis of a KNIME table based on a model.
* **Python Script (DB) :** modify a database query using python. Get back the query results as a pandas.DataFrame.

The node implementations may be found in the project *org.knime.python2.nodes*. All controller classes managing the communication between KNIME and Python can be found in the project *org.knime.python2*.

*NOTE: The name "python2" always refers to the KNIME Python Integration (major versions 2&3).*

A detailed explanation of how to set up python with KNIME can be found here: [https://www.knime.com/blog/setting-up-the-knime-python-extension-revisited-for-python-30-and-20](https://www.knime.com/blog/setting-up-the-knime-python-extension-revisited-for-python-30-and-20) 

## KNIME Python Integration (major version 2)

### Contained Projects

* *org.knime.features.python*: contains build.properties and feature.xml (only relevant for the KNIME build system)
* *org.knime.python*: all controller classes managing the communication between KNIME and Python
* *org.knime.python.nodes*: KNIME node implementations
* *org.knime.python.typeextensions* (shared with KNIME Python Integration (major version 2)): custom (de)serializers for more complex KNIME-types

### Explanation

The KNIME Python Integration (major version 2) has very much the same structure as KNIME Python Integration (major versions 2&3) but uses google-protobuf as (de)serialization backend. Currently not under development. The project org.knime.python.nodes contains the implementation of the included nodes. Again, all controller classes managing the communication between KNIME and Python can be found in the project org.knime.python.

*NOTE: The name "python" always refers to the KNIME Python Integration (major version 2).*

A detailed explanation of how to set up this python integration with KNIME can be found here: [https://www.knime.com/blog/how-to-setup-the-python-extension](https://www.knime.com/blog/how-to-setup-the-python-extension) 

## KNIME Jython Integration

### Contained Projects
* *org.knime.features.ext.jython*: contains build.properties and feature.xml (only relevant for the KNIME build system)
* *org.knime.ext.jython*: all Java sources relevant for the Jython Integration

### Explanation

The following nodes are available in the "KNIME Jython Integration" plugin:
* **JPython Function:** creates a new table column based on a combination of the input table's columns, using, for instance, mathematical operators.
* **JPython Script 1:1:** run a Jython script processing a single KNIME table and producing a single KNIME output table
* **JPython Script 2:1:** run a Jython script processing two KNIME tables and producing a single KNIME output table

All nodes and underlying code are contained in the project *org.knime.ext.jython*.

