# ![Image](https://www.knime.com/files/knime_logo_github_40x40_4layers.png) KNIME® - Python

The KNIME Python Integration closes the gap between KNIME Analytics Platform and Python.
It provides nodes to write and execute Python scripts and functionality to make use of Python in other parts of KNIME Analytics Platform.

_The legacy Python integrations can be found at [knime-python-legacy](https://github.com/KNIME/knime-python-legacy)._

## Content

This repository contains the source code for the KNIME Python Integration.
The code is organized as follows:

* _org.knime.ext.py4j_: OSGi Wrapper for py4j
* _org.knime.python3.py4j.dependencies_: Additional dependencies of the OSGi Wrapper for py4j to make classes visible to the classloader of the py4j wrapper plugin
* _org.knime.python3_: Core functionality for running Python code from KNIME AP
* _org.knime.python3.arrow_: Functionality for transferring Arrow tables between KNIME AP and Python
* _org.knime.python3.arrow.types_: Python implementation of special types
* _org.knime.python3.nodes_: Framework for writing KNIME AP nodes in Python
* _org.knime.python3.scripting_: Useful functionality for providing Python scripting in KNIME AP
* _org.knime.python3.scripting.nodes_: Implementation of Python scripting nodes for KNIME AP
* _org.knime.python3.views_: Library for creating node views in Python

## Development Notes

You can find instructions on how to work with our code or develop extensions for KNIME Analytics Platform in the _knime-sdk-setup_ repository on [BitBucket](https://bitbucket.org/KNIME/knime-sdk-setup) or [GitHub](http://github.com/knime/knime-sdk-setup).

## Join the Community

* [KNIME Forum](https://tech.knime.org/forum/knime-textprocessing)
