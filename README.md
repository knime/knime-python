# KNIME® Python

[![Jenkins](https://jenkins.knime.com/buildStatus/icon?job=knime-python%2Fmaster)](https://jenkins.knime.com/job/knime-python/job/master/)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=KNIME_knime-python&metric=alert_status&token=55129ac721eacd76417f57921368ed587ad8339d)](https://sonarcloud.io/summary/new_code?id=KNIME_knime-python)

This repository is maintained by the [KNIME Team Rakete](mailto:team-rakete@knime.com).

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

### Install the dev environment

```bash
$ pixi install
```
The environment will be installed in `.pixi/envs/default`. Visual Studio Code will notice that an environment was created and prompt you to use it.

### Running tests

```bash
$ pixi run test  # lets you choose the environment to run the tests
$ pixi run test-all  # run tests in all test environments
```

Or run the tests in the development environment from Visual Studio Code via the "Testing" tab.

### Coverage


Test the unit tests locally and see the coverage:
```bash
$ pixi run coverage
```

This command will write the coverage into an HTML report in `htmlcov/index.html`.
Additionally, you can view the coverage in Visual Studio Code using the extension "Coverage Gutters".

### Code Formatting

Use the [Ruff Formatter](https://docs.astral.sh/ruff/formatter/) for Python files.
```bash
$ pixi run format
```

You can find instructions on how to work with our code or develop extensions for KNIME Analytics Platform in the _knime-sdk-setup_ repository on [BitBucket](https://bitbucket.org/KNIME/knime-sdk-setup) or [GitHub](http://github.com/knime/knime-sdk-setup).

## Join the Community

* [KNIME Forum](https://forum.knime.com/)

