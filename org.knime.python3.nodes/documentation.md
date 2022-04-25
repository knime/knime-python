# Pure-Python KNIME Node Extensions

We introduce a new API to write nodes for KNIME completely in Python.

## Python Node Extension Setup

A Python node extension needs to contain a YAML file called `knime.yml` that gives general information about the node extension, which Python module to load, and what conda environment should be used for the nodes.

`knime.yml`:
```yaml
---
id: org.mycompany.knime.nodes.myextension
author: Jane Doe
environment_name: my_conda_env
extension_module: my_extension
```

The `id` needs to be a unique identifier for your extension, so it is a good idea to encode your username or company's URL to prevent `id` clashes.

The extension module will then be put on the Pythonpath and imported by KNIME using `import my_extension`. This module should contain KNIME nodes. Each class decorated with `@kn.Node` within this file will become available in KNIME as dedicated node.

Recommended project folder structure:

```
.
├── icons
│   └── my_node.svg
├── src
│   └── my_extension.py
├── test
│   └── test_my_extension.py
├── knime.yml
└── my_conda_env.yml
```

> **TODO**: Prepare a template project

> See `knime-python/org.knime.python3.nodes.tests/src/test/python/fluent_extension` for an example.

To use this KNIME Python extension locally, set the `knime.python.extensions` system property. Either in your KNIME launch configuration's VM arguments in Eclipse or in `knime.ini` add the following on Windows (extension paths are separated by semicolon "`;`"):

```
-Dknime.python.extensions=C:\path\to\extension\;C:\path\to\other\extension
```

or on linux and macOS, where extension paths are separated by colon "`:`":
```
-Dknime.python.extensions=/path/to/extension/:/path/to/other/extension
```

## Defining a KNIME Node in Python: Full API

A Python KNIME node should be a class deriving from `KnimePythonNode` and has to implement the `execute` and `configure` methods. The node description is automatically generated from the docstrings of the class and the `execute` method. The node's location in KNIME's _Node Repository_ as well as its icon are specified in the `@kn.node` decorator.

The simplest possible node does nothing but passing the input data to its outputs unmodified:

```python
from typing import List, Tuple
import knime_node as kn

@kn.node(name="My Node", node_type="Learner", icon_path="../icons/icon.png", category="/")
class MyNode(kn.PythonNode):
    def __init__(self) -> None:
        super().__init__()

    def configure(self, input_schemas: List[ks.Schema]) -> List[ks.Schema]:
        return input_schemas

    def execute(self, tables: List[kt.ReadTable], objects: List, exec_context) -> Tuple[List[kt.WriteTable], List]:
        return [kt.write_table(table) for table in tables], []

```

> `@kn.node`'s configuration options are:
> * name: the name of the node in KNIME
> * icon_path: module-relative path to the PNG file to use as icon. TODO: link icon requirements
> * category: defines the path to the node inside KNIME's _Node Repository_
>
> **TODO:**
> * num_in_ports: The number of input ports of the node?
> * num_out_ports: The output ports of the node?
> * `input_ports`: A list of specifiers `kn.TABLE` or `kn.PORT_OBJECT` to specify which input ports should be available. Default is a single table.
> * `output_ports`: A list of specifiers `kn.TABLE` or `kn.PORT_OBJECT` to specify which output ports will be populated. Default is a single table.


### Defining the node's configuration dialog

The parameters of the KNIME node that should be shown in its configuration dialog are defined in the Python code. Similar to Python's properties we annotate the configuration parameters with `@kn.Parameter`

> **TODO:** Update once https://knime-com.atlassian.net/browse/AP-18300 is done

```python
import knime_node as kn

@kn.node(name="My Node", node_type="Learner", icon_path="icon.png", category="/")
class MyNode(kn.PythonNode):
    def __init__(self) -> None:
        super().__init__()
        self._some_param = 42

    @kn.Parameter
    def some_param(self):
        return self._some_param

    @some_param.setter
    def some_param(self, value):
        self._some_param = value
    
    ... configure and execute ...
```

## Functional Node API

This shall be the preferred flexible and pythonic API to write KNIME nodes where one just has to define a single function that can work on a `Columnar` datastructure `= knime.Table`.

In the background the `@kn.functional_node` will build a full fledged node like described above.

> **TODO:** implement this :)

The simplest functional node looks like this:

```python
from typing import List
import knime_node as kn

@kn.functional_node(name="My Functional Node", icon_path="../icons/icon.png", category="/")
def my_function(inputs: List[kn.Table]) -> List[kn.Table]:
    return inputs
```

> See `knime-python/org.knime.python3.arrow.tests/src/test/python/unittest/test_functional_table_api.py` for first experiments with this API

## Configuration of alternative Python executable

Every Python node extension comes with a bundled environment that is automatically created during installation.
In most cases this environment should be sufficient for the extension to work as it contains all dependencies.
However, during development this bundled environment is typically not available.
It is therefore possible to provide a YAML file that specifies which Python executable should be used for which Python extension.
The path to this YAML has to be added as system property in the knime.ini via the key word `org.knime.python3.nodes.alternate_executable_yml`.
The YAML supports two types of exectuables, either a Conda environment or another (manual) Python executable (see 'Manually configured Python environments' in https://docs.knime.com/2021-12/python_installation_guide/index.html#configure_python_integration for more details).
Here is an example outlining the format of the YAML:
```yml
org.my.first.extension:
  type: conda
  environment_path: path/to/the/environment
org.my.second.extension:
  type: manual
  command_path: path/to/python/executable
```

