# Pure-Python KNIME Node Extensions

We introduce a new API to write nodes for KNIME completely in Python.

## Python Node Extension Setup

A Python node extension needs to contain a YAML file called `knime.yml` that gives general information about the node extension, which Python module to load, and what conda environment should be used for the nodes.

`knime.yml`:
```yaml
---
name: myextension # Will be concatenated with the group_id to an ID
author: Jane Doe
env_yml_path: path/to/env_yml # Path to the Conda environment yml, from which the environment for this extension will be built
extension_module: my_extension
description: My New Extension # Human readable bundle name / description
long_description: This extension provides functionality that everyone wants to have. # Text describing the extension (optional)
group_id: org.knime.python3.nodes.tests # Will be concatenated with the name to an ID
version: 4.6.0 # First version of the KNIME Analytics Platform, for which this extension can be used
vendor: KNIME AG, Zurich, Switzerland # Who offers the extension
```

The `id` will be of the form `group_id.name`. It needs to be a unique identifier for your extension, so it is a good idea to encode your username or company's URL followed by a logical structure as `group_id` to prevent `id` clashes. For example a developer from KNIME could encode its URL to `org.knime` and add `python3.nodes.tests` to indicate that the extension is a member of `tests` of `nodes` which are part of `python3`.

The extension module will then be put on the Pythonpath and imported by KNIME using `import my_extension`. This module should contain KNIME nodes. Each class decorated with `@kn.node` within this file will become available in KNIME as dedicated node.

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

To use this KNIME Python extension locally, set the `knime.python.extension.config` system property either in your KNIME launch configuration's VM arguments in Eclipse. See the chapters **Registering Python extensions during development** and **Customizing the Python executable** at the end of this document.

## Defining a KNIME Node in Python: Full API

A Python KNIME node should be a class deriving from `KnimePythonNode` and has to implement the `execute` and `configure` methods. The node description is automatically generated from the docstrings of the class and the `execute` method. The node's location in KNIME's _Node Repository_ as well as its icon are specified in the `@kn.node` decorator.

The simplest possible node does nothing but passing the input data to its outputs unmodified:

```python
from typing import List, Tuple
import knime_node as kn

@kn.node(name="My Node", node_type="Learner", icon_path="../icons/icon.png", category="/")
@kn.input_table(name="Input Data", description="The data to process in my node")
@kn.output_table("Output Data", "Result of processing in my node")
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
> * node_type: the type of node, should be one of "Learner", "Manipulator", "Predictor", ...?
> * icon_path: module-relative path to the PNG file to use as icon. TODO: link icon requirements
> * category: defines the path to the node inside KNIME's _Node Repository_

### Node port configuration

The number of input and output ports of a node can be configured by decorating the node with `@kn.input_table`, `@kn.input_port`, 
and respectively `@kn.output_table` and `@kn.output_port`. 
All of these decorators take a `name` and a `description` which will be displayed in the node description.
The port configuration decorators must be positioned _between_ the `@kn.node` decorator and the decorated objects, and their order determines the order of port connectors of the node in KNIME.

The `_table` variants of the decorators configure the port to consume or produce KNIME tables. 

If you want to receive or send other data, e.g. a trained machine learning model, use `@kn.input_port` and `@kn.output_port`. This decorator has an additional argument `id`, used to identify the type of data going along this port connection. Only ports with equal `id` can be
connected, and it is good practice to use your domain in reverse to prevent `id` clashes with other node extensions. 
The data is expected to have type `bytes`.

```python
@kn.node("My Predictor", node_type="Predictor", icon_path="icon.png", category="/")
@kn.input_port("Trained Model", "Trained fancy machine learning model", id="org.example.my.model")
@kn.input_table("Data", "The data on which to predict")
class MyPredictor():
    def configure(self, in_specs: List[Union[kn.Schema, kn.BinaryPortObjectSpec]]):
        return in_specs[1]
    
    def execute(self, input_data: List[Union[kn.Table, bytes]]):
        model = self._load_model_from_bytes(input_data[0])
        table = input_data[1]
        new_col = model.predict(table.to_pandas()) # TODO: make this work :)
        return [table.append_column(new_col)]
    
    def _load_model_from_bytes(self, data):
        return pickle.loads(data)
```

> Alternatively, you can populate the `input_ports` and `output_ports` attributes of your node class (on class or instance level) for more fine grained control.

### Node view declaration

You can use the `@kn.view(name="", description="")` decorator to specify that a node returns a view. 
In that case, the `execute` method should return a tuple of port outputs and the view. 

> **TODO:** Benny, add some details here?

### Defining the node's configuration dialog

The parameters of the KNIME node that should be shown in its configuration dialog are defined in the Python code. Similar to Python's properties we annotate the configuration parameters with `@kn.Parameter`

> **TODO:** Update once https://knime-com.atlassian.net/browse/AP-18300 is done

```python
import knime_node as kn
import knime_schema as ks
import knime_table as kt
import pyarrow as pa

@kn.node(name="My Node", node_type="Learner", icon_path="icon.png", category="/")
@kn.input_table("input", "table")
@kn.output_table("output", "table")
class MyNode():
    name = kn.StringParameter(label="Name", description="This name will be broadcasted...", default_value="peter")
    
    num_repetitions = kn.IntParameter("NumReps", "How often do we repeat?", 1, min_value=1)
    
    @num_repetitions.validator
    def reps_validator(value):
        if value == 2:
            raise ValueError("Stupid value!")
    
    def configure(self, table_schema) -> ks.Schema:
        out_schema = table_schema
        for i in range(self.num_repetitions):
            out_schema.append(ks.Column(ks.string(), self.name + " Column"))
        return out_schema
    
    def execute(self, exec_context, table: kt.ReadTable) -> kt.WriteTable:
        pa_table = table.to_pyarrow()
        col = pa.array([self.name] * len(pa_table))
        field = pa.field(self.name + " Column", type=pa.string())
    
        for i in range(self.num_repetitions):
            pa_table = pa_table.append_column(field, col)
        return kt.write_table(pa_table)
```

More involved nodes might have groups of parameters, which will show up in the UI as sections.

```python
import knime_node as kn
import knime_schema as ks
import knime_table as kt
import pyarrow as pa

@kn.parameter_group(label="My Settings")
class MySettings():
    name = kn.StringParameter(label="Name", description="This name will be broadcasted...", default_value="peter")
    
    num_repetitions = kn.IntParameter("NumReps", "How often do we repeat?", 1, min_value=1)
    
    @num_repetitions.validator
    def reps_validator(value):
        if value == 2:
            raise ValueError("Stupid value!")
            

@kn.node(name="My Node", node_type="Learner", icon_path="icon.png", category="/")
@kn.input_table("input", "table")
@kn.output_table("output", "table")
class MyNode():
    settings = MySettings()
    
    @settings.validator
    def settings_validator(values):
        assert len(values["name"]) > values["num_repetitions"]
    
    def configure(self, table_schema) -> ks.Schema:
        out_schema = table_schema
        for i in range(self.settings.num_repetitions):
            out_schema.append(ks.Column(ks.string(), self.settings.name + " Column"))
        return out_schema
    
    def execute(self, exec_context, table: kt.ReadTable) -> kt.WriteTable:
        pa_table = table.to_pyarrow()
        col = pa.array([self.settings.name] * len(pa_table))
        field = pa.field(self.settings.name + " Column", type=pa.string())
    
        for i in range(self.settings.num_repetitions):
            pa_table = pa_table.append_column(field, col)
        return kt.write_table(pa_table)
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

## Customizing the Python executable

Some extensions might have additional requirements that are not part of the bundled environment e.g. in case of third party models.
For these extensions, it is possible to overwrite the Python executable used for execution.
This can be done via the system property `org.knime.python.extension.config` that has to point to a special YAML file on disc.
Add it to your knime.ini with the following line:
`-Dorg.knime.python.extension.config=path/to/your/config.yml`
The format of the YAML is:
```yml
id.of.first.extension:
  conda_env_path: path/to/conda/env
id.of.second.extension:
  python_executable: path/to/python/executable
```
You have two options to specify a custom Python exectuable:
- Via the `conda_env_path` property (recommended) that points to a Conda environment on your machine
- Via the `python_executable` property that points to an executable script that starts Python (see 'Manually configured Python environments' in https://docs.knime.com/2021-12/python_installation_guide/index.html#configure_python_integration for more details)
If you specify both, then `conda_env_path` will take precedence.
It is your responsibility to ensure that the Python you specified in this file has the necessary dependencies to run the extension.
As illustrated above, you can overwrite the Python executable of multiple extensions.


## Registering Python extensions during development

In order to register a Python extension you are developing, you can added to the `org.knime.python.extension.config` YAML explained above by adding a src property:
```yml
id.of.your.dev.extension:
  src: path/to/your/extension
  conda_env_path: path/to/conda/env
```
Note that you have to specify either `conda_env_path` or `python_executable` because the Analytics Platform doesn't have a bundled environment for your extension installed.

## Other Topics

### Logging

You can use the logging module to send warnings and errors to the KNIME console