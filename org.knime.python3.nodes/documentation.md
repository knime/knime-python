# Pure-Python KNIME Node Extensions

We introduce a new API to write nodes for KNIME completely in Python.

## Contents

- [Pure-Python KNIME Node Extensions](#pure-python-knime-node-extensions)
  - [Contents](#contents)
  - [Tutorials](#tutorials)
    - [Prerequisites](#prerequisites)
    - [Tutorial 1: Your First Python Node From Scratch](#tutorial-1-your-first-python-node-from-scratch)
  - [Python Node Extension Setup](#python-node-extension-setup)
  - [Defining a KNIME Node in Python: Full API](#defining-a-knime-node-in-python-full-api)
    - [Node port configuration](#node-port-configuration)
    - [Defining the node's configuration dialog](#defining-the-nodes-configuration-dialog)
    - [Node view declaration](#node-view-declaration)
  - [Functional Node API](#functional-node-api)
  - [Customizing the Python executable](#customizing-the-python-executable)
  - [Registering Python extensions during development](#registering-python-extensions-during-development)
  - [Other Topics](#other-topics)
    - [Logging](#logging)
    - [Gateway caching](#gateway-caching)

## Tutorials

### Prerequisites
Conda installed (Anaconda or Miniconda). Quickest way:
1. Go to [https://docs.conda.io/en/latest/miniconda.html](https://docs.conda.io/en/latest/miniconda.html)
2. If Windows: download installer and execute
3. If MacOS: download Miniconda3 MacOSX 64-bit pkg and execute
4. If Linux: choose your installer and execute the script via terminal

Extract [documentation-files/basic.zip](documentation-files)

### Tutorial 1: Your First Python Node From Scratch

1. Install the KNIME Analytics Platform (KAP) version 4.6.0 or higher or a Nightly (if Nightly before the release of 4.6.0, use the master update site)

2. Go to File --> Install KNIME Extensions... and search within the `KNIME Labs Extensions` for `KNIME Python Node Development Extension (Labs)`; install it

3. The `tutorial_extension` will be your new extension; note that there is a `knime.yml` which holds information you need in step 5

4. Create a Python environment containing the `knime-python-base` metapackage and for now also `packaging`  

    Example via Conda:  

    `conda create -n my_python_env python=3.9 packaging knime-python-base -c knime -c conda-forge`

    If you do not know where to put the above line, open either your terminal (MacOS/Linux) or open 'Anaconda Prompt' (Windows).  

    If you already have some environment `my_python_env` *(with Python >= 3.9!)* and want to install only `knime-python-base` and `packaging` on top, use _in that environment_ `conda install knime-python-base packaging -c knime -c conda-forge`; it is indispensable that you take both channels, `knime` and `conda-forge`

5. Create/adjust a text file `config.yml` next to your extension; this will provide a link to your extension and to the used Python environment  

    It has the content:

    ```

    <extension_id>:

        src: path/to/folder/of/template

        conda_env_path: path/to/my_python_env

    ```

    As <extension_id> use from the `knime.yml` of your extension the `group_id` and `name` and connect them with a dot: `your_group_id.name`.  
    For the `conda_env_path` you can activate your environment via `conda activate my_python_env` and then `which python` to see the path. Finally, replace the `src` path accordingly.

6. Let the KAP know where the `config.yml` is; this allows the KAP to use the extension and its Python environment; for this go to `<path>/<your-KAP>/Contents\Eclipse\knime.ini`; in that `knime.ini` add the following line at the end and change it accordingly: `-Dknime.python.extension.config=path/to/your/config.yml`

7. Start your KAP

8. You should see the template node in the node repository

9. Import and open the included testworkflow `Example_with_Python_node.knwf`:  
    1.  Get familiar with the table 
    2.  Compare the `my_extension.py` with the node you see in the KAP; in particular, understand where the descriptions and names for the node, the inputs and outputs and parameters come from 
    3.  Execute the node and check that it has an output table

10. Build your first dialog! In my_extension.py: uncomment the parameters; congrats, you did your first configuration dialog! (also, take a minute to compare names, description and default values between the `my_extension.py` and the dialog)

11. Add your first port! Uncomment the corresponding lines to get a second input table:
    1.   Uncomment the input table in line 13 
    2.   Change the `configure` method to reflect the changes in the schema
    3.   Change the `execute` method to reflect the change of the additional input table

12. Add your first functionality! We will append a new column to the first table and output that new table:
    1.  `configure`: To inform downstream nodes of the changed schema, we need to change it in the return statement of the `configure` method; for this, we append metadata about a column to the output schema (lines 41f)
    2.  `execute`: Everything else is done in the execute method; we transform both input tables to Pandas dataframes and append a new column to the first Pandas dataframe (line 48)
    3.  `execute`: We transform that dataframe back to a KNIME table and return it (line 48)

13. Use your parameters and start logging!
    1.  `execute`: Use the LOGGER functionality to inform users or for debugging
    2.  `execute`: Use a parameter to change some table content; we will use a lambda for a row-wise multiplication with the parameter (line 53)

14. Execute; congrats, you have your first node doing something!



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
version: 0.1.0 # Version of this Python node extension. Must use three-component semantic versioning for deployment to work.
vendor: KNIME AG, Zurich, Switzerland # Who offers the extension
license_file: LICENSE.TXT # Best practice: put your LICENSE.TXT next to the knime.yml; otherwise you would need to change to path/to/LICENSE.txt
```

The `id` of the extension will be of the form `group_id.name`. It needs to be a unique identifier for your extension, so it is a good idea to encode your username or company's URL followed by a logical structure as `group_id` to prevent `id` clashes. For example a developer from KNIME could encode its URL to `org.knime` and add `python3.nodes.tests` to indicate that the extension is a member of `tests` of `nodes` which are part of `python3`.

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
├── LICENSE.txt
└── my_conda_env.yml
```

> See [Tutorial 1](#tutorial-1-your-first-python-node-from-scratch) above for an example.

To use this KNIME Python extension locally, set the `knime.python.extension.config` system property either in your KNIME launch configuration's VM arguments in Eclipse. See the chapters **Registering Python extensions during development** and **Customizing the Python executable** at the end of this document.

## Defining a KNIME Node in Python: Full API

A Python KNIME node needs to implement the `execute` and `configure` methods, so it will generally be a class. The node description is automatically generated from the docstrings of the class and the `execute` method. The node's location in KNIME's _Node Repository_ as well as its icon are specified in the `@kn.node` decorator.

The simplest possible node does nothing but passing an input table to its output unmodified:

```python
from typing import List, Tuple
import knime_node as kn
import knime_table as kt
import knime_schema as ks

@kn.node(name="My Node", node_type=kn.NodeType.MANIPULATOR, icon_path="../icons/icon.png", category="/")
@kn.input_table(name="Input Data", description="The data to process in my node")
@kn.output_table("Output Data", "Result of processing in my node")
class MyNode:
    def configure(self, config_context, input_table_schema):
        return input_table_schema

    def execute(self, exec_context, input_table)
        return input_table
```

> `@kn.node`'s configuration options are:
> * name: the name of the node in KNIME
> * node_type: the type of node, one of `kn.NodeType.MANIPULATOR`, `kn.NodeType.LEARNER`, `kn.NodeType.PREDICTOR`, `kn.NodeType.SOURCE`, `kn.NodeType.SINK` or `kn.NodeType.VISUALIZER`
> * icon_path: module-relative path to a 16x16 pixel PNG file to use as icon
> * category: defines the path to the node inside KNIME's _Node Repository_.

### Node port configuration

The number of input and output ports of a node can be configured by decorating the node with `@kn.input_table`, `@kn.input_binary`, 
and respectively `@kn.output_table` and `@kn.output_binary`. 
All of these decorators take a `name` and a `description` which will be displayed in the node description.
The port configuration decorators must be positioned _between_ the `@kn.node` decorator and the decorated objects, and their order determines the order of port connectors of the node in KNIME.

The `_table` variants of the decorators configure the port to consume or produce KNIME tables. 

If you want to receive or send other data, e.g. a trained machine learning model, use `@kn.input_binary` and `@kn.output_binary`. This decorator has an additional argument `id`, used to identify the type of data going along this port connection. Only ports with equal `id` can be
connected, and it is good practice to use your domain in reverse to prevent `id` clashes with other node extensions. 
The data is expected to have type `bytes`.

The port configuration determines the expected signature of the `configure` and `execute` methods. 

In the `configure` method, the first argument is a `ConfigurationContext`, followed by one argument per input port. For input table ports, the argument will be of type `kn.Schema`, for binary ports of `kn.BinaryPortObjectSpec`. The `configure` method is expected to return as many parameters as it has output ports configured, again of the types `kn.Schema` for tables and `kn.BinaryPortObjectSpec` for binary data. The order of the arguments and return values must match the order of the input and output port declarations. The arguments and expected return values of `execute` follow the same schema: one argument per input port, one return value per output port. 

Here is an example with two input ports and one output port.

```python
@kn.node("My Predictor", node_type=kn.NodeType.PREDICTOR, icon_path="icon.png", category="/")
@kn.input_binary("Trained Model", "Trained fancy machine learning model", id="org.example.my.model")
@kn.input_table("Data", "The data on which to predict")
@kn.output_table("Output", "Resulting table")
class MyPredictor():
    def configure(self, config_context, binary_input_spec, table_schema):
        # We will add one column of type double to the table
        return table_schema.append(kn.Column(kn.double(), "Predictions"))
    
    def execute(self, exec_context, trained_model, input_table):
        model = self._load_model_from_bytes(trained_model)
        predictions = model.predict(input_table.to_pandas())
        output_table = input_table
        output_table["Predictions"] = predictions
        return kn.Table.from_pandas(output_table)
    
    def _load_model_from_bytes(self, data):
        return pickle.loads(data)
```

> Alternatively, you can populate the `input_ports` and `output_ports` attributes of your node class (on class or instance level) for more fine grained control.

### Defining the node's configuration dialog

> TODO: Ivan add some more info here

The parameters of the KNIME node that should be shown in its configuration dialog are defined in the Python code. We have defined a set of parameter types to use, and these must be placed top level in your node class (they work like Python descriptors).

The availabla parameter types are

* `kn.IntParameter` for integral numbers
* `kn.DoubleParameter` for floating point numbers
* `kn.StringParameter` for string parameters
* `kn.BoolParameter` for boolean parameters
* `kn.ColumnParameter` for a single column selection
* `kn.MultiColumnParameter` to select multiple columns

All of those have arguments `label` and `description` as well as a `default_value`.

Per-parameter validation can be added similar to Python property setters. Define a function that receives a potential value for the parameter, and decorate it with `@my_parameter_name.validator` as seen in the example below.

```python
import knime_node as kn
import knime_schema as ks
import knime_table as kt
import pyarrow as pa

@kn.node(name="My Node", node_type=kn.NodeType.MANIPULATOR, icon_path="icon.png", category="/")
@kn.input_table(name="input", description="table")
@kn.output_table(name="output", description="table")
class MyNode:
    name = kn.StringParameter(label="Name", description="This name will be broadcasted...", default_value="foobar")
    
    num_repetitions = kn.IntParameter(label="NumReps", description="How often do we repeat?", default_value=1, min_value=1)
    
    @num_repetitions.validator
    def reps_validator(value):
        if value == 2:
            raise ValueError("I don't like the number 2")
    
    def configure(self, config_context, table_schema):
        out_schema = table_schema
        for i in range(self.num_repetitions):
            out_schema.append(ks.Column(ks.string(), self.name + " Column"))
        return out_schema
    
    def execute(self, exec_context, table):
        pa_table = table.to_pyarrow()
        col = pa.array([self.name] * len(pa_table))
        field = pa.field(self.name + " Column", type=pa.string())
    
        for i in range(self.num_repetitions):
            pa_table = pa_table.append_column(field, col)
        return kn.Table.from_pyarrow(pa_table)
```

More involved nodes might have groups of parameters, which will show up in the UI as sections. For these, you can define a class similar to a dataclass but using the `@kn.parameter_group` decorator which will turn this class into a parameter group that can be used inside your node just like the other parameters.

Validation on group level can be added using the `@my_group_instance.validator`, where the method receives a dictionary containing `parameter_name : value` mappings.

```python
import knime_node as kn
import knime_schema as ks
import knime_table as kt
import pyarrow as pa

@kn.parameter_group(label="My Settings")
class MySettings:
    name = kn.StringParameter(label="Name", description="This name will be broadcasted...", default_value="peter")
    
    num_repetitions = kn.IntParameter("NumReps", "How often do we repeat?", 1, min_value=1)
    
    @num_repetitions.validator
    def reps_validator(value):
        if value == 2:
            raise ValueError("I don't like the number 2")
            

@kn.node(name="My Node", node_type=kn.NodeType.MANIPULATOR, icon_path="icon.png", category="/")
@kn.input_table("input", "table")
@kn.output_table("output", "table")
class MyNode:
    settings = MySettings()
    
    @settings.validator
    def settings_validator(values):
        assert len(values["name"]) > values["num_repetitions"]
    
    def configure(self, config_context, table_schema):
        out_schema = table_schema
        for i in range(self.settings.num_repetitions):
            out_schema.append(ks.Column(ks.string(), self.settings.name + " Column"))
        return out_schema
    
    def execute(self, exec_context, table: kt.ReadTable):
        pa_table = table.to_pyarrow()
        col = pa.array([self.settings.name] * len(pa_table))
        field = pa.field(self.settings.name + " Column", type=pa.string())
    
        for i in range(self.settings.num_repetitions):
            pa_table = pa_table.append_column(field, col)
        return kt.write_table(pa_table)
```

### Node view declaration

You can use the `@kn.output_view(name="", description="")` decorator to specify that a node returns a view. 
In that case, the `execute` method should return a tuple of port outputs and the view (of type `knime_views.NodeView`). The package `knime_views` contains utilities to create node views from different kinds of objects.

```python
from typing import List
import knime_node as kn
import knime_table as kt
import knime_schema as ks
import knime_views as kv
import seaborn as sns


@kn.node(name="My Node", node_type=kn.NodeType.VISUALIZER, icon_path="icon.png", category="/")
@kn.input_table(name="Input Data", description="We read data from here")
@kn.output_view(name="My pretty view", description="Showing a seaborn plot")
class MyViewNode(kn.PythonNode):
    """A view node

    This node shows a plot.
    """

    def configure(self, config_context, input_table_schema)
        pass

    def execute(self, exec_context, table):
        df = table.to_pandas()
        sns.lineplot(x="x", y="y", data=df)
        return kv.view_seaborn()
```

## Customizing the Python executable

Some extensions might have additional requirements that are not part of the bundled environment e.g. in case of third party models.
For these extensions, it is possible to overwrite the Python executable used for execution.
This can be done via the system property `knime.python.extension.config` that has to point to a special YAML file on disc.
Add it to your knime.ini with the following line:
`-Dknime.python.extension.config=path/to/your/config.yml`
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

In order to register a Python extension you are developing, you can added to the `knime.python.extension.config` YAML explained above by adding a src property:
```yml
id.of.your.dev.extension:
  src: path/to/your/extension
  conda_env_path: path/to/conda/env
  debug_mode: true
```
Note that you have to specify either `conda_env_path` or `python_executable` because the Analytics Platform doesn't have a bundled environment for your extension installed.
For debugging it is also advisable to enable the debug mode by setting `debug_mode: true`.
The debug mode disables caching of Python processes which allows some of your code changes to be immediately shown in the Analytics Platform.
Those changes include:
- Changes to the execute and configure runtime logic
- Changes to existing parameters e.g. changing the label
Other changes such as adding a node or changing a node description require a restart of the Analytics Platform to take effect.
Last but not least, fully enabling and disabling the debug mode also requires a restart.

## Other Topics

### Logging

You can use the logging module to send warnings and errors to the KNIME console

### Gateway caching

In order to allow for a smooth user experience, the Analytics Platform caches the gateways used for non-execution tasks (such as the spec propagation or settings validation) of the last used Python extensions. This cache can be configured via two system properties:
- knime.python.extension.gateway.cache.size: Controls for how many extensions the gateway is cached. If the cache is full and a gateway for a new extension is requested, then the gateway of the least recently used extension is evicted from the cache. The default value is 3.
- knime.python.extension.gateway.cache.expiration: Controls the time period in seconds after which an unused gateway is removed from the cache. The default is 300 seconds.

It is also possible to disable caching for individual extensions via the config.yml file by adding the property `debug_mode: true` to the extensions entry. Note that you will have to restart the Analytics Platform for the config change to take effect. By default all extensions use caching.