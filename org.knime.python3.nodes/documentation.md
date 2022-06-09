# Pure-Python KNIME Node Extensions

We introduce a new API to write nodes for KNIME completely in Python.

## Contents

- [Pure-Python KNIME Node Extensions](#pure-python-knime-node-extensions)
  - [Contents](#contents)
  - [Tutorials](#tutorials)
    - [Prerequisites](#prerequisites)
    - [Tutorial 1: Writing your first Python node from scratch](#tutorial-1-writing-your-first-python-node-extension-from-scratch)
  - [Python Node Extension Setup](#python-node-extension-setup)
  - [Defining a KNIME Node in Python: Full API](#defining-a-knime-node-in-python-full-api)
    - [Node port configuration](#node-port-configuration)
    - [Defining the node's configuration dialog](#defining-the-nodes-configuration-dialog)
    - [Node view declaration](#node-view-declaration)
  - [Customizing the Python executable](#customizing-the-python-executable)
  - [Registering Python extensions during development](#registering-python-extensions-during-development)
  - [Other topics](#other-topics)
    - [Logging](#logging)
    - [Gateway caching](#gateway-caching)

## Tutorials

### Prerequisites
To get started with developing Python Node Extensions, you need to have `conda` installed (via Anaconda or Miniconda). Here is the quickest way:

1. Go to [https://docs.conda.io/en/latest/miniconda.html](https://docs.conda.io/en/latest/miniconda.html)
2. Download the appropriate installer for your OS.
3. For Windows and macOS: run the installer executable.
4. For Linux: execute the script in terminal (see [here](https://conda.io/projects/conda/en/latest/user-guide/install/linux.html) for help)

With `conda` set up, you should extract [documentation-files/basic.zip](documentation-files) to a convenient location. In the `basic` folder, you should see the following files:

```
.
├── tutorial_extension
│   │── icon.png
│   │── knime.yml
│   │── LICENSE.TXT
│   └── my_extension.py
├── config.yml
├── Example_with_Python_node.knwf
└── README.md
```

### Tutorial 1: Writing your first Python node extension from scratch

This is a quickstart guide that will walk you through the essential steps of writing and running your first Python Node Extension. We will use `tutorial_extension` as the basis. The steps of the tutorial requiring modification of the Python code in `my_extension.py` have corresponding comments in the file, for convenience.

For an extensive overview of the full API, please refer to the [Defining a KNIME Node in Python: Full API](#defining-a-knime-node-in-python-full-api) section, as well as our [Read the Docs page](https://knime-python.readthedocs.io/en/latest/content/content.html#python-extension-development).

1. Install the KNIME Analytics Platform (KAP) version 4.6.0 or higher, or a Nightly version (if Nightly before the release of 4.6.0, use the master update site. See [here](https://knime-com.atlassian.net/wiki/spaces/SPECS/pages/1369407489/How+to+find+download+install+update+and+use+KNIME+Nightly+Builds+for+Verification.) for help with Nightlies and update sites.)

2. Go to _File_ -> _Install KNIME Extensions…_, enter "Python" in the search field, and look for `KNIME Python Node Development Extension (Labs)`. Alternatively you can manually navigate to the `KNIME Labs Extensions` category and find the extension there. Select it and proceed with installation.

3. The `tutorial_extension` will be your new extension. Familiarise yourself with the files contained in that folder, in particular:
    - `knime.yml`, which contains important metadata about your extension.
    - `my_extension.py`, which contains Python definitions of the nodes of your extension.
    - `config.yml`, just outside of the folder, contains information that binds your extension and the corresponding `conda`/Python environment with KAP.

4. Create a `conda`/Python environment containing the [`knime-python-base`](https://anaconda.org/knime/knime-python-base) metapackage, together with the node development API [`knime-extension`](https://anaconda.org/knime/knime-extension), and [`packaging`](https://anaconda.org/anaconda/packaging). If you are using `conda`, you can create the environment by running the following command in your terminal (macOS/Linux) or Anaconda Prompt (Windows):

    ```console
    conda create -n my_python_env python=3.9 packaging knime-python-base knime-extension -c knime -c conda-forge
    ```
    If you would like to install `knime-python-base` and `packaging` into an already existing environment, you can run the following command _from within that environment_:

    ```console
    conda install knime-python-base knime-extension packaging -c knime -c conda-forge
    ```

    Note that you __must__ append both the `knime` and `conda-forge` channels to the commands in order for them to work.

5. Edit the `config.yml` file located just outside of the `tutorial_extension` (for this example, the file already exists with prefilled fields and values, but you would need to manually create it for future extensions that you develop). The contents should be as follows:

    ```
    <extension_id>:

        src: path/to/folder/of/template

        conda_env_path: path/to/my_python_env
    ```
    where:

    - `<extension_id>` should be replaced with the `group_id` and `name` values specified in `knime.yml`, combined with a dot.
    
    For our example extension, the value for `group_id` is `org.knime`, and the value for `name` is `fluent_extension`, therefore the `<extension_id>` placeholder should be replaced with `org.knime.fluent_extension`.
    
    - the `src` field should specify the path to the `tutorial_extension` folder.

    - similarly, the `conda_env_path` field should specify the path to the `conda`/Python environment created in Step 4. On macOS/Linux, you can get this path by activating your environment via `conda activate my_python_env` and then running `which python`.

6. We need to let the KAP know where the `config.yml` is in order to allow it to use our extension and its Python environment. To do this, you need to edit the `knime.ini` of your KAP installation, which is located at `<path-to-your-KAP>/Contents/Eclipse/knime.ini`.

    Append the following line to the end, and modify it to have the correct path to `config.yml`: 
    ```
    -Dknime.python.extension.config=path/to/your/config.yml
    ```

7. Start your KAP.

8. The "My Template Node" node should now be visible in the Node Repository.

9. Import and open the `Example_with_Python_node.knwf` workflow, which contains our test node:  
    1. Get familiar with the table.
    2. Study the code in `my_extension.py` and compare it with the node you see in the KAP. In particular, understand where the node name and description, as well as its inputs and outputs, come from.
    3. Execute the node and make sure that it produces an output table.

10. Build your first configuration dialog!

    In `my_extension.py`, uncomment the definitions of parameters (marked by the "Tutorial Step 10" comment), starting from line 23 and until the `configure` method. Restart your KAP, and you should be able to double-click the node and see configurable parameters - congrats!
    
    Take a minute to see how the names, descriptions, and default values compare between their definitions in `my_extension.py` and the node dialog.

11. Add your first port!

    To add a second input table to the node, follow these steps (marked by the "Tutorial Step 11" comment):
    1. Uncomment the `@knext.input_table` decorator on line 13.
    2. Change the `configure` method's definition to reflect the changes in the schema by commenting line 35 and uncommenting line 36.
    3. Change the `execute` method to reflect the addition of the second input table by commenting line 44 and uncommenting line 45.

12. Add some functionality to the node!

    With the following steps, we will append a new column to the first table and output the new table (the lines requiring to be changed are marked by the "Tutorial Step 12" comment):

    1. To inform downstream nodes of the changed schema, we need to change it in the return statement of the `configure` method; for this, we append metadata about a column to the output schema.
    2. Everything else is done in the `execute` method:
        - we transform both input tables to pandas dataframes and append a new column to the first dataframe
        - we transform that dataframe back to a KNIME table and return it

13. Use your parameters and start logging!

    In the `execute` method, uncomment the lines marked by the "Tutorial Step 13" comment:
    
    1. Use the LOGGER functionality to inform users, or for debugging.
    2. Use a parameter to change some table content; we will use a lambda function for a row-wise multiplication using the `double_param` parameter.

14. Congratulations, you have built your first functioning node entirely in Python!

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

The `id` of the extension will be of the form `group_id.name`. It needs to be a unique identifier for your extension, so it is a good idea to encode your username or company's URL followed by a logical structure as `group_id` in order to prevent `id` clashes. For example, a developer from KNIME could encode its URL to `org.knime` and add `python3.nodes.tests` to indicate that the extension is a member of `tests` of `nodes`, which are part of `python3`.

The extension module will then be put on the `Pythonpath` and imported by KNIME using `import my_extension`. This Python module should contain the definitions of KNIME nodes. Each class decorated with `@knext.node` within this file will become available in KNIME as a dedicated node.

Recommended project folder structure:

```
.
├── icons
│   └── my_node_icon.png
├── src
│   └── my_extension.py
├── test
│   └── test_my_extension.py
├── knime.yml
├── LICENSE.txt
└── my_conda_env.yml
```

> See [Tutorial 1](#tutorial-1-writing-your-first-python-node-extension-from-scratch) above for an example.

To use this KNIME Python extension locally, set the `knime.python.extension.config` system property in your KNIME launch configuration's VM arguments in Eclipse. See the [Registering Python extensions during development](#registering-python-extensions-during-development) and [Customizing the Python executable](#customizing-the-python-executable) sections at the end of this document.

## Defining a KNIME Node in Python: Full API

We provide a `conda` package that includes the full API for node development in Python - `knime-extension` (see [Tutorial 1](#tutorial-1-writing-your-first-python-node-extension-from-scratch) for help in setting up your development `conda` environment). To enable helpful code autocompletion via `import knime_extension as knext`, make sure your IDE of choice's Python interpreter is configured to work in that `conda` environment when you are developing your Python node extension (see [here](https://code.visualstudio.com/docs/python/environments#_work-with-python-interpreters) for help with Visual Studio Code, and [here](https://www.jetbrains.com/help/pycharm/configuring-python-interpreter.html) for PyCharm).

A Python KNIME node needs to implement the `configure` and `execute` methods, so it will generally be a class. The node description is _automatically generated from the docstrings_ of the class and the `execute` method. The node's location in KNIME's _Node Repository_ as well as its icon are specified in the `@knext.node` decorator.

The simplest possible node does nothing but pass an input table to its output unmodified. In the example below, we define a class `MyNode` and indicate that it is a KNIME node by decorating it with `@knext.node`. We then "attach" an input table and an output table to the node by decorating it with `@knext.input_table` and `@knext.output_table` respectively. Finally, we implement the two required methods, `configure` and `execute`.

```python
from typing import List, Tuple
import knime_extension as knext

@knext.node(name="My Node", node_type=knext.NodeType.MANIPULATOR, icon_path="../icons/icon.png", category="/")
@knext.input_table(name="Input Data", description="The data to process in my node")
@knext.output_table("Output Data", "Result of processing in my node")
class MyNode:
    """
    Node description which will be displayed in KAP.
    """
    def configure(self, config_context, input_table_schema):
        return input_table_schema

    def execute(self, exec_context, input_table)
        return input_table
```

`@knext.node`'s configuration options are:

- __name__: the name of the node in KNIME.
- __node_type__: the type of the node, one of:
    - `knext.NodeType.MANIPULATOR`
    - `knext.NodeType.LEARNER`
    - `knext.NodeType.PREDICTOR`
    - `knext.NodeType.SOURCE`
    - `knext.NodeType.SINK`
    - `knext.NodeType.VISUALIZER`
- __icon_path__: module-relative path to a 16x16 pixel PNG file to use as icon.
- __category__: defines the path to the node inside KNIME's _Node Repository_.

### Node port configuration

The input and output ports of a node can be configured by decorating the node class with `@knext.input_table`, `@knext.input_binary`, and respectively `@knext.output_table` and `@knext.output_binary`. 

These port decorators have the following properties:

- they take `name` and `description` arguments, which will be displayed in the node description area inside KNIME;
- they must be positioned _after_ the `@knext.node` decorator and _before_ the decorated object (e.g. the node class);
- their order determines the order of the port connectors of the node in KNIME. 

The `@knext.input_table` and `@knext.output_table` decorators configure the port to consume and respectively produce a KNIME table. 

If you want to receive or send other data, e.g. a trained machine learning model, use `@knext.input_binary` and `@knext.output_binary`. This decorator _has an additional argument_, `id`, used to identify the type of data going along this port connection. Only ports with equal `id` can be connected, and it is good practice to use your domain in reverse to prevent `id` clashes with other node extensions. The data is expected to have type `bytes`.

The port configuration determines the expected signature of the `configure` and `execute` methods:

- In the `configure` method, the first argument is a `ConfigurationContext`, followed by one argument per input port. The method is expected to return __as many parameters as it has output ports configured__. The argument and return value types corresponding to the input and output ports are:

    - for __table__ ports, the argument/return value must be of type `knext.Schema`;
    - for __binary__ ports, the argument/return value must be of type `knext.BinaryPortObjectSpec`.

    Note that the order of the arguments and return values must match the order of the input and output port declarations via the decorators.

- The arguments and expected return values of the `execute` method follow the same schema: one argument per input port, one return value per output port. 

Here is an example with two input ports and one output port.

```python
@knext.node("My Predictor", node_type=knext.NodeType.PREDICTOR, icon_path="icon.png", category="/")
@knext.input_binary("Trained Model", "Trained fancy machine learning model", id="org.example.my.model")
@knext.input_table("Data", "The data on which to predict")
@knext.output_table("Output", "Resulting table")
class MyPredictor():
    def configure(self, config_context, binary_input_spec, table_schema):
        # We will add one column of type double to the table
        return table_schema.append(knext.Column(knext.double(), "Predictions"))
    
    def execute(self, exec_context, trained_model, input_table):
        model = self._load_model_from_bytes(trained_model)
        predictions = model.predict(input_table.to_pandas())
        output_table = input_table
        output_table["Predictions"] = predictions
        return knext.Table.from_pandas(output_table)
    
    def _load_model_from_bytes(self, data):
        return pickle.loads(data)
```

> Alternatively, you can populate the `input_ports` and `output_ports` attributes of your node class (on class or instance level) for more fine grained control.

### Defining the node's configuration dialog

> Note: for the sake of brevity, in the following code snippets we omit repetitive portions of the code whose utility has already been established and demonstrated earlier.

In order to add parameterization to your node's functionality, we can define and customize its configuration dialog. The user-configurable parameters that will be displayed there, and whose values can be accessed inside the `execute` method of the node via `self.param_name`, are set up using a list of parameter types that have been predefined:

* `knext.IntParameter` for integer numbers:

    - Signature:
    ```python
    knext.IntParameter(
        label=None,
        description=None,
        default_value=0,
        min_value=None,
        max_value=None,
    )
    ```
    - Definition within a node/parameter group class:
    ```python
    no_steps = knext.IntParameter("Number of steps", "The number of repetition steps.", 10, max_value=50)
    ```
    - Usage within the `execute` method of the node class:
    ```python
    for i in range(self.no_steps):
        # do something
    ```

* `knext.DoubleParameter` for floating point numbers:

    - Signature:
    ```python
    knext.DoubleParameter(
        label=None,
        description=None,
        default_value=0.0,
        min_value=None,
        max_value=None,
    )
    ```
    - Definition within a node/parameter group class:
    ```python
    learning_rate = knext.DoubleParameter("Learning rate", "The learning rate for Adam.", 0.003, min_value=0.)
    ```
    - Usage within the `execute` method of the node class:
    ```python
    optimizer = torch.optim.Adam(lr=self.learning_rate)
    ```

* `knext.StringParameter` for string parameters and single-choice selections:

    - Signature:
    ```python
    knext.StringParameter(
        label=None,
        description=None,
        default_value:"",
        enum: List[str] = None
    )
    ```
    - Definition within a node/parameter group class:
    ```python
    # as a text input field
    search_term = knext.StringParameter("Search term", "The string to search for in the text.", "")

    # as a single-choice selection
    selection_param = knext.StringParameter("Selection", "The options to choose from.", "A", enum=["A", "B", "C", "D"])
    ```
    - Usage within the `execute` method of the node class:
    ```python
    table[table["str_column"].str.contains(self.search_term)]
    ```

* `knext.BoolParameter` for boolean parameters:

    - Signature:
    ```python
    knext.BoolParameter(
        label=None,
        description=None,
        default_value:False
    )
    ```
    - Definition within a node/parameter group class:
    ```python
    output_image = knext.BoolParameter("Enable image output", "Option to output the node view as an image.", False)
    ```
    - Usage within the `execute` method of the node class:
    ```python
    if self.output_image is True:
        # generate an image of the plot
    ```

* `knext.ColumnParameter` for a single column selection:

    - Signature:
    ```python
    knext.ColumnParameter(
        label=None,
        description=None,
        port_index=0, # the port from which to source the input table
        column_filter: Callable[[knext.Column], bool] = None, # a (lambda) function to filter columns
        include_row_key=False, # whether to include the table Row ID column in the list of selectable columns
        include_none_column=False # whether to enable None as a selectable option, which returns "<none>"
    )
    ```
    - Definition within a node/parameter group class:
    ```python
    selected_col = knext.ColumnParameter(
        "Target column",
        "Select the column containing country codes.",
        column_filter= lambda col: True if "country" in col.name else False,
        include_row_key=False,
        include_none_column=True
    )
    ```
    - Usage within the `execute` method of the node class:
    ```python
    if self.selected_column != "<none>":
        column = input_table[self.selected_column]
        # do something with the column
    ```

* `knext.MultiColumnParameter` for a multiple column selection

    - Signature:
    ```python
    knext.MultiColumnParameter(
        label=None,
        description=None,
        port_index=0, # the port from which to source the input table
        column_filter: Callable[[knext.Column], bool] = None, # a (lambda) function to filter columns
    )
    ```
    - Definition within a node/parameter group class:
    ```python
    selected_columns = knext.MultiColumnParameter(
        "Filter columns",
        "Select the columns that should be filtered out."
    )
    ```
    - Usage within the `execute` method of the node class:
    ```python
    for col_name in self.selected_columns:
        # drop the column from the table
    ```

All of the above have arguments `label` and `description`, which are displayed in the node description in KNIME, as well as in the configuration dialog itself.

Parameters are defined in the form of class attributes inside the node class definition (similar to Python [descriptors](https://docs.python.org/3/howto/descriptor.html)):

```python
@knext.node(…)
…
class MyNode:
    num_repetitions = knext.IntParameter(
        label="Number of repetitions",
        description="How often to repeat an action",
        default_value=42
    )

    def configure(…):
        …

    def execute(…):
        …
```

While each parameter type listed above has default type validation, they also support custom validation via a property-like decorator notation. By wrapping a function that receives a tentative parameter value, and raises an exception should some condition be violated, with the `@some_param.validator` decorator, you are able to add an additional layer of validation to the parameter `some_param`. This should be done _below_ the definition of the parameter for which you are adding a validator, and _above_ the `configure` and `execute` methods:

```python
@knext.node(…)
…
class MyNode:
    num_repetitions = knext.IntParameter(
        label="Number of repetitions",
        description="How often to repeat an action",
        default_value=42
    )

    @num_repetitions.validator
    def validate_reps(value):
        if value > 100:
            raise ValueError("Too many repetitions!")

    def configure(…):
        …

    def execute(…):
        …
```

It is also possible to define groups of parameters, which are displayed as separate sections in the configuration dialog UI. By using the `@knext.parameter_group` decorator with a [dataclass](https://docs.python.org/3/library/dataclasses.html)-like class definition, you are able to encapsulate parameters and, optionally, their validators into a separate entity outside of the node class definition, keeping your code clean and maintainable. A parameter group is linked to a node just like an individual parameter would be:

```python
@knext.parameter_group(label="My Settings")
class MySettings:
    name = knext.StringParameter("Name", "The name of the person", "Bario")
    
    num_repetitions = knext.IntParameter("NumReps", "How often do we repeat?", 1, min_value=1)
    
    @num_repetitions.validator
    def reps_validator(value):
        if value == 2:
            raise ValueError("I don't like the number 2")


@knext.node(…)
…
class MyNodeWithSettings:
    settings = MySettings()
    
    def configure(…):
        …

    def execute(…):
        …
```

Another benefit of defining parameter groups is the ability to provide group validation. As opposed to only being able to validate a single value when attaching a validator to a parameter, group validators have access to the values of all parameters contained in the group, allowing for more complex validation routines.

We provide two ways of defining a group validator, with the `values` argument being a dictionary of `parameter_name` : `parameter_value` mappings:

1. by implementing a `validate(self, values)` method inside the parameter group class definition:
    ```python
    @knext.parameter_group(label="My Group")
    class MyGroup:
        first_param = knext.IntParameter("Simple Int", "Testing a simple int param", 42)

        second_param = knext.StringParameter("Simple String", "Testing a simple string param", "foo")

        def validate(self, values):
            if values["first_param"] < len(values["second_param"]):
                raise ValueError("Params are unbalanced!")
    ```
2. by using the familiar `@group_name.validator` decorator notation with a validator function inside the class definition of the "parent" of the group:
    ```python
    @knext.parameter_group(label="My Group")
    class MyGroup:
        first_param = knext.IntParameter("Simple Int", "Testing a simple int param", 42)

        second_param = knext.StringParameter("Simple String", "Testing a simple string param", "foo")


    @knext.node(…)
    …
    class MyNode:
        param_group = MyGroup()

        @param_group.validator
        def validate_param_group(values):
            if values["first_param"] < len(values["second_param"]):
                raise ValueError("Params are unbalanced!")
    ```

> NOTE: if you define a validator using the first method, and then define another validator for the same group using the second method, the second validator will __override__ the first validator. If you would like to keep __both__ validators active, you can pass the optional `override=False` argument to the decorator: `@param_group.validator(override=False)`.

Intuitively, parameter groups can be nested inside other parameter groups, and their parameter values accessed during the parent group's validation:

```python
@knext.parameter_group(label="Inner Group")
class InnerGroup:
    inner_int = knext.IntParameter("Inner Int", "The inner int param", 1)

@knext.parameter_group(label="Outer Group")
class OuterGroup:
    outer_int = knext.IntParameter("Outer Int", "The outer int param", 2)
    inner_group = InnerGroup()

    def validate(self, values):
        if values["inner_group"]["inner_int"] > values["outer_int"]:
            raise ValueError("The inner int should not be larger than the outer!")
```

<!-- ```python
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
``` -->

### Node view declaration

You can use the `@knext.view(name="", description="")` decorator to specify that a node returns a view. 
In that case, the `execute` method should return a tuple of port outputs and the view (of type `knime_views.NodeView`). The package `knime_views` contains utilities to create node views from different kinds of objects.

```python
from typing import List
import knime_extension as knext
import knime_views as kv
import seaborn as sns


@knext.node(name="My Node", node_type=knext.NodeType.VISUALIZER, icon_path="icon.png", category="/")
@knext.input_table(name="Input Data", description="We read data from here")
@knext.view(name="My pretty view", description="Showing a seaborn plot")
class MyViewNode:
    """
    A view node

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