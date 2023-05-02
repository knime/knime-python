import unittest
import knime.extension.parameter as kp
import knime.api.schema as ks
import knime.extension.nodes as kn


def generate_values_dict(
    int_param=3,
    double_param=1.5,
    string_param="foo",
    bool_param=True,
    column_param="foo_column",
    multi_column_param=["foo_column", "bar_column"],
    first=1,
    second=5,
    third=3,
):
    return {
        "model": {
            "int_param": int_param,
            "double_param": double_param,
            "string_param": string_param,
            "bool_param": bool_param,
            "column_param": column_param,
            "multi_column_param": multi_column_param,
            "parameter_group": {
                "subgroup": {"first": first, "second": second},
                "third": third,
            },
        }
    }


def generate_values_dict_without_groups(
    int_param=3,
    double_param=1.5,
    string_param="foo",
    bool_param=True,
    column_param="foo_column",
    multi_column_param=["foo_column", "bar_column"],
):
    return {
        "model": {
            "int_param": int_param,
            "double_param": double_param,
            "string_param": string_param,
            "bool_param": bool_param,
            "column_param": column_param,
            "multi_column_param": multi_column_param,
        }
    }


def generate_values_dict_with_one_group(
    int_param=3,
    double_param=1.5,
    string_param="foo",
    bool_param=True,
    column_param="foo_column",
    multi_column_param=["foo_column", "bar_column"],
    first=3,
    second=5,
):
    return {
        "model": {
            "int_param": int_param,
            "double_param": double_param,
            "string_param": string_param,
            "bool_param": bool_param,
            "column_param": column_param,
            "multi_column_param": multi_column_param,
            "parameter_group": {
                "first": first,
                "second": second,
            },
        }
    }


def generate_values_dict_for_group_w_custom_method(
    outer_int_param=0, middle_int_param=1, inner_int_param=2, inner_int_param2=2
):
    return {
        "model": {
            "outer_group": {
                "outer_int_param": outer_int_param,
                "middle_group": {
                    "middle_int_param": middle_int_param,
                    "inner_group": {
                        "inner_int_param": inner_int_param,
                        "inner_int_param2": inner_int_param2,
                    },
                },
            }
        }
    }


def set_column_parameters(parameterized_object):
    parameterized_object.column_param = "foo_column"
    parameterized_object.multi_column_param = ["foo_column", "bar_column"]


def generate_versioned_values_dict(
    int_param=3,
    double_param=1.5,
    string_param="foo",
    bool_param=True,
    first=1,
    second=5,
    extension_version=None,
):
    model = {}
    if extension_version is None:
        model = {
            "int_param": int_param,
            "double_param": double_param,
            "string_param": string_param,
            "bool_param": bool_param,
            "group": {"first": first, "second": second},
        }
    else:
        if extension_version == "0.1.0":
            model = {
                "int_param": int_param,
                "double_param": double_param,
            }
        elif extension_version == "0.2.0":
            model = {
                "int_param": int_param,
                "double_param": double_param,
                "string_param": string_param,
                "group": {"first": first},
            }
        elif extension_version == "0.3.0":
            model = {
                "int_param": int_param,
                "double_param": double_param,
                "string_param": string_param,
                "bool_param": bool_param,
                "group": {"first": first, "second": second},
            }
    return {"model": model}


def generate_versioned_schema_dict(extension_version):
    if extension_version == "0.1.0":
        return {
            "type": "object",
            "properties": {
                "model": {
                    "type": "object",
                    "properties": {
                        "int_param": {
                            "title": "Int Parameter",
                            "description": "An integer parameter",
                            "type": "integer",
                            "format": "int32",
                        },
                        "double_param": {
                            "title": "Double Parameter",
                            "description": "A double parameter",
                            "type": "number",
                            "format": "double",
                        },
                    },
                }
            },
        }
    elif extension_version == "0.2.0":
        return {
            "type": "object",
            "properties": {
                "model": {
                    "type": "object",
                    "properties": {
                        "int_param": {
                            "title": "Int Parameter",
                            "description": "An integer parameter",
                            "type": "integer",
                            "format": "int32",
                        },
                        "double_param": {
                            "title": "Double Parameter",
                            "description": "A double parameter",
                            "type": "number",
                            "format": "double",
                        },
                        "string_param": {
                            "title": "String Parameter",
                            "description": "A string parameter",
                            "type": "string",
                        },
                        "group": {
                            "type": "object",
                            "properties": {
                                "first": {
                                    "title": "First Parameter",
                                    "description": "First parameter description",
                                    "type": "integer",
                                    "format": "int32",
                                }
                            },
                        },
                    },
                }
            },
        }
    elif extension_version == "0.3.0":
        return {
            "type": "object",
            "properties": {
                "model": {
                    "type": "object",
                    "properties": {
                        "int_param": {
                            "title": "Int Parameter",
                            "description": "An integer parameter",
                            "type": "integer",
                            "format": "int32",
                        },
                        "double_param": {
                            "title": "Double Parameter",
                            "description": "A double parameter",
                            "type": "number",
                            "format": "double",
                        },
                        "string_param": {
                            "title": "String Parameter",
                            "description": "A string parameter",
                            "type": "string",
                        },
                        "bool_param": {
                            "title": "Boolean Parameter",
                            "description": "A boolean parameter",
                            "type": "boolean",
                        },
                        "group": {
                            "type": "object",
                            "properties": {
                                "first": {
                                    "title": "First Parameter",
                                    "description": "First parameter description",
                                    "type": "integer",
                                    "format": "int32",
                                },
                                "second": {
                                    "title": "Second Parameter",
                                    "description": "Second parameter description",
                                    "type": "integer",
                                    "format": "int32",
                                },
                            },
                        },
                    },
                }
            },
        }


def generate_versioned_ui_schema_dict(extension_version):
    if extension_version == "0.1.0":
        return {
            "type": "VerticalLayout",
            "elements": [
                {
                    "type": "Control",
                    "label": "Int Parameter",
                    "scope": "#/properties/model/properties/int_param",
                    "options": {"format": "integer"},
                },
                {
                    "type": "Control",
                    "label": "Double Parameter",
                    "scope": "#/properties/model/properties/double_param",
                    "options": {"format": "number"},
                },
            ],
        }
    elif extension_version == "0.2.0":
        return {
            "type": "VerticalLayout",
            "elements": [
                {
                    "type": "Control",
                    "label": "Int Parameter",
                    "scope": "#/properties/model/properties/int_param",
                    "options": {"format": "integer"},
                },
                {
                    "type": "Control",
                    "label": "Double Parameter",
                    "scope": "#/properties/model/properties/double_param",
                    "options": {"format": "number"},
                },
                {
                    "type": "Control",
                    "label": "String Parameter",
                    "scope": "#/properties/model/properties/string_param",
                    "options": {"format": "string"},
                },
                {
                    "type": "Section",
                    "label": "Versioned parameter group",
                    "options": {},
                    "elements": [
                        {
                            "type": "Control",
                            "label": "First Parameter",
                            "scope": "#/properties/model/properties/group/properties/first",
                            "options": {"format": "integer"},
                        }
                    ],
                },
            ],
        }
    elif extension_version == "0.3.0":
        return {
            "type": "VerticalLayout",
            "elements": [
                {
                    "type": "Control",
                    "label": "Int Parameter",
                    "scope": "#/properties/model/properties/int_param",
                    "options": {"format": "integer"},
                },
                {
                    "type": "Control",
                    "label": "Double Parameter",
                    "scope": "#/properties/model/properties/double_param",
                    "options": {"format": "number"},
                },
                {
                    "type": "Control",
                    "label": "String Parameter",
                    "scope": "#/properties/model/properties/string_param",
                    "options": {"format": "string"},
                },
                {
                    "type": "Control",
                    "label": "Boolean Parameter",
                    "scope": "#/properties/model/properties/bool_param",
                    "options": {"format": "boolean"},
                },
                {
                    "type": "Section",
                    "label": "Versioned parameter group",
                    "options": {},
                    "elements": [
                        {
                            "type": "Control",
                            "label": "First Parameter",
                            "scope": "#/properties/model/properties/group/properties/first",
                            "options": {"format": "integer"},
                        },
                        {
                            "type": "Control",
                            "label": "Second Parameter",
                            "scope": "#/properties/model/properties/group/properties/second",
                            "options": {"format": "integer"},
                        },
                    ],
                },
            ],
        }


#### Primary parameterised object and its groups for testing main functionality: ####
@kp.parameter_group("Subgroup")
class NestedParameterGroup:
    """
    A parameter group where the sum of the parameters may not exceed 10.
    Is a subgroup of an external parameter group.
    """

    first = kp.IntParameter(
        label="First Parameter",
        description="First parameter description",
        default_value=1,
    )
    second = kp.IntParameter(
        label="Second Parameter",
        description="Second parameter description",
        default_value=5,
    )

    def validate(self, values):
        if values["first"] + values["second"] > 100:
            raise ValueError(
                "The sum of the parameters of subgroup must not exceed 100."
            )


@kp.parameter_group("Primary Group")
class ParameterGroup:
    """A parameter group which contains a parameter group as a subgroup, and sets a custom validator for a parameter."""

    subgroup = NestedParameterGroup()
    third = kp.IntParameter(
        label="Internal int Parameter",
        description="Internal int parameter description",
        default_value=3,
    )

    @third.validator
    def int_param_validator(value):
        if value < 0:
            raise ValueError("The third parameter must be positive.")

    @subgroup.validator(override=False)
    def validate_subgroup(values, version=None):
        if values["first"] + values["second"] < 0:
            raise ValueError("The sum of the parameters must be non-negative.")
        elif values["first"] == 42:
            raise ValueError("Detected a forbidden number.")


@kp.parameter_group("Primary Group Advanced", is_advanced=True)
class ParameterGroupAdvanced:
    """A parameter group which contains a parameter group as a subgroup, has is_advanced set True,
    and sets a custom validator for a parameter."""

    subgroup = NestedParameterGroup()
    third = kp.IntParameter(
        label="Internal int Parameter",
        description="Internal int parameter description",
        default_value=3,
    )

    @third.validator
    def int_param_validator(value):
        if value < 0:
            raise ValueError("The third parameter must be positive.")

    @subgroup.validator(override=False)
    def validate_subgroup(values, version=None):
        if values["first"] + values["second"] < 0:
            raise ValueError("The sum of the parameters must be non-negative.")
        elif values["first"] == 42:
            raise ValueError("Detected a forbidden number.")


class Parameterized:
    int_param = kp.IntParameter("Int Parameter", "An integer parameter", 3)
    double_param = kp.DoubleParameter("Double Parameter", "A double parameter", 1.5)
    string_param = kp.StringParameter("String Parameter", "A string parameter", "foo")
    bool_param = kp.BoolParameter("Boolean Parameter", "A boolean parameter", True)
    column_param = kp.ColumnParameter("Column Parameter", "A column parameter")
    multi_column_param = kp.MultiColumnParameter(
        "Multi Column Parameter",
        "A multi column parameter",
    )

    parameter_group = ParameterGroup()

    @string_param.validator
    def validate_string_param(value):
        if len(value) > 10:
            raise ValueError(f"Length of string must not exceed 10!")


class ParameterizedWithOneGroup:
    int_param = kp.IntParameter("Int Parameter", "An integer parameter", 3)
    double_param = kp.DoubleParameter("Double Parameter", "A double parameter", 1.5)
    string_param = kp.StringParameter("String Parameter", "A string parameter", "foo")
    bool_param = kp.BoolParameter("Boolean Parameter", "A boolean parameter", True)
    column_param = kp.ColumnParameter("Column Parameter", "A column parameter")
    multi_column_param = kp.MultiColumnParameter(
        "Multi Column Parameter",
        "A multi column parameter",
    )

    parameter_group = NestedParameterGroup()

    @string_param.validator
    def validate_string_param(value):
        if len(value) > 10:
            raise ValueError(f"Length of string must not exceed 10!")


class ParameterizedWithAdvancedOption:
    int_param = kp.IntParameter("Int Parameter", "An integer parameter", 3)
    int_advanced_param = kp.IntParameter(
        "Int Parameter", "An integer parameter", 3, is_advanced=True
    )

    double_param = kp.DoubleParameter("Double Parameter", "A double parameter", 1.5)
    double_advanced_param = kp.DoubleParameter(
        "Double Parameter", "A double parameter", 1.5, is_advanced=True
    )

    string_param = kp.StringParameter("String Parameter", "A string parameter", "foo")
    string_advanced_param = kp.StringParameter(
        "String Parameter", "A string parameter", "foo", is_advanced=True
    )

    bool_param = kp.BoolParameter("Boolean Parameter", "A boolean parameter", True)
    bool_advanced_param = kp.BoolParameter(
        "Boolean Parameter", "A boolean parameter", True, is_advanced=True
    )

    column_param = kp.ColumnParameter("Column Parameter", "A column parameter")
    column_advanced_param = kp.ColumnParameter(
        "Column Parameter", "A column parameter", is_advanced=True
    )

    multi_column_param = kp.MultiColumnParameter(
        "Multi Column Parameter",
        "A multi column parameter",
    )
    multi_column_advanced_param = kp.MultiColumnParameter(
        "Multi Column Parameter", "A multi column parameter", is_advanced=True
    )

    parameter_group = ParameterGroup()
    parameter_group_advanced = ParameterGroupAdvanced()


class ParameterizedWithoutGroup:
    int_param = kp.IntParameter("Int Parameter", "An integer parameter", 3)
    double_param = kp.DoubleParameter("Double Parameter", "A double parameter", 1.5)
    string_param = kp.StringParameter("String Parameter", "A string parameter", "foo")
    bool_param = kp.BoolParameter("Boolean Parameter", "A boolean parameter", True)
    column_param = kp.ColumnParameter("Column Parameter", "A column parameter")
    multi_column_param = kp.MultiColumnParameter(
        "Multi Column Parameter",
        "A multi column parameter",
    )


#### Secondary parameterised objects for testing composition: ####
@kp.parameter_group("Parameter group to be used for multiple descriptor instances.")
class ReusableGroup:
    first_param = kp.IntParameter(
        label="Plain int param",
        description="Description of the plain int param.",
        default_value=12345,
    )
    second_param = kp.IntParameter(
        label="Second int param",
        description="Description of the second plain int param.",
        default_value=54321,
    )

    @classmethod
    def create_default_dict(cls):
        return {"first_param": 12345, "second_param": 54321}


class ComposedParameterized:
    def __init__(self) -> None:
        # Instantiated here for brevety. Usually these would be supplied as arguments to __init__
        self.first_group = ReusableGroup()
        self.second_group = ReusableGroup()


@kp.parameter_group("Nested composed")
class NestedComposed:
    def __init__(self) -> None:
        self.first_group = ReusableGroup()
        self.second_group = ReusableGroup()

    @classmethod
    def create_default_dict(cls):
        return {
            "first_group": ReusableGroup.create_default_dict(),
            "second_group": ReusableGroup.create_default_dict(),
        }


class NestedComposedParameterized:
    def __init__(self) -> None:
        self.group = NestedComposed()

    @classmethod
    def create_default_dict(cls):
        return {"model": {"group": NestedComposed.create_default_dict()}}


@kp.parameter_group("Nested group")
class NestedNestedParameters:
    nested_root_param = kp.StringParameter(
        "Nested root param",
        "Nested root param.",
        "blah",
    )

    def __init__(self, new_value):
        self.nested_init_param = kp.StringParameter(
            "Nested init param",
            "Nested init param.",
            new_value,
        )


@kp.parameter_group("Root group")
class NestedParameters:
    group_root_param = kp.StringParameter(
        "Group root param",
        "Group root param.",
        "blah",
    )

    group_root_nested = NestedNestedParameters("blah")

    def __init__(self, new_value):
        self.new_constructor_param = kp.StringParameter(
            "Root group init param",
            "Root group init param.",
            new_value,
        )
        self.group_init_nested = NestedNestedParameters("blah")


class ComplexNestedComposedParameterized:
    root_param = kp.StringParameter("Root param", "Root param.", "blah")

    root_group = NestedParameters("blah")

    def __init__(self):
        self.init_group = NestedParameters("blah")

    @classmethod
    def get_expected_params(cls):
        return {
            "model": {
                "root_param": "blah",
                "root_group": {
                    "group_root_param": "blah",
                    "group_root_nested": {
                        "nested_root_param": "blah",
                        "nested_init_param": "blah",
                    },
                    "new_constructor_param": "blah",
                    "group_init_nested": {
                        "nested_root_param": "blah",
                        "nested_init_param": "blah",
                    },
                },
                "init_group": {
                    "group_root_param": "blah",
                    "group_root_nested": {
                        "nested_root_param": "blah",
                        "nested_init_param": "blah",
                    },
                    "new_constructor_param": "blah",
                    "group_init_nested": {
                        "nested_root_param": "blah",
                        "nested_init_param": "blah",
                    },
                },
            }
        }


@kp.parameter_group("Versioned parameter group")
class VersionedParameterGroup:
    first = kp.IntParameter(
        label="First Parameter",
        description="First parameter description",
        default_value=1,
        since_version="0.2.0",
    )
    second = kp.IntParameter(
        label="Second Parameter",
        description="Second parameter description",
        default_value=5,
        since_version="0.3.0",
    )


class VersionedParameterized:
    # no since_version specified defaults to it being "0.0.0"
    int_param = kp.IntParameter("Int Parameter", "An integer parameter", 3)
    double_param = kp.DoubleParameter(
        "Double Parameter", "A double parameter", 1.5, since_version="0.1.0"
    )
    string_param = kp.StringParameter(
        "String Parameter", "A string parameter", "foo", since_version="0.2.0"
    )
    bool_param = kp.BoolParameter(
        "Boolean Parameter", "A boolean parameter", True, since_version="0.3.0"
    )

    group = VersionedParameterGroup(since_version="0.2.0")


@kp.parameter_group("", since_version="0.2.0")
class VersionedDefaultsParameterGroup:
    first = kp.IntParameter(
        "",
        "",
        lambda v: -1 if v < kp.Version(0, 1, 0) else 1,
    )


class VersionedDefaultsParameterized:
    int_param = kp.IntParameter("Int Parameter", "An integer parameter", 3)

    double_param = kp.DoubleParameter(
        "Double Parameter",
        "",
        lambda v: 1.5 if v >= kp.Version(0, 1, 0) else 0.5,
        since_version="0.2.0",
    )

    group = VersionedDefaultsParameterGroup()


#### Parameterised object for testing parameter groups with additional methods defined by the developer ####
@kp.parameter_group("Inner group with a custom method")
class InnerGroupWCustomMethod:
    inner_int_param = kp.IntParameter("Inner int", "Inner int parameter", 2)
    inner_int_param2 = kp.IntParameter(
        "Second inner int", "Second inner int parameter", 2
    )

    def inner_foo(self):
        return self.inner_int_param + self.inner_int_param2  # 2 + 2 = 4

    def validate(self, values):
        if values["inner_int_param"] != values["inner_int_param2"]:
            raise ValueError("Inner int parameters should always be equal.")


@kp.parameter_group("Middle group with a custom method")
class MiddleGroupWCustomMethod:
    middle_int_param = kp.IntParameter("Middle int", "Middle int parameter", 1)
    inner_group = InnerGroupWCustomMethod()

    def middle_foo(self):
        return self.middle_int_param + self.inner_group.inner_foo()  # 1 + (2 + 2) = 5


@kp.parameter_group("Outer group with a custom method")
class OuterGroupWCustomMethod:
    outer_int_param = kp.IntParameter("Outer int", "Outer int parameter")
    middle_group = MiddleGroupWCustomMethod()

    def recursive_method(self, bar):
        if bar > 4:
            return self.middle_group.middle_foo()  # 5
        else:
            return 1 + self.recursive_method(
                bar + 1
            )  # 1 + (1 + (1 + (1 + (1 + 5)))) = 10

    def nested_method(self):
        return self.middle_group.inner_group.inner_foo()  # 4

    def _protected_method(self):
        return "bar"

    @middle_group.validator
    def validate_middle_group(values):
        if values["middle_int_param"] > values["inner_group"]["inner_int_param"]:
            raise ValueError(
                "Middle parameter should be smaller than inner int parameter."
            )


class ParameterizedWithCustomMethods:
    outer_group = OuterGroupWCustomMethod()


@kp.parameter_group(
    "Subgroup that assigns values to params inside its constructor call"
)
class SubgroupWithConstructor:
    inner_param = kp.IntParameter("Param", "Param description.", 12345)

    def __init__(self, param):
        self.inner_param = param


@kp.parameter_group("Group that assigns values to params inside its constructor call")
class GroupWithConstructor:
    param = kp.IntParameter("Param", "Param description.", 12345)

    def __init__(self, param):
        self.param = param

        # a non-descriptor group
        self.subgroup = SubgroupWithConstructor(param=69)


class ParameterizedUsingConstructor:
    group = GroupWithConstructor(param=42)


@kp.parameter_group("Subgroup with an inner class.")
class SubgroupWithInnerClass:
    class InnerClass:
        attr_1 = 1
        attr_2 = 100

        @classmethod
        def _some_inner_method(cls):
            return cls.attr_2

    inner_param = kp.IntParameter("Inner param", "Inner param description.", 12345)

    def __init__(self, param=InnerClass._some_inner_method()):
        self.inner_param = param + self.InnerClass.attr_1


@kp.parameter_group("Group that calls its subgroup's inner class methods")
class GroupCallingInnerClass:
    param = kp.IntParameter("Outer param", "Outer param description.", 54321)

    # will call the InnerClass method as the default value in the constructor
    subgroup_1 = SubgroupWithInnerClass()

    def __init__(self, param):
        self.param = param

        # will call the InnerClass attribute to be added to the provided value
        self.subgroup_2 = SubgroupWithInnerClass(param=69)


class ParameterizedWithInnerClassGroups:
    group = GroupCallingInnerClass(param=SubgroupWithInnerClass.InnerClass.attr_1 + 41)


class ParameterizedWithDialogCreationContext:
    credential_param = kp.StringParameter(
        label="Credential param",
        description="Choices is a callable",
        choices=lambda a: kn.DialogCreationContext.get_credential_names(a),
    )
    flow_variable_param = kp.StringParameter(
        label="Flow variable param",
        description="Call it a choice",
        choices=lambda a: kn.DialogCreationContext.get_flow_variables(a),
    )


class DummyDialogCreationContext:
    class DummyJavaContext:
        def get_credential_names(self):
            return ["foo", "bar", "baz"]

        def get_credential(self, name):
            return "dummy"

    _java_ctx = DummyJavaContext()
    _flow_variables = ["flow1", "flow2", "flow3"]


#### Tests: ####
class ParameterTest(unittest.TestCase):
    def setUp(self):
        self.parameterized = Parameterized()
        self.parameterized_advanced_option = ParameterizedWithAdvancedOption()
        self.versioned_parameterized = VersionedParameterized()
        self.parameterized_without_group = ParameterizedWithoutGroup()
        self.parameterized_with_custom_methods = ParameterizedWithCustomMethods()
        self.parameterized_with_dialog_creation_context = (
            ParameterizedWithDialogCreationContext()
        )

        self.maxDiff = None

    def test_forbidden_keywords_not_allowed(self):
        with self.assertRaises(SyntaxError):
            # we define these inside the test case since the error should be caused
            # when the parameter group class is initially parsed.
            @kp.parameter_group("Group with forbidden keyword arguments.")
            class GroupWithForbiddenKwargs:
                param = kp.IntParameter("Simple param", "Simple param.", 1)

                def __init__(self, since_version="foo", normal_arg=1):
                    self.param = normal_arg

            class ParameterizedWithForbiddenKwargs:
                group = GroupWithForbiddenKwargs("bar", 10)

    def test_inner_classes_are_accessible(self):
        obj = ParameterizedWithInnerClassGroups()
        params = kp.extract_parameters(obj)
        expected = {
            "model": {
                "group": {
                    "param": 42,
                    "subgroup_1": {"inner_param": 101},
                    "subgroup_2": {"inner_param": 70},
                }
            }
        }
        self.assertEqual(params, expected)
        self.assertEqual(obj.group.subgroup_1.InnerClass._some_inner_method(), 100)

    def test_parameter_group_constructors_set_values(self):
        obj = ParameterizedUsingConstructor()
        params = kp.extract_parameters(obj)
        expected = {"model": {"group": {"param": 42, "subgroup": {"inner_param": 69}}}}
        self.assertEqual(params, expected)

    def test_inject_with_version_dependent_defaults(self):
        obj = VersionedDefaultsParameterized()
        params = {"model": {"int_param": 5}}

        kp.inject_parameters(obj, params, "0.0.0")
        self.assertEqual(obj.int_param, 5)
        self.assertEqual(obj.double_param, 0.5)
        self.assertEqual(obj.group.first, -1)
        kp.inject_parameters(obj, params, "0.1.0")
        self.assertEqual(obj.int_param, 5)
        self.assertEqual(obj.double_param, 1.5)
        self.assertEqual(obj.group.first, 1)

    def test_init_versioned_default_on_get(self):
        kp.set_extension_version("0.2.0")
        obj = VersionedDefaultsParameterized()
        self.assertEqual(obj.int_param, 3)
        self.assertEqual(obj.double_param, 1.5)
        self.assertEqual(obj.group.first, 1)

    #### Test central functionality: ####
    def test_getting_parameters(self):
        """
        Test that parameter values can be retrieved.
        """
        set_column_parameters(self.parameterized)

        # root-level parameters
        self.assertEqual(self.parameterized.int_param, 3)
        self.assertEqual(self.parameterized.double_param, 1.5)
        self.assertEqual(self.parameterized.string_param, "foo")
        self.assertEqual(self.parameterized.bool_param, True)
        self.assertEqual(self.parameterized.column_param, "foo_column")
        self.assertEqual(
            self.parameterized.multi_column_param, ["foo_column", "bar_column"]
        )

        # group-level parameters
        self.assertEqual(self.parameterized.parameter_group.third, 3)

        # subgroup-level parameters
        self.assertEqual(self.parameterized.parameter_group.subgroup.first, 1)

    def test_setting_parameters(self):
        """
        Test that parameter values can be set.
        """
        # root-level parameters
        self.parameterized.int_param = 5
        self.parameterized.double_param = 5.5
        self.parameterized.string_param = "bar"
        self.parameterized.bool_param = False
        self.parameterized.column_param = "foo_column"
        self.parameterized.multi_column_param = ["foo_column", "bar_column"]

        # group-level parameters
        self.assertEqual(self.parameterized.int_param, 5)
        self.assertEqual(self.parameterized.double_param, 5.5)
        self.assertEqual(self.parameterized.string_param, "bar")
        self.assertEqual(self.parameterized.bool_param, False)
        self.assertEqual(self.parameterized.column_param, "foo_column")
        self.assertEqual(
            self.parameterized.multi_column_param, ["foo_column", "bar_column"]
        )

        # subgroup-level parameters
        self.parameterized.parameter_group.subgroup.first = 2
        self.assertEqual(self.parameterized.parameter_group.subgroup.first, 2)

    def test_extracting_parameters(self):
        """
        Test extracting nested parameter values.
        """
        set_column_parameters(self.parameterized)

        params = kp.extract_parameters(self.parameterized)
        expected = generate_values_dict()
        self.assertEqual(params, expected)

    def test_inject_parameters(self):
        params = generate_values_dict(
            4, 2.7, "bar", False, "foo_column", ["foo_column", "bar_column"], 3, 2, 1
        )

        kp.inject_parameters(self.parameterized, params)
        extracted = kp.extract_parameters(self.parameterized)
        self.assertEqual(params, extracted)

    def test_inject_parameters_with_missing_allowed(self):
        obj = Parameterized()
        params = {"model": {"int_param": 5}}

        kp.inject_parameters(obj, params)

        set_column_parameters(obj)

        extracted = kp.extract_parameters(obj)
        expected = generate_values_dict(5)
        self.assertEqual(expected, extracted)

    def test_extract_schema(self):
        expected = {
            "type": "object",
            "properties": {
                "model": {
                    "type": "object",
                    "properties": {
                        "int_param": {
                            "title": "Int Parameter",
                            "description": "An integer parameter",
                            "type": "integer",
                            "format": "int32",
                        },
                        "double_param": {
                            "title": "Double Parameter",
                            "description": "A double parameter",
                            "type": "number",
                            "format": "double",
                        },
                        "string_param": {
                            "title": "String Parameter",
                            "description": "A string parameter",
                            "type": "string",
                        },
                        "bool_param": {
                            "title": "Boolean Parameter",
                            "description": "A boolean parameter",
                            "type": "boolean",
                        },
                        "column_param": {
                            "title": "Column Parameter",
                            "description": "A column parameter",
                            "oneOf": [{"const": "", "title": ""}],
                        },
                        "multi_column_param": {
                            "title": "Multi Column Parameter",
                            "description": "A multi column parameter",
                            "anyOf": [{"const": "", "title": ""}],
                        },
                        "parameter_group": {
                            "type": "object",
                            "properties": {
                                "subgroup": {
                                    "properties": {
                                        "first": {
                                            "title": "First Parameter",
                                            "description": "First parameter description",
                                            "type": "integer",
                                            "format": "int32",
                                        },
                                        "second": {
                                            "title": "Second Parameter",
                                            "description": "Second parameter description",
                                            "type": "integer",
                                            "format": "int32",
                                        },
                                    },
                                    "type": "object",
                                },
                                "third": {
                                    "title": "Internal int Parameter",
                                    "description": "Internal int parameter description",
                                    "type": "integer",
                                    "format": "int32",
                                },
                            },
                        },
                    },
                }
            },
        }
        extracted = kp.extract_schema(self.parameterized)
        self.assertEqual(expected, extracted)

    def test_extract_dialog_creation_context_parameters(self):
        # test credential names
        expected = {
            "type": "object",
            "properties": {
                "model": {
                    "type": "object",
                    "properties": {
                        "credential_param": {
                            "title": "Credential param",
                            "description": "Choices is a callable",
                            "oneOf": [
                                {"const": "foo", "title": "foo"},
                                {"const": "bar", "title": "bar"},
                                {"const": "baz", "title": "baz"},
                            ],
                        },
                        "flow_variable_param": {
                            "title": "Flow variable param",
                            "description": "Call it a choice",
                            "oneOf": [
                                {"const": "flow1", "title": "flow1"},
                                {"const": "flow2", "title": "flow2"},
                                {"const": "flow3", "title": "flow3"},
                            ],
                        },
                    },
                }
            },
        }
        dummy_dialog = DummyDialogCreationContext()
        extracted = kp.extract_schema(
            self.parameterized_with_dialog_creation_context,
            dialog_creation_context=dummy_dialog,
        )

        self.assertEqual(expected, extracted)

    def test_extract_ui_schema(self):
        expected = {
            "type": "VerticalLayout",
            "elements": [
                {
                    "type": "Control",
                    "label": "Int Parameter",
                    "scope": "#/properties/model/properties/int_param",
                    "options": {"format": "integer"},
                },
                {
                    "type": "Control",
                    "label": "Double Parameter",
                    "scope": "#/properties/model/properties/double_param",
                    "options": {"format": "number"},
                },
                {
                    "type": "Control",
                    "label": "String Parameter",
                    "scope": "#/properties/model/properties/string_param",
                    "options": {"format": "string"},
                },
                {
                    "type": "Control",
                    "label": "Boolean Parameter",
                    "scope": "#/properties/model/properties/bool_param",
                    "options": {"format": "boolean"},
                },
                {
                    "type": "Control",
                    "label": "Column Parameter",
                    "scope": "#/properties/model/properties/column_param",
                    "options": {
                        "format": "columnSelection",
                        "showRowKeys": False,
                        "showNoneColumn": False,
                    },
                },
                {
                    "type": "Control",
                    "label": "Multi Column Parameter",
                    "scope": "#/properties/model/properties/multi_column_param",
                    "options": {"format": "columnFilter"},
                },
                {
                    "type": "Section",
                    "label": "Primary Group",
                    "options": {},
                    "elements": [
                        {
                            "type": "Group",
                            "label": "Subgroup",
                            "options": {},
                            "elements": [
                                {
                                    "type": "Control",
                                    "label": "First Parameter",
                                    "scope": "#/properties/model/properties/parameter_group/properties/subgroup/properties/first",
                                    "options": {"format": "integer"},
                                },
                                {
                                    "type": "Control",
                                    "label": "Second Parameter",
                                    "scope": "#/properties/model/properties/parameter_group/properties/subgroup/properties/second",
                                    "options": {"format": "integer"},
                                },
                            ],
                        },
                        {
                            "type": "Control",
                            "label": "Internal int Parameter",
                            "scope": "#/properties/model/properties/parameter_group/properties/third",
                            "options": {"format": "integer"},
                        },
                    ],
                },
            ],
        }
        extracted = kp.extract_ui_schema(self.parameterized)
        self.assertEqual(expected, extracted)

    def test_extract_ui_schema_is_advanced_option(self):
        expected = {
            "type": "VerticalLayout",
            "elements": [
                {
                    "type": "Control",
                    "label": "Int Parameter",
                    "scope": "#/properties/model/properties/int_param",
                    "options": {"format": "integer"},
                },
                {
                    "type": "Control",
                    "label": "Int Parameter",
                    "scope": "#/properties/model/properties/int_advanced_param",
                    "options": {"format": "integer", "isAdvanced": True},
                },
                {
                    "type": "Control",
                    "label": "Double Parameter",
                    "scope": "#/properties/model/properties/double_param",
                    "options": {"format": "number"},
                },
                {
                    "type": "Control",
                    "label": "Double Parameter",
                    "scope": "#/properties/model/properties/double_advanced_param",
                    "options": {"format": "number", "isAdvanced": True},
                },
                {
                    "type": "Control",
                    "label": "String Parameter",
                    "scope": "#/properties/model/properties/string_param",
                    "options": {"format": "string"},
                },
                {
                    "type": "Control",
                    "label": "String Parameter",
                    "scope": "#/properties/model/properties/string_advanced_param",
                    "options": {"format": "string", "isAdvanced": True},
                },
                {
                    "type": "Control",
                    "label": "Boolean Parameter",
                    "scope": "#/properties/model/properties/bool_param",
                    "options": {"format": "boolean"},
                },
                {
                    "type": "Control",
                    "label": "Boolean Parameter",
                    "scope": "#/properties/model/properties/bool_advanced_param",
                    "options": {"format": "boolean", "isAdvanced": True},
                },
                {
                    "type": "Control",
                    "label": "Column Parameter",
                    "scope": "#/properties/model/properties/column_param",
                    "options": {
                        "format": "columnSelection",
                        "showRowKeys": False,
                        "showNoneColumn": False,
                    },
                },
                {
                    "type": "Control",
                    "label": "Column Parameter",
                    "scope": "#/properties/model/properties/column_advanced_param",
                    "options": {
                        "format": "columnSelection",
                        "showRowKeys": False,
                        "showNoneColumn": False,
                        "isAdvanced": True,
                    },
                },
                {
                    "type": "Control",
                    "label": "Multi Column Parameter",
                    "scope": "#/properties/model/properties/multi_column_param",
                    "options": {"format": "columnFilter"},
                },
                {
                    "type": "Control",
                    "label": "Multi Column Parameter",
                    "scope": "#/properties/model/properties/multi_column_advanced_param",
                    "options": {"format": "columnFilter", "isAdvanced": True},
                },
                {
                    "type": "Section",
                    "label": "Primary Group",
                    "options": {},
                    "elements": [
                        {
                            "type": "Group",
                            "label": "Subgroup",
                            "options": {},
                            "elements": [
                                {
                                    "type": "Control",
                                    "label": "First Parameter",
                                    "scope": "#/properties/model/properties/parameter_group/properties/subgroup/properties/first",
                                    "options": {"format": "integer"},
                                },
                                {
                                    "type": "Control",
                                    "label": "Second Parameter",
                                    "scope": "#/properties/model/properties/parameter_group/properties/subgroup/properties/second",
                                    "options": {"format": "integer"},
                                },
                            ],
                        },
                        {
                            "type": "Control",
                            "label": "Internal int Parameter",
                            "scope": "#/properties/model/properties/parameter_group/properties/third",
                            "options": {"format": "integer"},
                        },
                    ],
                },
                {
                    "type": "Section",
                    "label": "Primary Group Advanced",
                    "options": {
                        "isAdvanced": True,
                    },
                    "elements": [
                        {
                            "type": "Group",
                            "label": "Subgroup",
                            "options": {},
                            "elements": [
                                {
                                    "type": "Control",
                                    "label": "First Parameter",
                                    "scope": "#/properties/model/properties/parameter_group_advanced/properties/subgroup/properties/first",
                                    "options": {"format": "integer"},
                                },
                                {
                                    "type": "Control",
                                    "label": "Second Parameter",
                                    "scope": "#/properties/model/properties/parameter_group_advanced/properties/subgroup/properties/second",
                                    "options": {"format": "integer"},
                                },
                            ],
                        },
                        {
                            "type": "Control",
                            "label": "Internal int Parameter",
                            "scope": "#/properties/model/properties/parameter_group_advanced/properties/third",
                            "options": {"format": "integer"},
                        },
                    ],
                },
            ],
        }

        extracted = kp.extract_ui_schema(self.parameterized_advanced_option)
        self.assertEqual(expected, extracted)

    def test_default_validators(self):
        """
        Test the default type-checking validators provided with each parameter class.
        """
        with self.assertRaises(TypeError):
            self.parameterized.int_param = "foo"

        with self.assertRaises(TypeError):
            self.parameterized.string_param = 1

        with self.assertRaises(TypeError):
            self.parameterized.bool_param = 1

    def test_custom_validators(self):
        """
        Test custom validators for parameters.

        Note: custom validators can currently only be set inside of the parameter
        group class definition.
        """
        with self.assertRaises(ValueError):
            self.parameterized.parameter_group.third = -1

        with self.assertRaises(ValueError):
            self.parameterized.string_param = "hello there, General Kenobi"

        # Check that the default type validators still work
        with self.assertRaises(TypeError):
            self.parameterized.parameter_group.third = "foo"

        with self.assertRaises(TypeError):
            self.parameterized.string_param = 1

    def test_group_validation(self):
        """
        Test validators for parameter groups. Group validators can be set internally inside the
        group class definition using the validate(self, values) method, or externally using the
        @group_name.validator decorator notation.

        Validators for parameterized.parameter_group.subgroup:
        - Internal validator: sum of values must not be larger than 100.
        - External validator: sum of values must not be negative OR 'first' must not be equal to 42.
        """
        params_internal = generate_values_dict(first=100)
        params_external = generate_values_dict(first=-90)
        params_forbidden = generate_values_dict(first=42)

        with self.assertRaises(ValueError):
            kp.validate_parameters(self.parameterized, params_internal)

        with self.assertRaises(ValueError):
            kp.validate_parameters(self.parameterized, params_external)

        with self.assertRaises(ValueError):
            kp.validate_parameters(self.parameterized, params_forbidden)

    def test_groups_are_independent(self):
        obj1 = Parameterized()
        obj2 = Parameterized()
        group1 = obj1.parameter_group
        group1.third = 5
        self.assertEqual(group1.third, 5)
        obj2.parameter_group.third = 7
        self.assertEqual(group1.third, 5)

    #### Test parameter composition: ####
    def test_extract_parameters_from_uninitialized_composed(self):
        obj = ComposedParameterized()
        parameters = kp.extract_parameters(obj)
        expected = {
            "model": {
                "first_group": {"first_param": 12345, "second_param": 54321},
                "second_group": {"first_param": 12345, "second_param": 54321},
            }
        }
        self.assertEqual(parameters, expected)

    def test_extract_parameters_from_altered_composed(self):
        obj = ComposedParameterized()
        obj.first_group.first_param = 3
        parameters = kp.extract_parameters(obj)
        expected = {
            "model": {
                "first_group": {"first_param": 3, "second_param": 54321},
                "second_group": {"first_param": 12345, "second_param": 54321},
            }
        }
        self.assertEqual(parameters, expected)

    def test_extract_altered_nested_composition(self):
        obj = NestedComposedParameterized()
        obj.group.first_group.first_param = 42
        extracted = kp.extract_parameters(obj)
        expected = NestedComposedParameterized.create_default_dict()
        expected["model"]["group"]["first_group"]["first_param"] = 42
        self.assertEqual(expected, extracted)

    def test_extract_default_nested_compositon(self):
        obj = NestedComposedParameterized()
        extracted = kp.extract_parameters(obj)
        expected = NestedComposedParameterized.create_default_dict()
        self.assertEqual(expected, extracted)

    def test_inject_extract_nested_composition(self):
        obj = NestedComposedParameterized()
        inject = NestedComposedParameterized.create_default_dict()
        inject["model"]["group"]["first_group"]["first_param"] = 2
        inject["model"]["group"]["second_group"]["first_param"] = -5
        kp.inject_parameters(obj, inject, None)
        extracted = kp.extract_parameters(obj)
        self.assertEqual(inject, extracted)

    def test_nested_composed_init_setting(self):
        """
        Test value retention when the root group of a nested series of groups is defined inside __init__.
        Test both composed and descriptor nested groups.
        """
        obj = ComplexNestedComposedParameterized()
        obj_expected = obj.get_expected_params()

        inject_composed = obj_expected.copy()
        inject_composed["model"]["init_group"]["group_root_param"] = "CHANGED"
        inject_composed["model"]["init_group"]["new_constructor_param"] = "CHANGED"
        inject_composed["model"]["init_group"]["group_root_nested"][
            "nested_root_param"
        ] = "CHANGED"
        inject_composed["model"]["init_group"]["group_root_nested"][
            "nested_init_param"
        ] = "CHANGED"
        inject_composed["model"]["init_group"]["group_init_nested"][
            "nested_root_param"
        ] = "CHANGED"
        inject_composed["model"]["init_group"]["group_init_nested"][
            "nested_init_param"
        ] = "CHANGED"
        kp.inject_parameters(obj, inject_composed)
        composed_extracted = kp.extract_parameters(obj)
        self.assertEqual(composed_extracted, inject_composed)

    def test_nested_composed_descriptor_setting(self):
        """
        Test value retention when the root group of a nested series of groups is a descriptor.
        Test both composed and descriptor nested groups.
        """
        obj = ComplexNestedComposedParameterized()
        obj_expected = obj.get_expected_params()

        inject_descriptor = obj_expected.copy()
        inject_descriptor["model"]["root_param"] = "CHANGED"
        inject_descriptor["model"]["root_group"]["group_root_param"] = "CHANGED"
        inject_descriptor["model"]["root_group"]["new_constructor_param"] = "CHANGED"
        inject_descriptor["model"]["root_group"]["group_root_nested"][
            "nested_root_param"
        ] = "CHANGED"
        inject_descriptor["model"]["root_group"]["group_root_nested"][
            "nested_init_param"
        ] = "CHANGED"

        inject_descriptor["model"]["root_group"]["group_init_nested"][
            "nested_root_param"
        ] = "CHANGED"
        inject_descriptor["model"]["root_group"]["group_init_nested"][
            "nested_init_param"
        ] = "CHANGED"
        kp.inject_parameters(obj, inject_descriptor)
        descriptor_extracted = kp.extract_parameters(obj)
        self.assertEqual(descriptor_extracted, inject_descriptor)

    def test_extract_schema_from_composed(self):
        obj = ComposedParameterized()
        schema = kp.extract_schema(obj)
        expected = {
            "type": "object",
            "properties": {
                "model": {
                    "type": "object",
                    "properties": {
                        "first_group": {
                            "type": "object",
                            "properties": {
                                "first_param": {
                                    "title": "Plain int param",
                                    "description": "Description of the plain int param.",
                                    "type": "integer",
                                    "format": "int32",
                                },
                                "second_param": {
                                    "title": "Second int param",
                                    "description": "Description of the second plain int param.",
                                    "type": "integer",
                                    "format": "int32",
                                },
                            },
                        },
                        "second_group": {
                            "type": "object",
                            "properties": {
                                "first_param": {
                                    "title": "Plain int param",
                                    "description": "Description of the plain int param.",
                                    "type": "integer",
                                    "format": "int32",
                                },
                                "second_param": {
                                    "title": "Second int param",
                                    "description": "Description of the second plain int param.",
                                    "type": "integer",
                                    "format": "int32",
                                },
                            },
                        },
                    },
                },
            },
        }
        self.assertEqual(schema, expected)

    def test_extract_ui_schema_from_composed(self):
        obj = ComposedParameterized()
        ui_schema = kp.extract_ui_schema(obj)
        expected = {
            "type": "VerticalLayout",
            "elements": [
                {
                    "type": "Section",
                    "label": "Parameter group to be used for multiple descriptor instances.",
                    "options": {},
                    "elements": [
                        {
                            "type": "Control",
                            "label": "Plain int param",
                            "scope": "#/properties/model/properties/first_group/properties/first_param",
                            "options": {"format": "integer"},
                        },
                        {
                            "type": "Control",
                            "label": "Second int param",
                            "scope": "#/properties/model/properties/first_group/properties/second_param",
                            "options": {"format": "integer"},
                        },
                    ],
                },
                {
                    "type": "Section",
                    "label": "Parameter group to be used for multiple descriptor instances.",
                    "options": {},
                    "elements": [
                        {
                            "type": "Control",
                            "label": "Plain int param",
                            "scope": "#/properties/model/properties/second_group/properties/first_param",
                            "options": {"format": "integer"},
                        },
                        {
                            "type": "Control",
                            "label": "Second int param",
                            "scope": "#/properties/model/properties/second_group/properties/second_param",
                            "options": {"format": "integer"},
                        },
                    ],
                },
            ],
        }
        self.assertEqual(ui_schema, expected)

    def test_extract_description(self):
        expected = [
            {
                "name": "Options",
                "description": "",
                "options": [
                    {"name": "Int Parameter", "description": "An integer parameter"},
                    {"name": "Double Parameter", "description": "A double parameter"},
                    {"name": "String Parameter", "description": "A string parameter"},
                    {"name": "Boolean Parameter", "description": "A boolean parameter"},
                    {
                        "name": "Column Parameter",
                        "description": "A column parameter",
                    },
                    {
                        "name": "Multi Column Parameter",
                        "description": "A multi column parameter",
                    },
                ],
            },
            {
                "name": "Primary Group",
                "description": "A parameter group which contains a parameter group as a subgroup, and sets a custom validator for a parameter.",
                "options": [
                    {
                        "name": "First Parameter",
                        "description": "First parameter description",
                    },
                    {
                        "name": "Second Parameter",
                        "description": "Second parameter description",
                    },
                    {
                        "name": "Internal int Parameter",
                        "description": "Internal int parameter description",
                    },
                ],
            },
        ]

        description, use_tabs = kp.extract_parameter_descriptions(self.parameterized)

        self.assertTrue(use_tabs)
        self.assertEqual(description, expected)

        # Without a group -> only top level options
        expected = [
            {"name": "Int Parameter", "description": "An integer parameter"},
            {"name": "Double Parameter", "description": "A double parameter"},
            {"name": "String Parameter", "description": "A string parameter"},
            {"name": "Boolean Parameter", "description": "A boolean parameter"},
            {"name": "Column Parameter", "description": "A column parameter"},
            {
                "name": "Multi Column Parameter",
                "description": "A multi column parameter",
            },
        ]
        description, use_tabs = kp.extract_parameter_descriptions(
            self.parameterized_without_group
        )
        self.assertFalse(use_tabs)
        self.assertEqual(description, expected)

    def test_inject_validates(self):
        pass  # TODO
        # injection of custom parameter/parameter group validators can only be done
        # inside their "parent" class declaration

    ### Test versioning of node settings ####
    def test_extract_schema_with_version(self):
        for version in ["0.1.0", "0.2.0", "0.3.0"]:
            schema = kp.extract_schema(self.versioned_parameterized, version)
            expected = generate_versioned_schema_dict(extension_version=version)
            self.assertEqual(schema, expected)

    def test_extract_ui_schema_with_version(self):
        for version in ["0.1.0", "0.2.0", "0.3.0"]:
            ui_schema = kp.extract_ui_schema(self.versioned_parameterized, version)
            expected = generate_versioned_ui_schema_dict(extension_version=version)
            self.assertEqual(ui_schema, expected)

    def test_version_parsing(self):
        # test default behaviour
        self.assertEqual(kp.Version.parse_version(None), kp.Version(0, 0, 0))

        self.assertEqual(
            kp.Version.parse_version(kp.Version.parse_version(None)),
            kp.Version(0, 0, 0),
        )

        self.assertEqual(kp.Version.parse_version("0.1.0"), kp.Version(0, 1, 0))

        # test that incorrect formatting raises ValueError
        for version in [
            "0.0.0.1",
            "0.0.a",
            "0.1.a",
            "0.1.0-alpha",
            "0.0-alpha.1",
            "1.-3.7.-5.2",
            ".0.1",
            "a.b.c",
            "",
            "0",
            "...",
        ]:
            with self.assertRaises(ValueError):
                kp.Version.parse_version(version)

        # check that comparing version works as expected
        self.assertTrue(kp.Version(0, 1, 0) > kp.Version(0, 0, 0))
        self.assertTrue(kp.Version(0, 1, 0) >= kp.Version(0, 0, 0))
        self.assertTrue(kp.Version(0, 0, 1) >= kp.Version(0, 0, 0))
        self.assertTrue(kp.Version(1, 1, 2) >= kp.Version(1, 1, 1))

    def test_determining_compatibility(self):
        # given the version of the extension that the node settings were saved with,
        # and the version of the installed extension, test identifying whether
        # we have a case of backward or forward compatibility

        cases = [
            # (saved_version, installed_version, saved_params)
            (
                "0.2.0",
                "0.1.0",
                generate_versioned_values_dict(extension_version="0.2.0"),
            ),
            (
                "0.1.0",
                "0.2.0",
                generate_versioned_values_dict(extension_version="0.1.0"),
            ),
            (
                "0.1.0",
                "0.3.0",
                generate_versioned_values_dict(extension_version="0.1.0"),
            ),
            (
                "0.2.0",
                "0.3.0",
                generate_versioned_values_dict(extension_version="0.2.0"),
            ),
        ]

        with self.assertLogs(level="DEBUG") as context_manager:
            for saved_version, installed_version, saved_params in cases:
                kp.determine_compatability(
                    self.versioned_parameterized,
                    saved_version,
                    installed_version,
                    saved_params,
                )

            self.assertEqual(
                [
                    # 0.2.0 -> 0.1.0: forward compatibility (not supported)
                    "ERROR:Python backend: The node was previously configured with a newer version of the extension, 0.2.0, while the current version is 0.1.0.",
                    "ERROR:Python backend: The node might not work as expected without being reconfigured.",
                    # 0.1.0 -> 0.2.0: backward compatibility
                    "DEBUG:Python backend: The node was previously configured with an older version of the extension, 0.1.0, while the current version is 0.2.0.",
                    "DEBUG:Python backend: The following parameters have since been added, and are configured with their default values:",
                    'DEBUG:Python backend: - "String Parameter"',
                    'DEBUG:Python backend: - "First Parameter"',
                    # 0.1.0 -> 0.3.0: backward compatibility
                    "DEBUG:Python backend: The node was previously configured with an older version of the extension, 0.1.0, while the current version is 0.3.0.",
                    "DEBUG:Python backend: The following parameters have since been added, and are configured with their default values:",
                    'DEBUG:Python backend: - "String Parameter"',
                    'DEBUG:Python backend: - "Boolean Parameter"',
                    'DEBUG:Python backend: - "First Parameter"',
                    'DEBUG:Python backend: - "Second Parameter"',
                    # 0.2.0 -> 0.3.0: backward compatibility
                    "DEBUG:Python backend: The node was previously configured with an older version of the extension, 0.2.0, while the current version is 0.3.0.",
                    "DEBUG:Python backend: The following parameters have since been added, and are configured with their default values:",
                    'DEBUG:Python backend: - "Boolean Parameter"',
                    'DEBUG:Python backend: - "Second Parameter"',
                ],
                context_manager.output,
            )

    def test_custom_methods_in_parameter_groups(self):
        self.assertEqual(
            self.parameterized_with_custom_methods.outer_group.recursive_method(0), 10
        )

        self.assertEqual(
            self.parameterized_with_custom_methods.outer_group.middle_group.inner_group.inner_foo(),
            4,
        )

        self.assertEqual(
            self.parameterized_with_custom_methods.outer_group.nested_method(),
            self.parameterized_with_custom_methods.outer_group.middle_group.inner_group.inner_foo(),
        )

        self.assertEqual(
            self.parameterized_with_custom_methods.outer_group._protected_method(),
            "bar",
        )

        # test that validation still works
        forbidden_params_inner = generate_values_dict_for_group_w_custom_method(
            inner_int_param2=3
        )
        forbidden_params_middle = generate_values_dict_for_group_w_custom_method(
            middle_int_param=10
        )

        with self.assertRaises(ValueError):
            kp.validate_parameters(
                self.parameterized_with_custom_methods, forbidden_params_inner
            )

        with self.assertRaises(ValueError):
            kp.validate_parameters(
                self.parameterized_with_custom_methods, forbidden_params_middle
            )


if __name__ == "__main__":
    unittest.main()
