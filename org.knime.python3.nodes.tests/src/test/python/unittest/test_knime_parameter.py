import unittest

import knime.api.schema as ks
import knime.extension.nodes as kn
import knime.extension.parameter as kp
from typing import List

test_columns = {
    "int": ks.Column(
        ks.int32(),
        "IntColumn",
        {
            "preferred_value_type": "org.knime.core.IntValue",
            "displayed_column_type": "Integer",
        },
    ),
    "double": ks.Column(
        ks.double(),
        "DoubleColumn",
        {
            "preferred_value_type": "org.knime.core.DoubleValue",
            "displayed_column_type": "Double",
        },
    ),
    "string": ks.Column(
        ks.string(),
        "StringColumn",
        {
            "preferred_value_type": "org.knime.core.StringValue",
            "displayed_column_type": "String",
        },
    ),
    "long list": ks.Column(
        ks.list_(ks.int64()),
        "LongListColumn",
        {
            "preferred_value_type": "org.knime.core.data.collection.ListDataValue",
            "displayed_column_type": "Long List",
        },
    ),
}

test_schema = ks.Schema.from_columns(
    [
        test_columns["int"],
        test_columns["double"],
        test_columns["string"],
        test_columns["long list"],
    ]
)

test_values = {
    "int": {
        "id": "IntColumn",
        "text": "IntColumn",
        "type": {"id": "org.knime.core.IntValue", "text": "Integer"},
        "compatibleTypes": ["org.knime.core.IntValue"],
    },
    "double": {
        "id": "DoubleColumn",
        "text": "DoubleColumn",
        "type": {"id": "org.knime.core.DoubleValue", "text": "Double"},
        "compatibleTypes": ["org.knime.core.DoubleValue"],
    },
    "string": {
        "id": "StringColumn",
        "text": "StringColumn",
        "type": {"id": "org.knime.core.StringValue", "text": "String"},
        "compatibleTypes": ["org.knime.core.StringValue"],
    },
    "long list": {
        "id": "LongListColumn",
        "text": "LongListColumn",
        "type": {
            "id": "org.knime.core.data.collection.ListDataValue",
            "text": "Long List",
        },
        "compatibleTypes": ["org.knime.core.data.collection.ListDataValue"],
    },
}

test_possible_values = [
    test_values["int"],
    test_values["double"],
    test_values["string"],
    test_values["long list"],
]


class TestEnumSetOptions(kp.EnumParameterOptions):
    FOO = ("Foo", "The foo")
    BAR = ("Bar", "The bar")
    BAZ = ("Baz", "The baz")


@kp.parameter_group("Parameter Group for Parameter Array")
class ParameterArrayGroup:
    """A parameter group which contains two parameters and passed to a ParameterArray."""

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


@kp.parameter_group("Parameter Group with Nested Group")
class ParameterGroupWithGroup:
    """A parameter group which contains two parameters and a parameter group
    and passed to a ParameterArray."""

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

    group = ParameterArrayGroup()


@kp.parameter_group("Parameter Group with Nested Group")
class ParameterGroupWithArray:
    """A parameter group which contains two parameters and a parameter array
    and passed to a ParameterArray."""

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

    third = kp.ParameterArray(
        parameters=ParameterArrayGroup(),
    )


def generate_values_dict(
    int_param=3,
    double_param=1.5,
    string_param="foo",
    multiline_string_param="foo\nbar",
    bool_param=True,
    column_param="foo_column",
    multi_column_param=["foo_column", "bar_column"],
    full_multi_column_param=kp.ColumnFilterConfig(
        included_column_names=["foo_column", "bar_column"]
    ),
    enum_set_param=[TestEnumSetOptions.FOO.name],
    first=1,
    second=5,
    third=3,
    parameter_array=[],
):
    return {
        "model": {
            "int_param": int_param,
            "double_param": double_param,
            "string_param": string_param,
            "multiline_string_param": multiline_string_param,
            "bool_param": bool_param,
            "column_param": column_param,
            "multi_column_param": multi_column_param,
            "full_multi_column_param": full_multi_column_param._to_dict(),
            "enum_set_param": enum_set_param,
            "parameter_group": {
                "subgroup": {"first": first, "second": second},
                "third": third,
            },
            "parameter_group_layout": {
                "string_param": string_param,
                "double_param": double_param,
            },
            "parameter_array": parameter_array,
        }
    }


def generate_values_dict_without_groups(
    int_param=3,
    double_param=1.5,
    string_param="foo",
    bool_param=True,
    column_param="foo_column",
    multi_column_param=["foo_column", "bar_column"],
    full_multi_column_param=kp.ColumnFilterConfig(
        included_column_names=["foo_column", "bar_column"]
    ),
    enum_set_param=[TestEnumSetOptions.FOO.name],
):
    return {
        "model": {
            "int_param": int_param,
            "double_param": double_param,
            "string_param": string_param,
            "bool_param": bool_param,
            "column_param": column_param,
            "multi_column_param": multi_column_param,
            "full_multi_column_param": full_multi_column_param._to_dict(),
            "enum_set_param": enum_set_param,
        }
    }


def generate_values_dict_with_one_group(
    int_param=3,
    double_param=1.5,
    string_param="foo",
    bool_param=True,
    column_param="foo_column",
    multi_column_param=["foo_column", "bar_column"],
    full_multi_column_param=kp.ColumnFilterConfig(
        included_column_names=["foo_column", "bar_column"]
    ),
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
            "full_multi_column_param": full_multi_column_param._to_dict(),
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
    parameterized_object.full_multi_column_param = kp.ColumnFilterConfig(
        included_column_names=["foo_column", "bar_column"]
    )


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
                        "enum_set_param": {
                            "description": "An EnumSet Parameter\n\n**Available options:**\n\n- Foo: The "
                            "foo\n- Bar: The bar\n- Baz: The baz\n",
                            "items": {"type": "string"},
                            "title": "EnumSet Parameter",
                            "type": "array",
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
                        "enum_set_param": {
                            "description": "An EnumSet Parameter\n\n**Available options:**\n\n- Foo: The "
                            "foo\n- Bar: The bar\n- Baz: The baz\n",
                            "items": {"type": "string"},
                            "title": "EnumSet Parameter",
                            "type": "array",
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


@kp.parameter_group(
    "Test group with layout type", layout_direction=kp.LayoutDirection.HORIZONTAL
)
class TestGroupWithLayoutType:
    string_param = kp.StringParameter("String Parameter", "A string parameter", "foo")
    double_param = kp.DoubleParameter("Double Parameter", "A double parameter", 1.5)


class Parameterized:
    int_param = kp.IntParameter("Int Parameter", "An integer parameter", 3)
    double_param = kp.DoubleParameter("Double Parameter", "A double parameter", 1.5)
    string_param = kp.StringParameter("String Parameter", "A string parameter", "foo")
    multiline_string_param = kp.MultilineStringParameter(
        "Multiline String Parameter",
        "A multiline string parameter",
        "foo\nbar",
        number_of_lines=5,
    )
    bool_param = kp.BoolParameter("Boolean Parameter", "A boolean parameter", True)
    column_param = kp.ColumnParameter("Column Parameter", "A column parameter")
    multi_column_param = kp.MultiColumnParameter(
        "Multi Column Parameter",
        "A multi column parameter",
    )
    full_multi_column_param = kp.ColumnFilterParameter(
        "Full Multi Column Parameter",
        "A full multi column parameter",
    )
    enum_set_param = kp.EnumSetParameter(
        "EnumSet Parameter",
        "An EnumSet Parameter",
        [TestEnumSetOptions.FOO.name],
        TestEnumSetOptions,
    )
    parameter_group = ParameterGroup()
    parameter_array = kp.ParameterArray(
        parameters=ParameterArrayGroup(),
        array_title="First Array Title",
    )

    parameter_group_layout = TestGroupWithLayoutType()

    @string_param.validator
    def validate_string_param(value):
        if len(value) > 10:
            raise ValueError("Length of string must not exceed 10!")


class ParameterizedIndentation:
    indented_spaces_enum = kp.EnumParameter(
        label="Indented Enum Parameter",
        description="""
                Any Text
                """,
        default_value="txt",
    )
    indented_tabs_enum = kp.EnumParameter(
        label="Indented Enum Parameter",
        description="""
\t\tAny Text
\t\t""",
        default_value="txt",
    )


class ParameterizedEnumSet:
    """Provides edge case EnumSetParameters"""

    no_default_value_enum_set = kp.EnumSetParameter(
        label="EnumSet Parameter",
        description="An EnumSet Parameter",
        enum=TestEnumSetOptions,
    )
    no_enum_enum_set = kp.EnumSetParameter(
        label="EnumSet Parameter",
        description="An EnumSet Parameter",
        default_value=[TestEnumSetOptions.FOO.name],
    )


def _validate_parameter_array(values):
    for value in values:
        if value["first"] + value["second"] > 8:
            raise ValueError("The sum of 'first' and 'second' must not exceed 8.")


class ParameterizedParameterArray:
    """Provides checks for other ParameterArray parameters"""

    parameter_array_vertical = kp.ParameterArray(
        label="Vertical Array",
        description="A vertical array",
        parameters=ParameterArrayGroup(),
        allow_reorder=True,
        layout_direction=kp.LayoutDirection.VERTICAL,
        array_title="Second Array Title",
    )

    parameter_array_horizontal = kp.ParameterArray(
        label="Horizontal Array",
        description="A horizontal array",
        parameters=ParameterArrayGroup(),
        allow_reorder=False,
        layout_direction=kp.LayoutDirection.HORIZONTAL,
        button_text="Add new parameter",
        validator=_validate_parameter_array,
    )

    @parameter_array_vertical.validator
    def validate_parameter_array(values):
        _validate_parameter_array(values)


class ParameterizedParameterArrayNested:
    """Provides checks for invalid ParameterArray initialisation"""

    parameter_array_first = kp.ParameterArray(
        label="First Array",
        description="First array",
        parameters=ParameterGroupWithGroup(),
    )

    parameter_array_second = kp.ParameterArray(
        label="Second Array",
        description="Second array",
        parameters=ParameterGroupWithArray(),
        validator=_validate_parameter_array,
    )

    @parameter_array_first.validator
    def validate_parameter_array(values):
        _validate_parameter_array(values)


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
    full_multi_column_param = kp.ColumnFilterParameter(
        "Full Multi Column Parameter",
        "A full multi column parameter",
    )
    parameter_group = NestedParameterGroup()

    @string_param.validator
    def validate_string_param(value):
        if len(value) > 10:
            raise ValueError("Length of string must not exceed 10!")


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
    multiline_string_param = kp.MultilineStringParameter(
        "Multiline String Parameter",
        "A multiline string parameter",
        "foo\nbar",
        number_of_lines=5,
    )
    multiline_string_advanced_param = kp.MultilineStringParameter(
        "Multiline String Parameter",
        "A multiline string parameter",
        "foo\nbar",
        number_of_lines=5,
        is_advanced=True,
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
    full_multi_column_param = kp.ColumnFilterParameter(
        "Full Multi Column Parameter",
        "A full multi column parameter",
    )
    full_multi_column_advanced_param = kp.ColumnFilterParameter(
        "Full Multi Column Parameter", "A full multi column parameter", is_advanced=True
    )
    enum_set_param = kp.EnumSetParameter(
        "EnumSet Parameter",
        "An EnumSet Parameter",
        [TestEnumSetOptions.FOO.name],
        TestEnumSetOptions,
    )
    enum_set_advanced_param = kp.EnumSetParameter(
        "EnumSet Parameter",
        "An EnumSet Parameter",
        [TestEnumSetOptions.FOO.name],
        TestEnumSetOptions,
        is_advanced=True,
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
    full_multi_column_param = kp.ColumnFilterParameter(
        "Full Multi Column Parameter",
        "A full multi column parameter",
    )
    enum_set_param = kp.EnumSetParameter(
        "EnumSet Parameter",
        "An EnumSet Parameter",
        [TestEnumSetOptions.FOO.name],
        TestEnumSetOptions,
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
    enum_set_param = kp.EnumSetParameter(
        "EnumSet Parameter",
        "An EnumSet Parameter",
        [TestEnumSetOptions.FOO.name],
        TestEnumSetOptions,
        since_version="0.2.0",
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


class TestEnumOptions(kp.EnumParameterOptions):
    FOO = ("Foo", "The foo")
    BAR = ("Bar", "The bar")
    BAZ = ("Baz", "The baz")


class ParameterizedWithEnumStyles:
    radio = kp.EnumParameter(
        "radio",
        "Radio buttons",
        TestEnumOptions.FOO,
        TestEnumOptions,
        style=kp.EnumParameter.Style.RADIO,
    )
    value_switch = kp.EnumParameter(
        "value switch",
        "Value switch",
        TestEnumOptions.FOO,
        TestEnumOptions,
        style=kp.EnumParameter.Style.VALUE_SWITCH,
    )
    dropdown = kp.EnumParameter(
        "Dropdown",
        "dropdown",
        TestEnumOptions.FOO,
        TestEnumOptions,
        style=kp.EnumParameter.Style.DROPDOWN,
    )
    default = kp.EnumParameter(
        "Default",
        "The default (should be radio for fewer than 4 choices)",
        TestEnumOptions.FOO,
        TestEnumOptions,
    )


class DummyDialogCreationContext:
    def __init__(self, specs: List = None) -> None:
        class DummyJavaContext:
            def get_credential_names(self):
                return ["foo", "bar", "baz"]

            def get_credential(self, name):
                return "dummy"

        self._java_ctx = DummyJavaContext()
        self._flow_variables = ["flow1", "flow2", "flow3"]
        self._specs = specs if specs is not None else [test_schema]

    def get_input_specs(self):
        return self._specs


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
        self.parameterized_with_indented_docstring = ParameterizedIndentation()
        self.parameterized_with_enum_set_params = ParameterizedEnumSet()
        self.parameterized_with_parameter_array = ParameterizedParameterArray()
        self.parameterized_with_parameter_array_nested = (
            ParameterizedParameterArrayNested()
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

    def test_create_param_group_instance_param_array(self):
        obj = Parameterized()
        params = {"model": {"parameter_array": [{"first": 1, "second": 5}]}}

        kp.inject_parameters(obj, params)
        for param in obj.parameter_array:
            self.assertEqual(param.first, 1)
            self.assertEqual(param.second, 5)

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
        self.assertEqual(
            self.parameterized.full_multi_column_param,
            kp.ColumnFilterConfig(
                manual_filter=kp.ManualFilterConfig(
                    included=["foo_column", "bar_column"]
                )
            ),
        )
        self.assertEqual(
            self.parameterized.enum_set_param, [TestEnumSetOptions.FOO.name]
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
        self.parameterized.enum_set_param = [
            TestEnumSetOptions.FOO.name,
            TestEnumSetOptions.BAZ.name,
        ]

        # group-level parameters
        self.assertEqual(self.parameterized.int_param, 5)
        self.assertEqual(self.parameterized.double_param, 5.5)
        self.assertEqual(self.parameterized.string_param, "bar")
        self.assertEqual(self.parameterized.bool_param, False)
        self.assertEqual(self.parameterized.column_param, "foo_column")
        self.assertEqual(
            self.parameterized.multi_column_param, ["foo_column", "bar_column"]
        )
        self.assertEqual(
            self.parameterized.enum_set_param,
            [
                TestEnumSetOptions.FOO.name,
                TestEnumSetOptions.BAZ.name,
            ],
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
            4,
            2.7,
            "bar",
            "bar",
            False,
            "foo_column",
            ["foo_column", "bar_column"],
            kp.ColumnFilterConfig(
                manual_filter=kp.ManualFilterConfig(
                    included=["foo_column", "bar_column"]
                )
            ),
            [TestEnumSetOptions.FOO.name],
            3,
            2,
            1,
            [
                {"first": 1, "second": 2},
                {"first": 3, "second": 4},
                {"first": 5, "second": 6},
            ],
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

    def test_inject_parameters_with_validation(self):
        """Test validation behaviour for LocalPathParameter."""

        class ParameterizedWithPath:
            int_param = kp.IntParameter("Int Parameter", "An integer parameter", 3)
            path_param = kp.LocalPathParameter(
                "Local Path Parameter", "A local path parameter", ""
            )

        params = {
            "model": {
                "int_param": 4,
                "path_param": "C:\\Users\\Name\\Wrong_Path",
            }
        }

        parameterized = ParameterizedWithPath()

        kp.inject_parameters(parameterized, params, exclude_validations=True)
        extracted = kp.extract_parameters(parameterized)
        self.assertEqual(params, extracted)

        # Check if error occurs when validation is done
        with self.assertRaises(ValueError):
            kp.inject_parameters(parameterized, params, exclude_validations=False)

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
                        "multiline_string_param": {
                            "title": "Multiline String Parameter",
                            "description": "A multiline string parameter",
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
                            "type": "string",
                        },
                        "multi_column_param": {
                            "title": "Multi Column Parameter",
                            "description": "A multi column parameter",
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "full_multi_column_param": {
                            "title": "Full Multi Column Parameter",
                            "description": "A full multi column parameter",
                            "type": "object",
                            "properties": {
                                "patternFilter": {
                                    "type": "object",
                                    "properties": {
                                        "isCaseSensitive": {
                                            "type": "boolean",
                                            "default": True,
                                        },
                                        "isInverted": {
                                            "type": "boolean",
                                            "default": False,
                                        },
                                        "pattern": {"type": "string", "default": ""},
                                    },
                                },
                                "typeFilter": {
                                    "type": "object",
                                    "properties": {
                                        "selectedTypes": {
                                            "default": [],
                                            "type": "array",
                                            "items": {"type": "string"},
                                        },
                                        "typeDisplays": {
                                            "default": [],
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "properties": {
                                                    "id": {"type": "string"},
                                                    "text": {"type": "string"},
                                                },
                                            },
                                        },
                                    },
                                },
                                "manualFilter": {
                                    "type": "object",
                                    "properties": {
                                        "includeUnknownColumns": {
                                            "type": "boolean",
                                            "default": True,
                                        },
                                        "manuallyDeselected": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                            "default": [],
                                        },
                                        "manuallySelected": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                            "default": [],
                                        },
                                    },
                                },
                                "mode": {
                                    "oneOf": [
                                        {"const": "MANUAL", "title": "Manual"},
                                        {"const": "REGEX", "title": "Regex"},
                                        {"const": "WILDCARD", "title": "Wildcard"},
                                        {"const": "TYPE", "title": "Type"},
                                    ]
                                },
                                "selected": {
                                    "type": "array",
                                    "items": {
                                        "type": "string",
                                        "configKeys": ["selected_Internals"],
                                    },
                                    "configKeys": ["selected_Internals"],
                                },
                            },
                        },
                        "enum_set_param": {
                            "description": "An EnumSet Parameter\n\n**Available options:**\n\n- Foo: The "
                            "foo\n- Bar: The bar\n- Baz: The baz\n",
                            "items": {"type": "string"},
                            "title": "EnumSet Parameter",
                            "type": "array",
                        },
                        "parameter_group": {
                            "type": "object",
                            "properties": {
                                "subgroup": {
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
                                "third": {
                                    "title": "Internal int Parameter",
                                    "description": "Internal int parameter description",
                                    "type": "integer",
                                    "format": "int32",
                                },
                            },
                        },
                        "parameter_array": {
                            "title": "parameter_array",
                            "description": "",
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "first": {
                                        "title": "First Parameter",
                                        "description": "First parameter description",
                                        "type": "integer",
                                        "format": "int32",
                                        "default": 1,
                                    },
                                    "second": {
                                        "title": "Second Parameter",
                                        "description": "Second parameter description",
                                        "type": "integer",
                                        "format": "int32",
                                        "default": 5,
                                    },
                                },
                            },
                        },
                        "parameter_group_layout": {
                            "type": "object",
                            "properties": {
                                "string_param": {
                                    "title": "String Parameter",
                                    "description": "A string parameter",
                                    "type": "string",
                                },
                                "double_param": {
                                    "title": "Double Parameter",
                                    "description": "A double parameter",
                                    "type": "number",
                                    "format": "double",
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
                    "scope": "#/properties/model/properties/int_param",
                    "type": "Control",
                    "label": "Int Parameter",
                    "options": {"format": "integer"},
                },
                {
                    "scope": "#/properties/model/properties/double_param",
                    "type": "Control",
                    "label": "Double Parameter",
                    "options": {"format": "number"},
                },
                {
                    "scope": "#/properties/model/properties/string_param",
                    "type": "Control",
                    "label": "String Parameter",
                    "options": {"format": "string"},
                },
                {
                    "scope": "#/properties/model/properties/multiline_string_param",
                    "type": "Control",
                    "label": "Multiline String Parameter",
                    "options": {"format": "textArea", "rows": 5},
                },
                {
                    "scope": "#/properties/model/properties/bool_param",
                    "type": "Control",
                    "label": "Boolean Parameter",
                    "options": {"format": "boolean"},
                },
                {
                    "scope": "#/properties/model/properties/column_param",
                    "type": "Control",
                    "label": "Column Parameter",
                    "options": {
                        "format": "dropDown",
                        "showRowKeys": False,
                        "showNoneColumn": False,
                        "possibleValues": test_possible_values,
                    },
                },
                {
                    "scope": "#/properties/model/properties/multi_column_param",
                    "type": "Control",
                    "label": "Multi Column Parameter",
                    "options": {
                        "format": "twinList",
                        "possibleValues": test_possible_values,
                    },
                },
                {
                    "scope": "#/properties/model/properties/full_multi_column_param",
                    "type": "Control",
                    "label": "Full Multi Column Parameter",
                    "options": {
                        "format": "typedStringFilter",
                        "unknownValuesText": "Any unknown column",
                        "emptyStateLabel": "No columns in this list.",
                        "possibleValues": test_possible_values,
                    },
                },
                {
                    "scope": "#/properties/model/properties/enum_set_param",
                    "type": "Control",
                    "label": "EnumSet Parameter",
                    "options": {
                        "format": "twinList",
                        "possibleValues": [
                            {"id": "FOO", "text": "Foo"},
                            {"id": "BAR", "text": "Bar"},
                            {"id": "BAZ", "text": "Baz"},
                        ],
                    },
                },
                {
                    "type": "Section",
                    "label": "Primary Group",
                    "options": {},
                    "elements": [
                        {
                            "type": "VerticalLayout",
                            "elements": [
                                {
                                    "type": "Section",
                                    "label": "Subgroup",
                                    "options": {},
                                    "elements": [
                                        {
                                            "type": "VerticalLayout",
                                            "elements": [
                                                {
                                                    "scope": "#/properties/model/properties/parameter_group/properties/subgroup/properties/first",
                                                    "type": "Control",
                                                    "label": "First Parameter",
                                                    "options": {"format": "integer"},
                                                },
                                                {
                                                    "scope": "#/properties/model/properties/parameter_group/properties/subgroup/properties/second",
                                                    "type": "Control",
                                                    "label": "Second Parameter",
                                                    "options": {"format": "integer"},
                                                },
                                            ],
                                        }
                                    ],
                                },
                                {
                                    "scope": "#/properties/model/properties/parameter_group/properties/third",
                                    "type": "Control",
                                    "label": "Internal int Parameter",
                                    "options": {"format": "integer"},
                                },
                            ],
                        }
                    ],
                },
                {
                    "type": "Section",
                    "label": "parameter_array",
                    "elements": [
                        {
                            "scope": "#/properties/model/properties/parameter_array",
                            "type": "Control",
                            "label": "parameter_array",
                            "options": {
                                "addButtonText": None,
                                "detail": {
                                    "layout": {
                                        "type": "VerticalLayout",
                                        "elements": [
                                            {
                                                "type": "Control",
                                                "scope": "#/properties/first",
                                                "label": "First Parameter",
                                                "options": {"format": "integer"},
                                            },
                                            {
                                                "type": "Control",
                                                "scope": "#/properties/second",
                                                "label": "Second Parameter",
                                                "options": {"format": "integer"},
                                            },
                                        ],
                                    }
                                },
                                "arrayElementTitle": "First Array Title",
                                "showSortButtons": True,
                            },
                        }
                    ],
                },
                {
                    "type": "Section",
                    "label": "Test group with layout type",
                    "options": {},
                    "elements": [
                        {
                            "type": "HorizontalLayout",
                            "elements": [
                                {
                                    "scope": "#/properties/model/properties/parameter_group_layout/properties/string_param",
                                    "type": "Control",
                                    "label": "String Parameter",
                                    "options": {"format": "string"},
                                },
                                {
                                    "scope": "#/properties/model/properties/parameter_group_layout/properties/double_param",
                                    "type": "Control",
                                    "label": "Double Parameter",
                                    "options": {"format": "number"},
                                },
                            ],
                        }
                    ],
                },
            ],
        }
        extracted = kp.extract_ui_schema(
            self.parameterized, DummyDialogCreationContext()
        )
        self.assertEqual(expected, extracted)

    def test_enum_styles(self):
        expected = {
            "type": "VerticalLayout",
            "elements": [
                {
                    "type": "Control",
                    "label": "radio",
                    "scope": "#/properties/model/properties/radio",
                    "options": {"format": "radio"},
                },
                {
                    "type": "Control",
                    "label": "value switch",
                    "scope": "#/properties/model/properties/value_switch",
                    "options": {"format": "valueSwitch"},
                },
                {
                    "type": "Control",
                    "label": "Dropdown",
                    "scope": "#/properties/model/properties/dropdown",
                    "options": {"format": "string"},
                },
                {
                    "type": "Control",
                    "label": "Default",
                    "scope": "#/properties/model/properties/default",
                    "options": {"format": "radio"},
                },
            ],
        }
        extracted = kp.extract_ui_schema(
            ParameterizedWithEnumStyles(), DummyDialogCreationContext()
        )
        self.assertEqual(expected, extracted)

    def test_extract_ui_schema_is_advanced_option(self):
        expected = {
            "type": "VerticalLayout",
            "elements": [
                {
                    "scope": "#/properties/model/properties/int_param",
                    "type": "Control",
                    "label": "Int Parameter",
                    "options": {"format": "integer"},
                },
                {
                    "scope": "#/properties/model/properties/int_advanced_param",
                    "type": "Control",
                    "label": "Int Parameter",
                    "options": {"format": "integer", "isAdvanced": True},
                },
                {
                    "scope": "#/properties/model/properties/double_param",
                    "type": "Control",
                    "label": "Double Parameter",
                    "options": {"format": "number"},
                },
                {
                    "scope": "#/properties/model/properties/double_advanced_param",
                    "type": "Control",
                    "label": "Double Parameter",
                    "options": {"format": "number", "isAdvanced": True},
                },
                {
                    "scope": "#/properties/model/properties/string_param",
                    "type": "Control",
                    "label": "String Parameter",
                    "options": {"format": "string"},
                },
                {
                    "scope": "#/properties/model/properties/string_advanced_param",
                    "type": "Control",
                    "label": "String Parameter",
                    "options": {"format": "string", "isAdvanced": True},
                },
                {
                    "scope": "#/properties/model/properties/multiline_string_param",
                    "type": "Control",
                    "label": "Multiline String Parameter",
                    "options": {"format": "textArea", "rows": 5},
                },
                {
                    "scope": "#/properties/model/properties/multiline_string_advanced_param",
                    "type": "Control",
                    "label": "Multiline String Parameter",
                    "options": {"format": "textArea", "rows": 5, "isAdvanced": True},
                },
                {
                    "scope": "#/properties/model/properties/bool_param",
                    "type": "Control",
                    "label": "Boolean Parameter",
                    "options": {"format": "boolean"},
                },
                {
                    "scope": "#/properties/model/properties/bool_advanced_param",
                    "type": "Control",
                    "label": "Boolean Parameter",
                    "options": {"format": "boolean", "isAdvanced": True},
                },
                {
                    "scope": "#/properties/model/properties/column_param",
                    "type": "Control",
                    "label": "Column Parameter",
                    "options": {
                        "format": "dropDown",
                        "showRowKeys": False,
                        "showNoneColumn": False,
                        "possibleValues": test_possible_values,
                    },
                },
                {
                    "scope": "#/properties/model/properties/column_advanced_param",
                    "type": "Control",
                    "label": "Column Parameter",
                    "options": {
                        "format": "dropDown",
                        "showRowKeys": False,
                        "showNoneColumn": False,
                        "possibleValues": test_possible_values,
                        "isAdvanced": True,
                    },
                },
                {
                    "scope": "#/properties/model/properties/multi_column_param",
                    "type": "Control",
                    "label": "Multi Column Parameter",
                    "options": {
                        "format": "twinList",
                        "possibleValues": test_possible_values,
                    },
                },
                {
                    "scope": "#/properties/model/properties/multi_column_advanced_param",
                    "type": "Control",
                    "label": "Multi Column Parameter",
                    "options": {
                        "format": "twinList",
                        "possibleValues": test_possible_values,
                        "isAdvanced": True,
                    },
                },
                {
                    "scope": "#/properties/model/properties/full_multi_column_param",
                    "type": "Control",
                    "label": "Full Multi Column Parameter",
                    "options": {
                        "format": "typedStringFilter",
                        "unknownValuesText": "Any unknown column",
                        "emptyStateLabel": "No columns in this list.",
                        "possibleValues": test_possible_values,
                    },
                },
                {
                    "scope": "#/properties/model/properties/full_multi_column_advanced_param",
                    "type": "Control",
                    "label": "Full Multi Column Parameter",
                    "options": {
                        "format": "typedStringFilter",
                        "unknownValuesText": "Any unknown column",
                        "emptyStateLabel": "No columns in this list.",
                        "possibleValues": test_possible_values,
                        "isAdvanced": True,
                    },
                },
                {
                    "label": "EnumSet Parameter",
                    "options": {
                        "format": "twinList",
                        "possibleValues": [
                            {"id": "FOO", "text": "Foo"},
                            {"id": "BAR", "text": "Bar"},
                            {"id": "BAZ", "text": "Baz"},
                        ],
                    },
                    "scope": "#/properties/model/properties/enum_set_param",
                    "type": "Control",
                },
                {
                    "label": "EnumSet Parameter",
                    "options": {
                        "format": "twinList",
                        "isAdvanced": True,
                        "possibleValues": [
                            {"id": "FOO", "text": "Foo"},
                            {"id": "BAR", "text": "Bar"},
                            {"id": "BAZ", "text": "Baz"},
                        ],
                    },
                    "scope": "#/properties/model/properties/enum_set_advanced_param",
                    "type": "Control",
                },
                {
                    "type": "Section",
                    "label": "Primary Group",
                    "options": {},
                    "elements": [
                        {
                            "type": "VerticalLayout",
                            "elements": [
                                {
                                    "type": "Section",
                                    "label": "Subgroup",
                                    "options": {},
                                    "elements": [
                                        {
                                            "type": "VerticalLayout",
                                            "elements": [
                                                {
                                                    "scope": "#/properties/model/properties/parameter_group/properties/subgroup/properties/first",
                                                    "type": "Control",
                                                    "label": "First Parameter",
                                                    "options": {"format": "integer"},
                                                },
                                                {
                                                    "scope": "#/properties/model/properties/parameter_group/properties/subgroup/properties/second",
                                                    "type": "Control",
                                                    "label": "Second Parameter",
                                                    "options": {"format": "integer"},
                                                },
                                            ],
                                        }
                                    ],
                                },
                                {
                                    "scope": "#/properties/model/properties/parameter_group/properties/third",
                                    "type": "Control",
                                    "label": "Internal int Parameter",
                                    "options": {"format": "integer"},
                                },
                            ],
                        }
                    ],
                },
                {
                    "type": "Section",
                    "label": "Primary Group Advanced",
                    "options": {"isAdvanced": True},
                    "elements": [
                        {
                            "type": "VerticalLayout",
                            "elements": [
                                {
                                    "type": "Section",
                                    "label": "Subgroup",
                                    "options": {},
                                    "elements": [
                                        {
                                            "type": "VerticalLayout",
                                            "elements": [
                                                {
                                                    "scope": "#/properties/model/properties/parameter_group_advanced/properties/subgroup/properties/first",
                                                    "type": "Control",
                                                    "label": "First Parameter",
                                                    "options": {"format": "integer"},
                                                },
                                                {
                                                    "scope": "#/properties/model/properties/parameter_group_advanced/properties/subgroup/properties/second",
                                                    "type": "Control",
                                                    "label": "Second Parameter",
                                                    "options": {"format": "integer"},
                                                },
                                            ],
                                        }
                                    ],
                                },
                                {
                                    "scope": "#/properties/model/properties/parameter_group_advanced/properties/third",
                                    "type": "Control",
                                    "label": "Internal int Parameter",
                                    "options": {"format": "integer"},
                                },
                            ],
                        }
                    ],
                },
            ],
        }
        extracted = kp.extract_ui_schema(
            self.parameterized_advanced_option,
            DummyDialogCreationContext(),
        )
        self.assertEqual(expected, extracted)

    def test_extract_ui_schema_parameter_array(self):
        expected = {
            "type": "VerticalLayout",
            "elements": [
                {
                    "type": "Section",
                    "label": "Vertical Array",
                    "elements": [
                        {
                            "scope": "#/properties/model/properties/parameter_array_vertical",
                            "type": "Control",
                            "label": "Vertical Array",
                            "options": {
                                "addButtonText": None,
                                "detail": {
                                    "layout": {
                                        "type": "VerticalLayout",
                                        "elements": [
                                            {
                                                "type": "Control",
                                                "scope": "#/properties/first",
                                                "label": "First Parameter",
                                                "options": {"format": "integer"},
                                            },
                                            {
                                                "type": "Control",
                                                "scope": "#/properties/second",
                                                "label": "Second Parameter",
                                                "options": {"format": "integer"},
                                            },
                                        ],
                                    }
                                },
                                "arrayElementTitle": "Second Array Title",
                                "showSortButtons": True,
                            },
                        }
                    ],
                },
                {
                    "type": "Section",
                    "label": "Horizontal Array",
                    "elements": [
                        {
                            "scope": "#/properties/model/properties/parameter_array_horizontal",
                            "type": "Control",
                            "label": "Horizontal Array",
                            "options": {
                                "addButtonText": "Add new parameter",
                                "detail": {
                                    "layout": {
                                        "type": "HorizontalLayout",
                                        "elements": [
                                            {
                                                "type": "Control",
                                                "scope": "#/properties/first",
                                                "label": "First Parameter",
                                                "options": {"format": "integer"},
                                            },
                                            {
                                                "type": "Control",
                                                "scope": "#/properties/second",
                                                "label": "Second Parameter",
                                                "options": {"format": "integer"},
                                            },
                                        ],
                                    }
                                },
                                "arrayElementTitle": "Group",
                            },
                        }
                    ],
                },
            ],
        }
        extracted = kp.extract_ui_schema(
            self.parameterized_with_parameter_array,
            DummyDialogCreationContext(),
        )
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

        with self.assertRaises(TypeError):
            self.parameterized.enum_set_param = 1

        with self.assertRaises(ValueError):
            self.parameterized_with_parameter_array_nested.parameter_array_first = [
                {"first": 1, "second": 5, "group": {"first": 1, "second": 5}}
            ]

        with self.assertRaises(ValueError):
            self.parameterized_with_parameter_array_nested.parameter_array_second = [
                {"first": 1, "second": 5, "third": [{"first": 1, "second": 5}]}
            ]

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

        with self.assertRaises(ValueError):
            self.parameterized.enum_set_param = ["Test"]

        # Check that the default type validators still work
        with self.assertRaises(TypeError):
            self.parameterized.parameter_group.third = "foo"

        with self.assertRaises(TypeError):
            self.parameterized.string_param = 1

        with self.assertRaises(TypeError):
            self.parameterized.enum_set_param = 1

        # Test custom validator for a valid parameter array (no nested groups or parameter arrays)
        expected_msg = "The sum of 'first' and 'second' must not exceed 8."
        with self.assertRaises(ValueError) as ve:
            self.parameterized_with_parameter_array.parameter_array_vertical = [
                {"first": 3, "second": 6}
            ]
        self.assertEqual(expected_msg, str(ve.exception))

        # Test custom validator set via constructor for a valid parameter array
        expected_msg = "The sum of 'first' and 'second' must not exceed 8."
        with self.assertRaises(ValueError) as ve:
            self.parameterized_with_parameter_array.parameter_array_horizontal = [
                {"first": 3, "second": 6}
            ]
        self.assertEqual(expected_msg, str(ve.exception))

        # Test default validator for an invalid parameter array, while a custom validator is set via decorator
        with self.assertRaises(ValueError) as ve:
            self.parameterized_with_parameter_array_nested.parameter_array_first = [
                {"first": 1, "second": 5, "group": {"first": 1, "second": 5}}
            ]
        self.assertNotEqual(
            expected_msg, str(ve.exception)
        )  # complains about nested array

        # Test default validator for an invalid parameter array, while a custom validator is set via constructor
        with self.assertRaises(ValueError) as ve:
            self.parameterized_with_parameter_array_nested.parameter_array_second = [
                {"first": 3, "second": 6, "group": [{"first": 1, "second": 5}]}
            ]
        self.assertNotEqual(
            expected_msg, str(ve.exception)
        )  # complains about nested array

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

    def test_all_pipelines(self):
        """
        Test getting and setting for simple parameters and parameter groups.
        Both descriptor-based and composed approaches are tested.
        """
        ##### descriptor-based #####
        # descriptor non-nested
        obj_descr_simple = self.parameterized_without_group
        obj_descr_simple = ParameterizedWithoutGroup()
        set_column_parameters(obj_descr_simple)
        # test getting
        self.assertEqual(obj_descr_simple.int_param, 3)
        # test setting
        obj_descr_simple.int_param = 42
        descr_simple_extracted = kp.extract_parameters(obj_descr_simple)
        descr_simple_expected = generate_values_dict_without_groups(42)
        self.assertEqual(descr_simple_extracted, descr_simple_expected)

        # descriptor one group
        obj_descr_one_group = ParameterizedWithOneGroup()
        set_column_parameters(obj_descr_one_group)
        # test getting
        self.assertEqual(obj_descr_one_group.parameter_group.first, 1)
        # test setting
        obj_descr_one_group.parameter_group.first = 42
        descr_one_group_extracted = kp.extract_parameters(obj_descr_one_group)
        descr_one_group_expected = generate_values_dict_with_one_group(first=42)
        self.assertEqual(descr_one_group_extracted, descr_one_group_expected)

        # descriptor nested groups
        obj_descr_nested_groups = Parameterized()
        set_column_parameters(obj_descr_nested_groups)
        # test getting
        self.assertEqual(obj_descr_nested_groups.parameter_group.subgroup.first, 1)
        # test setting
        obj_descr_nested_groups.parameter_group.subgroup.first = 42
        descr_nested_groups_extracted = kp.extract_parameters(obj_descr_nested_groups)
        descr_nested_groups_expected = generate_values_dict(first=42)
        self.assertEqual(descr_nested_groups_extracted, descr_nested_groups_expected)
        ##### composed #####
        # # composed non-nested (here `param` was also declared as a class-level descriptor)
        # obj_composed_simple = ComposedParameterizedWithoutGroup(54321)
        # # test getting
        # self.assertEqual(obj_composed_simple.param, 54321)
        # # test setting
        # obj_composed_simple.param = 42
        # composed_simple_extracted = kp.extract_parameters(obj_composed_simple)
        # composed_simple_expected = {"model": {"param": 42}}
        # self.assertEqual(composed_simple_extracted, composed_simple_expected)

        # composed one group
        obj_composed_one_group = ComposedParameterized()
        # test getting
        self.assertEqual(obj_composed_one_group.first_group.first_param, 12345)
        # test setting
        obj_composed_one_group.first_group.first_param = 42
        composed_one_group_extracted = kp.extract_parameters(obj_composed_one_group)
        composed_one_group_expected = {
            "model": {
                "first_group": {"first_param": 42, "second_param": 54321},
                "second_group": {"first_param": 12345, "second_param": 54321},
            }
        }
        self.assertEqual(composed_one_group_extracted, composed_one_group_expected)

        # composed nested groups
        obj_composed_nested_groups = NestedComposedParameterized()
        # test getting
        self.assertEqual(
            obj_composed_nested_groups.group.first_group.first_param, 12345
        )
        # test setting
        obj_composed_nested_groups.group.first_group.first_param = 42
        composed_nested_groups_extracted = kp.extract_parameters(
            obj_composed_nested_groups
        )
        composed_nested_groups_expected = (
            obj_composed_nested_groups.create_default_dict()
        )
        composed_nested_groups_expected["model"]["group"]["first_group"][
            "first_param"
        ] = 42

        self.assertEqual(
            composed_nested_groups_extracted, composed_nested_groups_expected
        )

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
        ui_schema = kp.extract_ui_schema(obj, DummyDialogCreationContext())
        expected = {
            "type": "VerticalLayout",
            "elements": [
                {
                    "type": "Section",
                    "label": "Parameter group to be used for multiple descriptor instances.",
                    "options": {},
                    "elements": [
                        {
                            "type": "VerticalLayout",
                            "elements": [
                                {
                                    "scope": "#/properties/model/properties/first_group/properties/first_param",
                                    "type": "Control",
                                    "label": "Plain int param",
                                    "options": {"format": "integer"},
                                },
                                {
                                    "scope": "#/properties/model/properties/first_group/properties/second_param",
                                    "type": "Control",
                                    "label": "Second int param",
                                    "options": {"format": "integer"},
                                },
                            ],
                        }
                    ],
                },
                {
                    "type": "Section",
                    "label": "Parameter group to be used for multiple descriptor instances.",
                    "options": {},
                    "elements": [
                        {
                            "type": "VerticalLayout",
                            "elements": [
                                {
                                    "scope": "#/properties/model/properties/second_group/properties/first_param",
                                    "type": "Control",
                                    "label": "Plain int param",
                                    "options": {"format": "integer"},
                                },
                                {
                                    "scope": "#/properties/model/properties/second_group/properties/second_param",
                                    "type": "Control",
                                    "label": "Second int param",
                                    "options": {"format": "integer"},
                                },
                            ],
                        }
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
                    {
                        "name": "Multiline String Parameter",
                        "description": "A multiline string parameter",
                    },
                    {"name": "Boolean Parameter", "description": "A boolean parameter"},
                    {"name": "Column Parameter", "description": "A column parameter"},
                    {
                        "name": "Multi Column Parameter",
                        "description": "A multi column parameter",
                    },
                    {
                        "name": "Full Multi Column Parameter",
                        "description": "A full multi column parameter",
                    },
                    {
                        "name": "EnumSet Parameter",
                        "description": "An EnumSet Parameter\n\n**Available options:**\n\n- Foo: The foo\n- Bar: The bar\n- Baz: The baz\n",
                    },
                    {
                        "name": "parameter_array",
                        "description": "\n\n**First Array Title:**\n\n- First Parameter : First parameter description\n- Second Parameter : Second parameter description\n",
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
            {
                "name": "Test group with layout type",
                "description": None,
                "options": [
                    {"name": "String Parameter", "description": "A string parameter"},
                    {"name": "Double Parameter", "description": "A double parameter"},
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
            {
                "name": "Full Multi Column Parameter",
                "description": "A full multi column parameter",
            },
            {
                "name": "EnumSet Parameter",
                "description": "An EnumSet Parameter\n"
                "\n"
                "**Available options:**\n"
                "\n"
                "- Foo: The foo\n"
                "- Bar: The bar\n"
                "- Baz: The baz\n",
            },
        ]
        description, use_tabs = kp.extract_parameter_descriptions(
            self.parameterized_without_group
        )
        self.assertFalse(use_tabs)
        self.assertEqual(description, expected)

    def test_extract_description_with_intendation(self):
        expected = [
            {
                "description": "\n                Any Text\n                \n\n                **Available options:**\n\n                - Default: This is the default option, since additional options have not been provided.\n",
                "name": "Indented Enum Parameter",
            },
            {
                "description": "\n                Any Text\n                \n\n                **Available options:**\n\n                - Default: This is the default option, since additional options have not been provided.\n",
                "name": "Indented Enum Parameter",
            },
        ]
        description, use_tabs = kp.extract_parameter_descriptions(
            self.parameterized_with_indented_docstring
        )

        self.assertEqual(description, expected)

    def test_extract_description_with_no_default_enum_set(self):
        expected = [
            {
                "description": "An EnumSet Parameter\n\n**Available options:**\n\n- Foo: The "
                "foo\n- Bar: The bar\n- Baz: The baz\n",
                "name": "EnumSet Parameter",
            },
            {
                "description": "An EnumSet Parameter\n\n**Available options:**\n\n- Default: This is the "
                "default option, since additional options have not been provided.\n",
                "name": "EnumSet Parameter",
            },
        ]
        description, use_tabs = kp.extract_parameter_descriptions(
            self.parameterized_with_enum_set_params
        )

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
                    'DEBUG:Python backend: - "EnumSet Parameter"',
                    'DEBUG:Python backend: - "First Parameter"',
                    # 0.1.0 -> 0.3.0: backward compatibility
                    "DEBUG:Python backend: The node was previously configured with an older version of the extension, 0.1.0, while the current version is 0.3.0.",
                    "DEBUG:Python backend: The following parameters have since been added, and are configured with their default values:",
                    'DEBUG:Python backend: - "String Parameter"',
                    'DEBUG:Python backend: - "Boolean Parameter"',
                    'DEBUG:Python backend: - "EnumSet Parameter"',
                    'DEBUG:Python backend: - "First Parameter"',
                    'DEBUG:Python backend: - "Second Parameter"',
                    # 0.2.0 -> 0.3.0: backward compatibility
                    "DEBUG:Python backend: The node was previously configured with an older version of the extension, 0.2.0, while the current version is 0.3.0.",
                    "DEBUG:Python backend: The following parameters have since been added, and are configured with their default values:",
                    'DEBUG:Python backend: - "Boolean Parameter"',
                    'DEBUG:Python backend: - "EnumSet Parameter"',
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

    def test_extract_ui_schema_with_custom_schema_provider(self):
        def _first_input_table(
            dialog_creation_context: kn.DialogCreationContext,
        ) -> ks.Schema:
            return dialog_creation_context.get_input_specs()[0]

        class Parameterized:
            column_selection = kn.ColumnParameter(
                "Column selection",
                "Column selection",
                schema_provider=_first_input_table,
            )
            column_filter = kn.ColumnFilterParameter(
                "Column filter", "Column filter", schema_provider=_first_input_table
            )

        expected = {
            "type": "VerticalLayout",
            "elements": [
                {
                    "scope": "#/properties/model/properties/column_selection",
                    "type": "Control",
                    "label": "Column selection",
                    "options": {
                        "format": "dropDown",
                        "showRowKeys": False,
                        "showNoneColumn": False,
                        "possibleValues": test_possible_values,
                    },
                },
                {
                    "scope": "#/properties/model/properties/column_filter",
                    "type": "Control",
                    "label": "Column filter",
                    "options": {
                        "format": "typedStringFilter",
                        "unknownValuesText": "Any unknown column",
                        "emptyStateLabel": "No columns in this list.",
                        "possibleValues": test_possible_values,
                    },
                },
            ],
        }
        extracted = kp.extract_ui_schema(Parameterized(), DummyDialogCreationContext())
        self.assertEqual(extracted, expected)

    def test_column_parameter_with_dynamic_ports(self):
        class Parameterized:
            column_selection = kn.ColumnParameter(
                "Column selection", "Column selection", port_index=(1, 0)
            )
            multi_column_param = kn.MultiColumnParameter(
                "Multi Column Parameter", "Multi Column Parameter", port_index=(1, 1)
            )
            column_filter = kn.ColumnFilterParameter(
                "Column filter", "Column filter", port_index=(2, 0)
            )

        table_specs = [
            ks.Schema.from_columns([test_columns["int"]]),  # never accessed anyways
            [
                ks.Schema.from_columns([test_columns["int"], test_columns["double"]]),
                ks.Schema.from_columns(
                    [test_columns["string"], test_columns["long list"]]
                ),
            ],
            [
                ks.Schema.from_columns([test_columns["string"]]),
            ],
        ]

        possible_values = [
            [test_values["int"]],  # never accessed anyways
            [
                [test_values["int"], test_values["double"]],
                [test_values["string"], test_values["long list"]],
            ],
            [[test_values["string"]]],
        ]

        expected = {
            "type": "VerticalLayout",
            "elements": [
                {
                    "scope": "#/properties/model/properties/column_selection",
                    "type": "Control",
                    "label": "Column selection",
                    "options": {
                        "format": "dropDown",
                        "showRowKeys": False,
                        "showNoneColumn": False,
                        "possibleValues": possible_values[1][0],
                    },
                },
                {
                    "scope": "#/properties/model/properties/multi_column_param",
                    "type": "Control",
                    "label": "Multi Column Parameter",
                    "options": {
                        "format": "twinList",
                        "possibleValues": possible_values[1][1],
                    },
                },
                {
                    "scope": "#/properties/model/properties/column_filter",
                    "type": "Control",
                    "label": "Column filter",
                    "options": {
                        "format": "typedStringFilter",
                        "unknownValuesText": "Any unknown column",
                        "emptyStateLabel": "No columns in this list.",
                        "possibleValues": possible_values[2][0],
                    },
                },
            ],
        }
        extracted = kp.extract_ui_schema(
            Parameterized(), DummyDialogCreationContext(table_specs)
        )
        self.assertEqual(extracted, expected)


class FullColumnSelectionTest(unittest.TestCase):
    def test_apply_manual_filter(self):
        schema = ks.Schema.from_types(
            [ks.string(), ks.double(), ks.int32()],
            ["string", "number_double", "number_int"],
        )

        selection = kp.ColumnFilterConfig(mode=kp.ColumnFilterMode.MANUAL)

        selection.manual_filter = kp.ManualFilterConfig()
        filtered = selection.apply(schema)
        self.assertEqual(
            ["string", "number_double", "number_int"], filtered.column_names
        )

        selection.manual_filter = kp.ManualFilterConfig(include_unknown_columns=False)
        filtered = selection.apply(schema)
        self.assertEqual([], filtered.column_names)

        selection.manual_filter = kp.ManualFilterConfig(
            include_unknown_columns=False, included=["string"]
        )
        filtered = selection.apply(schema)
        self.assertEqual(["string"], filtered.column_names)

        selection.manual_filter = kp.ManualFilterConfig(
            include_unknown_columns=True, included=["string"]
        )
        filtered = selection.apply(schema)
        self.assertEqual(
            ["string", "number_double", "number_int"], filtered.column_names
        )

        selection.manual_filter = kp.ManualFilterConfig(
            include_unknown_columns=True, excluded=["string"]
        )
        filtered = selection.apply(schema)
        self.assertEqual(["number_double", "number_int"], filtered.column_names)

    def test_apply_regex_filter(self):
        schema = ks.Schema.from_types(
            [ks.string(), ks.double(), ks.int32()],
            ["string", "number_double", "number_int"],
        )

        selection = kp.ColumnFilterConfig(mode=kp.ColumnFilterMode.REGEX)

        selection.pattern_filter = kp.PatternFilterConfig(pattern=".*")
        filtered = selection.apply(schema)
        self.assertEqual(
            ["string", "number_double", "number_int"], filtered.column_names
        )

        selection.pattern_filter = kp.PatternFilterConfig(inverted=True, pattern=".*")
        filtered = selection.apply(schema)
        self.assertEqual([], filtered.column_names)

        selection.pattern_filter = kp.PatternFilterConfig(pattern="Number")
        filtered = selection.apply(schema)
        self.assertEqual([], filtered.column_names)

        selection.pattern_filter = kp.PatternFilterConfig(
            case_sensitive=True, pattern="Number"
        )
        filtered = selection.apply(schema)
        self.assertEqual([], filtered.column_names)

        selection.pattern_filter = kp.PatternFilterConfig(
            case_sensitive=False, pattern="Number"
        )
        filtered = selection.apply(schema)
        self.assertEqual(["number_double", "number_int"], filtered.column_names)

        selection.pattern_filter = kp.PatternFilterConfig(
            case_sensitive=True, inverted=True, pattern="Number"
        )
        filtered = selection.apply(schema)
        self.assertEqual(
            ["string", "number_double", "number_int"], filtered.column_names
        )

        selection.pattern_filter = kp.PatternFilterConfig(
            case_sensitive=False, inverted=True, pattern="Number"
        )
        filtered = selection.apply(schema)
        self.assertEqual(["string"], filtered.column_names)

    def test_apply_wildcard_filter(self):
        schema = ks.Schema.from_types(
            [ks.string(), ks.double(), ks.int32()],
            ["string", "number[double]", "number[int]"],
        )

        selection = kp.ColumnFilterConfig(mode=kp.ColumnFilterMode.WILDCARD)

        selection.pattern_filter = kp.PatternFilterConfig(pattern="*")
        filtered = selection.apply(schema)
        self.assertEqual(
            ["string", "number[double]", "number[int]"], filtered.column_names
        )

        selection.pattern_filter = kp.PatternFilterConfig(inverted=True, pattern="*")
        filtered = selection.apply(schema)
        self.assertEqual([], filtered.column_names)

        selection.pattern_filter = kp.PatternFilterConfig(pattern="Number")
        filtered = selection.apply(schema)
        self.assertEqual([], filtered.column_names)

        selection.pattern_filter = kp.PatternFilterConfig(
            case_sensitive=True, pattern="Number"
        )
        filtered = selection.apply(schema)
        self.assertEqual([], filtered.column_names)

        selection.pattern_filter = kp.PatternFilterConfig(
            case_sensitive=False, pattern="Number*"
        )
        filtered = selection.apply(schema)
        self.assertEqual(["number[double]", "number[int]"], filtered.column_names)

        selection.pattern_filter = kp.PatternFilterConfig(
            case_sensitive=True, inverted=True, pattern="Number"
        )
        filtered = selection.apply(schema)
        self.assertEqual(
            ["string", "number[double]", "number[int]"], filtered.column_names
        )

        selection.pattern_filter = kp.PatternFilterConfig(
            case_sensitive=False, inverted=True, pattern="Number*"
        )
        filtered = selection.apply(schema)
        self.assertEqual(["string"], filtered.column_names)

        selection.pattern_filter = kp.PatternFilterConfig(
            case_sensitive=False, inverted=True, pattern="?umber*"
        )
        filtered = selection.apply(schema)
        self.assertEqual(["string"], filtered.column_names)

        selection.pattern_filter = kp.PatternFilterConfig(
            case_sensitive=True, inverted=False, pattern="?umber*"
        )
        filtered = selection.apply(schema)
        self.assertEqual(["number[double]", "number[int]"], filtered.column_names)

        selection.pattern_filter = kp.PatternFilterConfig(
            case_sensitive=True, inverted=False, pattern="number[*]"
        )
        filtered = selection.apply(schema)
        self.assertEqual(["number[double]", "number[int]"], filtered.column_names)

        selection.pattern_filter = kp.PatternFilterConfig(
            case_sensitive=True, inverted=False, pattern="number[*"
        )
        filtered = selection.apply(schema)
        self.assertEqual(["number[double]", "number[int]"], filtered.column_names)

    def test_apply_type_filter(self):
        schema = ks.Schema.from_types(
            [ks.string(), ks.double(), ks.int32()],
            ["string", "number[double]", "number[int]"],
            [
                {"preferred_value_type": "MyString"},
                {"preferred_value_type": "MyDouble"},
                {"preferred_value_type": "MyInt"},
            ],
        )

        selection = kp.ColumnFilterConfig(mode=kp.ColumnFilterMode.TYPE)

        selection.type_filter = kp.TypeFilterConfig(
            selected_types=["MyString"], type_displays=["My String"]
        )
        filtered = selection.apply(schema)
        self.assertEqual(["string"], filtered.column_names)

        selection.type_filter = kp.TypeFilterConfig(
            selected_types=["MyDouble"], type_displays=["My Double"]
        )
        filtered = selection.apply(schema)
        self.assertEqual(["number[double]"], filtered.column_names)

        selection.type_filter = kp.TypeFilterConfig(
            selected_types=["Random"], type_displays=["Random"]
        )
        filtered = selection.apply(schema)
        self.assertEqual([], filtered.column_names)

        selection.type_filter = kp.TypeFilterConfig(
            selected_types=["MyString", "MyDouble"],
            type_displays=["My String", "My Double"],
        )
        filtered = selection.apply(schema)
        self.assertEqual(["string", "number[double]"], filtered.column_names)

        schema = ks.Schema.from_types(
            [ks.string(), ks.double(), ks.int32()],
            ["string", "number[double]", "number[int]"],
            # No metadata given
        )
        selection.type_filter = kp.TypeFilterConfig(
            selected_types=["MyString", "MyDouble"],
            type_displays=["My String", "My Double"],
        )
        with self.assertLogs() as log:
            filtered = selection.apply(schema)
            self.assertEqual([], filtered.column_names)  # all columns will be dropped
            self.assertIn(
                "Ignoring column 'string' because it does not have a 'preferred_value_type' set.",
                log.output[0],
            )
            self.assertIn(
                "Ignoring column 'number[double]' because it does not have a 'preferred_value_type' set.",
                log.output[1],
            )
            self.assertIn(
                "Ignoring column 'number[int]' because it does not have a 'preferred_value_type' set.",
                log.output[2],
            )

    def test_apply_with_prefilter(self):
        selection = kp.ColumnFilterConfig(
            mode=kp.ColumnFilterMode.MANUAL, pre_filter=lambda c: c.name != "bar"
        )
        schema = ks.Schema.from_types([ks.string()] * 3, ["foo", "bar", "baz"])
        selection.manual_filter = kp.ManualFilterConfig(
            included=["foo"], excluded=["baz"], include_unknown_columns=True
        )
        filtered = selection.apply(schema)
        self.assertEqual(["foo"], filtered.column_names)


class DateTimeParameterTest(unittest.TestCase):
    def test_wrong_default_value(self):
        with self.assertRaises(ValueError):
            kp.DateTimeParameter(
                label="Wrong datetime",
                default_value="This is not a datetime",
                description="A datetime parameter",
            )
        with self.assertRaises(ValueError):
            kp.DateTimeParameter(
                label="Wrong datetime",
                default_value=12345,
                description="A datetime parameter",
            )

    def test_date_format(self):
        import datetime

        # test that date format is correctly handled
        date_format = "%Y-%m-%d"
        param = kp.DateTimeParameter(
            label="Date format",
            default_value="2023-02-01",
            description="A datetime parameter",
            show_time=False,
            date_format=date_format,
        )
        self.assertEqual(param._default_value, datetime.date(2023, 2, 1))
        date_format = "%Y-%d-%m"

        param = kp.DateTimeParameter(
            label="Date format",
            default_value="2023-01-02",
            description="A datetime parameter",
            show_time=False,
            date_format=date_format,
        )
        self.assertEqual(param._default_value, datetime.date(2023, 2, 1))


if __name__ == "__main__":
    unittest.main()
