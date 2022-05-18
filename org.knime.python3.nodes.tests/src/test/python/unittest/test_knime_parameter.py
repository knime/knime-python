import unittest

import pythonpath
import knime_parameter as kp


def generate_values_dict(
    int_param=3,
    double_param=1.5,
    string_param="foo",
    bool_param=True,
    first=1,
    second=5,
    third=3,
):
    return {
        "int_param": int_param,
        "double_param": double_param,
        "string_param": string_param,
        "bool_param": bool_param,
        "parameter_group": {
            "subgroup": {"first": first, "second": second},
            "third": third,
        },
    }


#### Primary parameterised object for testing all functionality: ####
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


@kp.parameter_group("Primary Group")
class ParameterGroup:
    """
    A parameter group which contains a parameter group as a subgroup, and sets a
    custom validator for a parameter.
    """

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

    @subgroup.validator
    def validate_subgroup(values, version=None):
        if values["first"] + values["second"] > 10:
            raise ValueError("The sum of the parameters may not exceed 10.")


class Parameterized:
    int_param = kp.IntParameter("Int Parameter", "An integer parameter", 3)
    double_param = kp.DoubleParameter("Double Parameter", "A double parameter", 1.5)
    string_param = kp.StringParameter("String Parameter", "A string parameter", "foo")
    bool_param = kp.BoolParameter("Boolean Parameter", "A boolean parameter", True)

    parameter_group = ParameterGroup()

    @string_param.validator
    def validate_string_param(value):
        if len(value) > 10:
            raise ValueError(f"Length of string must not exceed 10!")


#### Secondary parameterised object for testing group independance: ####
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


class ParameterisedWithTwoGroups:
    first_group = ReusableGroup()
    second_group = ReusableGroup()


#### Tests: ####
class ParameterTest(unittest.TestCase):
    def setUp(self):
        self.parameterized = Parameterized()
        self.parameterised_with_two_groups = ParameterisedWithTwoGroups()

        self.maxDiff = None

    def test_getting_parameters(self):
        """
        Test that parameter values can be retrieved.
        """
        # root-level parameters
        self.assertEqual(self.parameterized.int_param, 3)
        self.assertEqual(self.parameterized.double_param, 1.5)
        self.assertEqual(self.parameterized.string_param, "foo")
        self.assertEqual(self.parameterized.bool_param, True)

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

        # group-level parameters
        self.assertEqual(self.parameterized.int_param, 5)
        self.assertEqual(self.parameterized.double_param, 5.5)
        self.assertEqual(self.parameterized.string_param, "bar")
        self.assertEqual(self.parameterized.bool_param, False)

        # subgroup-level parameters
        self.parameterized.parameter_group.subgroup.first = 2
        self.assertEqual(self.parameterized.parameter_group.subgroup.first, 2)

    def test_default_validators(self):
        """
        Test the default type-checking validators provided with each parameter class.
        """
        with self.assertRaises(TypeError):
            self.parameterized.int_param = "foo"

        with self.assertRaises(TypeError):
            self.parameterized.double_param = 1

        with self.assertRaises(TypeError):
            self.parameterized.string_param = 1

        with self.assertRaises(TypeError):
            self.parameterized.bool_param = 1

    def test_extracting_parameters(self):
        """
        Test extracting nested parameter values.
        """
        params = kp.extract_parameters(self.parameterized)
        expected = generate_values_dict()
        self.assertEqual(params, expected)

    def test_inject_parameters(self):
        params = generate_values_dict(4, 2.7, "bar", False, 3, 2, 1)
        # TODO versioning
        kp.inject_parameters(self.parameterized, params, version=None)
        extracted = kp.extract_parameters(self.parameterized)
        self.assertEqual(params, extracted)

    def test_group_individual_validation(self):
        """
        Test caling validators on individual parameters of a group/parameterised object on-demand.
        """
        params = generate_values_dict(first=5, second=7)
        with self.assertRaises(ValueError):
            # TODO versioning
            kp.validate_parameters(self.parameterized, params, version=None)

    def test_extract_schema(self):
        expected = {
            "type": "object",
            "properties": {
                "int_param": {"type": "integer"},
                "double_param": {"type": "number"},
                "string_param": {"type": "string"},
                "bool_param": {"type": "boolean"},
                "parameter_group": {
                    "type": "object",
                    "properties": {
                        "subgroup": {
                            "properties": {
                                "first": {"type": "integer"},
                                "second": {"type": "integer"},
                            },
                            "type": "object",
                        },
                        "third": {"type": "integer"},
                    },
                },
            },
        }
        extracted = kp.extract_schema(self.parameterized)
        self.assertEqual(expected, extracted)

    def test_extract_ui_schema(self):
        expected = {
            "elements": [
                {
                    "label": "Int Parameter",
                    "options": {"format": "integer"},
                    "scope": "#/properties/int_param",
                    "type": "Control",
                },
                {
                    "label": "Double Parameter",
                    "options": {"format": "number"},
                    "scope": "#/properties/double_param",
                    "type": "Control",
                },
                {
                    "label": "String Parameter",
                    "options": {"format": "string"},
                    "scope": "#/properties/string_param",
                    "type": "Control",
                },
                {
                    "label": "Boolean Parameter",
                    "options": {"format": "boolean"},
                    "scope": "#/properties/bool_param",
                    "type": "Control",
                },
                {
                    "elements": [
                        {
                            "elements": [
                                {
                                    "label": "First Parameter",
                                    "options": {"format": "integer"},
                                    "scope": "#/properties/parameter_group/properties/subgroup/properties/first",
                                    "type": "Control",
                                },
                                {
                                    "label": "Second Parameter",
                                    "options": {"format": "integer"},
                                    "scope": "#/properties/parameter_group/properties/subgroup/properties/second",
                                    "type": "Control",
                                },
                            ],
                            "label": "Subgroup",
                            "type": "Group",
                        },
                        {
                            "label": "Internal int Parameter",
                            "options": {"format": "integer"},
                            "scope": "#/properties/parameter_group/properties/third",
                            "type": "Control",
                        },
                    ],
                    "label": "Primary Group",
                    "type": "Section",
                },
            ],
            "type": "VerticalLayout",
        }
        extracted = kp.extract_ui_schema(self.parameterized)
        self.assertEqual(expected, extracted)

    def test_group_independence(self):
        """
        Test that instances of the same parameter group class don't share references to the
        same parameter instances. Parameter values are fetched from the __parameters__ dictionary
        of the root object, so, given correct setting and fetching, parameter values should be independent for
        multiple descriptor instances.
        """
        self.parameterised_with_two_groups.first_group.first_param = 54321
        self.parameterised_with_two_groups.second_group.second_param = 12345

        self.assertEqual(
            self.parameterised_with_two_groups.second_group.first_param, 12345
        )
        self.assertEqual(
            self.parameterised_with_two_groups.first_group.second_param, 54321
        )

    def test_custom_validators(self):
        """
        Test custom validators for parameters.

        Note: custom validators can currently only be set inside of the parameter
        group class declaration.
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

    def test_groups_are_independent(self):
        obj1 = Parameterized()
        obj2 = Parameterized()
        group1 = obj1.parameter_group
        group1.third = 5
        self.assertEqual(group1.third, 5)
        obj2.parameter_group.third = 7
        self.assertEqual(group1.third, 5)

    def test_inject_validates(self):
        pass  # TODO
        # injection of custom parameter/parameter group validators can only be done
        # inside their "parent" class declaration


if __name__ == "__main__":
    unittest.main()
