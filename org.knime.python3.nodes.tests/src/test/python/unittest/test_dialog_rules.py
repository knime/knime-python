import unittest
import knime.extension.parameter as kp
from test_knime_parameter import DummyDialogCreationContext


@kp.parameter_group("Group with rules")
class ParameterGroupWithRules:
    string_param = kp.StringParameter("String Parameter", "A string parameter", "foo")
    int_param = kp.IntParameter(
        "Int Parameter",
        "Int parameter that is hidden if the string parameter is foo",
        42,
    ).rule(kp.OneOf(string_param, ["foo"]), kp.Effect.HIDE)


class ParameterizedWithRule:
    string_param = kp.StringParameter("String Parameter", "A string parameter", "foo")
    int_param = kp.IntParameter(
        "Int Parameter",
        "Int parameter that is only shown the string parameter is foo",
        42,
    ).rule(kp.OneOf(string_param, ["foo"]), kp.Effect.SHOW)

    group = ParameterGroupWithRules()


@kp.parameter_group("Composed group")
class ComposedGroup:
    def __init__(self) -> None:
        string_param = kp.StringParameter("String parameter", "String parameter", "")
        self.string_param = string_param
        self.int_param = kp.IntParameter(
            "Int parameter",
            "Int parameter",
        ).rule(kp.OneOf(string_param, ["foo", "bar"]), kp.Effect.DISABLE)


class ParameterizedWithComposedGroup:
    def __init__(self) -> None:
        self.group = ComposedGroup()


class RulesTest(unittest.TestCase):
    def test_dialog_rules(self):
        expected = {
            "type": "VerticalLayout",
            "elements": [
                {
                    "type": "Control",
                    "label": "String Parameter",
                    "scope": "#/properties/model/properties/string_param",
                    "options": {"format": "string"},
                },
                {
                    "type": "Control",
                    "label": "Int Parameter",
                    "scope": "#/properties/model/properties/int_param",
                    "options": {"format": "integer"},
                    "rule": {
                        "effect": "SHOW",
                        "condition": {
                            "scope": "#/properties/model/properties/string_param",
                            "schema": {"oneOf": [{"const": "foo"}]},
                        },
                    },
                },
                {
                    "type": "Section",
                    "label": "Group with rules",
                    "options": {},
                    "elements": [
                        {
                            "type": "Control",
                            "label": "String Parameter",
                            "scope": "#/properties/model/properties/group/properties/string_param",
                            "options": {"format": "string"},
                        },
                        {
                            "type": "Control",
                            "label": "Int Parameter",
                            "scope": "#/properties/model/properties/group/properties/int_param",
                            "options": {"format": "integer"},
                            "rule": {
                                "effect": "HIDE",
                                "condition": {
                                    "scope": "#/properties/model/properties/group/properties/string_param",
                                    "schema": {"oneOf": [{"const": "foo"}]},
                                },
                            },
                        },
                    ],
                },
            ],
        }

        extracted = kp.extract_ui_schema(
            ParameterizedWithRule(), DummyDialogCreationContext()
        )
        self.assertEqual(expected, extracted)

    def test_dialog_rules_with_composition(self):
        expected = {
            "type": "VerticalLayout",
            "elements": [
                {
                    "type": "Section",
                    "label": "Composed group",
                    "options": {},
                    "elements": [
                        {
                            "type": "Control",
                            "label": "String parameter",
                            "scope": "#/properties/model/properties/group/properties/string_param",
                            "options": {"format": "string"},
                        },
                        {
                            "type": "Control",
                            "label": "Int parameter",
                            "scope": "#/properties/model/properties/group/properties/int_param",
                            "options": {"format": "integer"},
                            "rule": {
                                "effect": "DISABLE",
                                "condition": {
                                    "scope": "#/properties/model/properties/group/properties/string_param",
                                    "schema": {
                                        "oneOf": [{"const": "foo"}, {"const": "bar"}]
                                    },
                                },
                            },
                        },
                    ],
                },
            ],
        }
        extracted = kp.extract_ui_schema(
            ParameterizedWithComposedGroup(), DummyDialogCreationContext()
        )
        self.assertEqual(expected, extracted)
