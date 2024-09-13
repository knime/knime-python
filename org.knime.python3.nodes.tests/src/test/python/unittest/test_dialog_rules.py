import unittest
import knime.extension.parameter as kp
from test_knime_parameter import DummyDialogCreationContext, test_schema


class TestEnumOptions(kp.EnumParameterOptions):
    FOO = ("Foo", "The foo")
    BAR = ("Bar", "The bar")
    BAZ = ("Baz", "The baz")


@kp.parameter_group("Group with rules")
class ParameterGroupWithRules:
    string_param = kp.StringParameter("String Parameter", "A string parameter", "foo")
    int_param = kp.IntParameter(
        "Int Parameter",
        "Int parameter that is hidden if the string parameter is foo",
        42,
    ).rule(kp.OneOf(string_param, ["foo"]), kp.Effect.HIDE)

    statement_1 = kp.BoolParameter(
        "Bool Parameter",
        "Bool parameter that is used to test combined Conditions",
        True,
    )
    statement_2 = kp.BoolParameter(
        "Bool Parameter",
        "Bool parameter that is used to test combined Conditions",
        True,
    )
    bool_param_or = kp.BoolParameter(
        "Bool Parameter",
        "Bool parameter that is hidden if statement_1 or statement_2 is true",
    ).rule(
        kp.Or(kp.OneOf(statement_1, [True]), kp.OneOf(statement_2, [True])),
        kp.Effect.HIDE,
    )
    bool_param_and = kp.BoolParameter(
        "Bool Parameter",
        "Bool parameter that is hidden if statement_1 and statement_2 is true",
    ).rule(
        kp.And(kp.OneOf(statement_1, [True]), kp.OneOf(statement_2, [True])),
        kp.Effect.HIDE,
    )

    enum_set_param = kp.EnumSetParameter(
        "EnumSet Parameter",
        "An EnumSet Parameter",
        [TestEnumOptions.FOO.name, TestEnumOptions.BAR.name],
        TestEnumOptions,
    )
    bool_param_contains = kp.BoolParameter(
        "Bool Parameter",
        "Bool parameter that is showns if enum_set_param contains FOO",
        True,
    ).rule(kp.Contains(enum_set_param, TestEnumOptions.FOO.name), kp.Effect.HIDE)


class ParameterizedWithRule:
    string_param = kp.StringParameter("String Parameter", "A string parameter", "foo")
    int_param = kp.IntParameter(
        "Int Parameter",
        "Int parameter that is only shown the string parameter is foo",
        42,
    ).rule(kp.OneOf(string_param, ["foo"]), kp.Effect.SHOW)

    statement_1 = kp.BoolParameter(
        "Bool Parameter",
        "Bool parameter that is used to test combined Conditions",
        True,
    )
    statement_2 = kp.BoolParameter(
        "Bool Parameter",
        "Bool parameter that is used to test combined Conditions",
        True,
    )
    bool_param_or = kp.BoolParameter(
        "Bool Parameter",
        "Bool parameter that is only shown if statement_1 or statement_2 is true",
    ).rule(
        kp.Or(kp.OneOf(statement_1, [True]), kp.OneOf(statement_2, [True])),
        kp.Effect.SHOW,
    )
    bool_param_and = kp.BoolParameter(
        "Bool Parameter",
        "Bool parameter that is only shown if statement_1 and statement_2 is true",
    ).rule(
        kp.And(kp.OneOf(statement_1, [True]), kp.OneOf(statement_2, [True])),
        kp.Effect.SHOW,
    )

    enum_set_param = kp.EnumSetParameter(
        "EnumSet Parameter",
        "An EnumSet Parameter",
        [TestEnumOptions.FOO.name, TestEnumOptions.BAR.name],
        TestEnumOptions,
    )
    bool_param_contains = kp.BoolParameter(
        "Bool Parameter",
        "Bool parameter that is showns if enum_set_param contains FOO",
        True,
    ).rule(kp.Contains(enum_set_param, TestEnumOptions.FOO.name), kp.Effect.SHOW)

    group = ParameterGroupWithRules()


@kp.parameter_group("Composed group")
class ComposedGroup:
    def __init__(self) -> None:
        # instantiated here such that we can refer to the StringParameter object in the rule below
        string_param = kp.StringParameter("String parameter", "String parameter", "")
        self.string_param = string_param
        self.int_param = kp.IntParameter(
            "Int parameter",
            "Int parameter",
        ).rule(kp.OneOf(string_param, ["foo", "bar"]), kp.Effect.DISABLE)

        statement_1 = kp.BoolParameter(
            "Bool Parameter",
            "Bool parameter that is used to test combined Conditions",
            True,
        )
        self.statement_1 = statement_1
        statement_2 = kp.BoolParameter(
            "Bool Parameter",
            "Bool parameter that is used to test combined Conditions",
            True,
        )
        self.statement_2 = statement_2
        self.bool_param_or = kp.BoolParameter(
            "Bool Parameter",
            "Bool parameter that is disabled if statement_1 or statement_2 is true",
        ).rule(
            kp.Or(kp.OneOf(statement_1, [True]), kp.OneOf(statement_2, [True])),
            kp.Effect.DISABLE,
        )
        self.bool_param_and = kp.BoolParameter(
            "Bool Parameter",
            "Bool parameter that is disabled if statement_1 and statement_2 is true",
        ).rule(
            kp.And(kp.OneOf(statement_1, [True]), kp.OneOf(statement_2, [True])),
            kp.Effect.DISABLE,
        )

        enum_set_param = kp.EnumSetParameter(
            "EnumSet Parameter",
            "An EnumSet Parameter",
            [TestEnumOptions.FOO.name, TestEnumOptions.BAR.name],
            TestEnumOptions,
        )
        self.enum_set_param = enum_set_param
        self.bool_param_contains = kp.BoolParameter(
            "Bool Parameter",
            "Bool parameter that is disabled if enum_set_param contains FOO",
            True,
        ).rule(kp.Contains(enum_set_param, TestEnumOptions.FOO.name), kp.Effect.DISABLE)


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
                    "type": "Control",
                    "label": "Bool Parameter",
                    "scope": "#/properties/model/properties/statement_1",
                    "options": {"format": "boolean"},
                },
                {
                    "type": "Control",
                    "label": "Bool Parameter",
                    "scope": "#/properties/model/properties/statement_2",
                    "options": {"format": "boolean"},
                },
                {
                    "type": "Control",
                    "label": "Bool Parameter",
                    "scope": "#/properties/model/properties/bool_param_or",
                    "options": {"format": "boolean"},
                    "rule": {
                        "condition": {
                            "conditions": [
                                {
                                    "schema": {"oneOf": [{"const": True}]},
                                    "scope": "#/properties/model/properties/statement_1",
                                },
                                {
                                    "schema": {"oneOf": [{"const": True}]},
                                    "scope": "#/properties/model/properties/statement_2",
                                },
                            ],
                            "type": "OR",
                        },
                        "effect": "SHOW",
                    },
                },
                {
                    "type": "Control",
                    "label": "Bool Parameter",
                    "scope": "#/properties/model/properties/bool_param_and",
                    "options": {"format": "boolean"},
                    "rule": {
                        "condition": {
                            "conditions": [
                                {
                                    "schema": {"oneOf": [{"const": True}]},
                                    "scope": "#/properties/model/properties/statement_1",
                                },
                                {
                                    "schema": {"oneOf": [{"const": True}]},
                                    "scope": "#/properties/model/properties/statement_2",
                                },
                            ],
                            "type": "AND",
                        },
                        "effect": "SHOW",
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
                    "label": "Bool Parameter",
                    "options": {"format": "boolean"},
                    "rule": {
                        "condition": {
                            "schema": {"contains": {"const": "FOO"}},
                            "scope": "#/properties/model/properties/enum_set_param",
                        },
                        "effect": "SHOW",
                    },
                    "scope": "#/properties/model/properties/bool_param_contains",
                    "type": "Control",
                },
                {
                    "type": "Section",
                    "label": "Group with rules",
                    "options": {},
                    "elements": [
                        {
                            "type": "VerticalLayout",
                            "elements": [
                                {
                                    "scope": "#/properties/model/properties/group/properties/string_param",
                                    "type": "Control",
                                    "label": "String Parameter",
                                    "options": {"format": "string"},
                                },
                                {
                                    "scope": "#/properties/model/properties/group/properties/int_param",
                                    "type": "Control",
                                    "label": "Int Parameter",
                                    "options": {"format": "integer"},
                                    "rule": {
                                        "effect": "HIDE",
                                        "condition": {
                                            "scope": "#/properties/model/properties/group/properties/string_param",
                                            "schema": {"oneOf": [{"const": "foo"}]},
                                        },
                                    },
                                },
                                {
                                    "scope": "#/properties/model/properties/group/properties/statement_1",
                                    "type": "Control",
                                    "label": "Bool Parameter",
                                    "options": {"format": "boolean"},
                                },
                                {
                                    "scope": "#/properties/model/properties/group/properties/statement_2",
                                    "type": "Control",
                                    "label": "Bool Parameter",
                                    "options": {"format": "boolean"},
                                },
                                {
                                    "scope": "#/properties/model/properties/group/properties/bool_param_or",
                                    "type": "Control",
                                    "label": "Bool Parameter",
                                    "options": {"format": "boolean"},
                                    "rule": {
                                        "effect": "HIDE",
                                        "condition": {
                                            "type": "OR",
                                            "conditions": [
                                                {
                                                    "scope": "#/properties/model/properties/group/properties/statement_1",
                                                    "schema": {
                                                        "oneOf": [{"const": True}]
                                                    },
                                                },
                                                {
                                                    "scope": "#/properties/model/properties/group/properties/statement_2",
                                                    "schema": {
                                                        "oneOf": [{"const": True}]
                                                    },
                                                },
                                            ],
                                        },
                                    },
                                },
                                {
                                    "scope": "#/properties/model/properties/group/properties/bool_param_and",
                                    "type": "Control",
                                    "label": "Bool Parameter",
                                    "options": {"format": "boolean"},
                                    "rule": {
                                        "effect": "HIDE",
                                        "condition": {
                                            "type": "AND",
                                            "conditions": [
                                                {
                                                    "scope": "#/properties/model/properties/group/properties/statement_1",
                                                    "schema": {
                                                        "oneOf": [{"const": True}]
                                                    },
                                                },
                                                {
                                                    "scope": "#/properties/model/properties/group/properties/statement_2",
                                                    "schema": {
                                                        "oneOf": [{"const": True}]
                                                    },
                                                },
                                            ],
                                        },
                                    },
                                },
                                {
                                    "scope": "#/properties/model/properties/group/properties/enum_set_param",
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
                                    "scope": "#/properties/model/properties/group/properties/bool_param_contains",
                                    "type": "Control",
                                    "label": "Bool Parameter",
                                    "options": {"format": "boolean"},
                                    "rule": {
                                        "effect": "HIDE",
                                        "condition": {
                                            "scope": "#/properties/model/properties/group/properties/enum_set_param",
                                            "schema": {"contains": {"const": "FOO"}},
                                        },
                                    },
                                },
                            ],
                        }
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
                            "type": "VerticalLayout",
                            "elements": [
                                {
                                    "scope": "#/properties/model/properties/group/properties/string_param",
                                    "type": "Control",
                                    "label": "String parameter",
                                    "options": {"format": "string"},
                                },
                                {
                                    "scope": "#/properties/model/properties/group/properties/int_param",
                                    "type": "Control",
                                    "label": "Int parameter",
                                    "options": {"format": "integer"},
                                    "rule": {
                                        "effect": "DISABLE",
                                        "condition": {
                                            "scope": "#/properties/model/properties/group/properties/string_param",
                                            "schema": {
                                                "oneOf": [
                                                    {"const": "foo"},
                                                    {"const": "bar"},
                                                ]
                                            },
                                        },
                                    },
                                },
                                {
                                    "scope": "#/properties/model/properties/group/properties/statement_1",
                                    "type": "Control",
                                    "label": "Bool Parameter",
                                    "options": {"format": "boolean"},
                                },
                                {
                                    "scope": "#/properties/model/properties/group/properties/statement_2",
                                    "type": "Control",
                                    "label": "Bool Parameter",
                                    "options": {"format": "boolean"},
                                },
                                {
                                    "scope": "#/properties/model/properties/group/properties/bool_param_or",
                                    "type": "Control",
                                    "label": "Bool Parameter",
                                    "options": {"format": "boolean"},
                                    "rule": {
                                        "effect": "DISABLE",
                                        "condition": {
                                            "type": "OR",
                                            "conditions": [
                                                {
                                                    "scope": "#/properties/model/properties/group/properties/statement_1",
                                                    "schema": {
                                                        "oneOf": [{"const": True}]
                                                    },
                                                },
                                                {
                                                    "scope": "#/properties/model/properties/group/properties/statement_2",
                                                    "schema": {
                                                        "oneOf": [{"const": True}]
                                                    },
                                                },
                                            ],
                                        },
                                    },
                                },
                                {
                                    "scope": "#/properties/model/properties/group/properties/bool_param_and",
                                    "type": "Control",
                                    "label": "Bool Parameter",
                                    "options": {"format": "boolean"},
                                    "rule": {
                                        "effect": "DISABLE",
                                        "condition": {
                                            "type": "AND",
                                            "conditions": [
                                                {
                                                    "scope": "#/properties/model/properties/group/properties/statement_1",
                                                    "schema": {
                                                        "oneOf": [{"const": True}]
                                                    },
                                                },
                                                {
                                                    "scope": "#/properties/model/properties/group/properties/statement_2",
                                                    "schema": {
                                                        "oneOf": [{"const": True}]
                                                    },
                                                },
                                            ],
                                        },
                                    },
                                },
                                {
                                    "scope": "#/properties/model/properties/group/properties/enum_set_param",
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
                                    "scope": "#/properties/model/properties/group/properties/bool_param_contains",
                                    "type": "Control",
                                    "label": "Bool Parameter",
                                    "options": {"format": "boolean"},
                                    "rule": {
                                        "effect": "DISABLE",
                                        "condition": {
                                            "scope": "#/properties/model/properties/group/properties/enum_set_param",
                                            "schema": {"contains": {"const": "FOO"}},
                                        },
                                    },
                                },
                            ],
                        }
                    ],
                }
            ],
        }
        extracted = kp.extract_ui_schema(
            ParameterizedWithComposedGroup(), DummyDialogCreationContext()
        )
        self.assertEqual(expected, extracted)

    def test_existing_rule_attribute_not_overwritten(self):
        with self.assertLogs("Python backend", "WARN"):

            @kp.parameter_group("Group with rule attribute")
            class Group:
                rule = kp.StringParameter("Rule", "Rule", "foo")

            class Parameterized:
                group = Group()

            obj = Parameterized()
            self.assertEqual("foo", obj.group.rule)

    def test_condition_initialization_errors(self):
        with self.assertRaises(ValueError):
            kp.BoolParameter(
                "Bool Parameter",
                "Bool parameter that is missing Conditions",
            ).rule(
                kp.Or(),
                kp.Effect.SHOW,
            )
        with self.assertRaises(ValueError):
            kp.BoolParameter(
                "Bool Parameter",
                "Bool parameter that is missing Conditions",
            ).rule(
                kp.And(),
                kp.Effect.SHOW,
            )

    def test_dialog_context_condition(self):
        class Parameterized:
            conditional_param = kp.BoolParameter(
                "Conditional Parameter",
                "Conditional param that is only shown if there is exactly one input spec.",
            ).rule(
                kp.DialogContextCondition(lambda ctx: len(ctx.get_input_specs()) == 1),
                kp.Effect.SHOW,
            )

        expected_when_condition_true = {
            "type": "VerticalLayout",
            "elements": [
                {
                    "type": "Control",
                    "label": "Conditional Parameter",
                    "scope": "#/properties/model/properties/conditional_param",
                    "options": {"format": "boolean"},
                    "rule": {
                        "condition": {"scope": "#", "schema": {"type": ["object"]}},
                        "effect": "SHOW",
                    },
                }
            ],
        }

        self.assertEqual(
            expected_when_condition_true,
            kp.extract_ui_schema(Parameterized(), DummyDialogCreationContext()),
        )

        expected_when_condition_false = {
            "type": "VerticalLayout",
            "elements": [
                {
                    "type": "Control",
                    "label": "Conditional Parameter",
                    "scope": "#/properties/model/properties/conditional_param",
                    "options": {"format": "boolean"},
                    "rule": {
                        "condition": {"scope": "#", "schema": {"type": ["null"]}},
                        "effect": "SHOW",
                    },
                }
            ],
        }
        self.assertEqual(
            expected_when_condition_false,
            kp.extract_ui_schema(
                Parameterized(), DummyDialogCreationContext([test_schema] * 2)
            ),
        )
