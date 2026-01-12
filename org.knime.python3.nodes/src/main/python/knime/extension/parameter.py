# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------
#  Copyright by KNIME AG, Zurich, Switzerland
#  Website: http://www.knime.com; Email: contact@knime.com
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License, Version 3, as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, see <http://www.gnu.org/licenses>.
#
#  Additional permission under GNU GPL version 3 section 7:
#
#  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
#  Hence, KNIME and ECLIPSE are both independent programs and are not
#  derived from each other. Should, however, the interpretation of the
#  GNU GPL Version 3 ("License") under any applicable laws result in
#  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
#  you the additional permission to use and propagate KNIME together with
#  ECLIPSE with only the license terms in place for ECLIPSE applying to
#  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
#  license terms of ECLIPSE themselves allow for the respective use and
#  propagation of ECLIPSE together with KNIME.
#
#  Additional permission relating to nodes for KNIME that extend the Node
#  Extension (and in particular that are based on subclasses of NodeModel,
#  NodeDialog, and NodeView) and that only interoperate with KNIME through
#  standard APIs ("Nodes"):
#  Nodes are deemed to be separate and independent programs and to not be
#  covered works.  Notwithstanding anything to the contrary in the
#  License, the License does not apply to Nodes, you are not required to
#  license Nodes under the License, and you are granted a license to
#  prepare and propagate Nodes, in each case even if such Nodes are
#  propagated with or for interoperation with KNIME.  The owner of a Node
#  may freely choose the license terms applicable to such Node, including
#  when such Node is propagated with or for interoperation with KNIME.
# ------------------------------------------------------------------------

"""
Contains the implementation of the Parameter Dialogue API for building native Python nodes in KNIME.

@author Ivan Prigarin, KNIME GmbH, Konstanz, Germany
@author Adrian Nembach, KNIME GmbH, Konstanz, Germany
"""

import datetime
import inspect
import logging
import numbers
import os
import sys
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union, Tuple

import pytz
from dateutil import parser

import knime.api.schema as ks
from knime.extension.version import Version

LOGGER = logging.getLogger("Python backend")


def _get_parameters(obj) -> Dict[str, "_BaseParameter"]:
    """
    Get all top-level parameter objects from obj, which can be the root parameterized
    object or a parameter group.

    Parameters
    ----------
    obj : object
        The root parameterized object or a parameter group.

    Returns
    -------
    list
        A list of top-level parameter objects.
    """
    class_params = {}
    instance_params = {}
    ancestor_classes = inspect.getmro(type(obj))[:-1]  # omit the base `object` class

    for cls in ancestor_classes:
        for param_name, param_obj in cls.__dict__.items():
            if _is_parameter_or_group(param_obj) and param_name not in class_params:
                class_params[param_name] = param_obj

    for param_name, param_obj in obj.__dict__.items():
        if _is_parameter_or_group(param_obj) and param_name not in instance_params:
            instance_params[param_name] = param_obj

    return {**class_params, **instance_params}


def extract_parameters(obj, for_dialog: bool = False) -> dict:
    """
    Get all parameter values from obj as a nested dict.
    """
    return {"model": _extract_parameters(obj, for_dialog)}


def _extract_parameters(obj, for_dialog: bool) -> dict:
    params = _get_parameters(obj)
    return {
        name: param_obj._extract_value(obj, name, for_dialog)
        for name, param_obj in params.items()
    }


def inject_parameters(
    obj,
    parameters: dict,
    parameters_version: str = None,
    exclude_validations: bool = False,
) -> None:
    """
    This method injects the provided values into the parameter descriptors of the parameterized object,
    which can be a node or a parameter group.
    """
    parameters_version = Version.parse_version(parameters_version)
    _inject_parameters(
        obj, parameters["model"], parameters_version, exclude_validations
    )


def _inject_parameters(
    obj,
    parameters: dict,
    parameters_version: Version,
    exclude_validations: bool = False,
) -> None:
    # Modify the parameters dict if the node has a _modify_parameters() method
    if hasattr(obj, "_modify_parameters"):
        parameters = obj._modify_parameters(parameters)
    for name, param_obj in _get_parameters(obj).items():
        if name in parameters:
            # Check if name is available as it might have been added by _modify_parameters above
            # In that case we don't care about the since_version
            param_obj._inject(
                obj, parameters[name], name, parameters_version, exclude_validations
            )
        elif param_obj._since_version > parameters_version:
            # The parameter was introduced in a newer version but we might want to initialize
            # the default based on the version the workflow was created with
            param_obj._set_default_for_version(
                obj, name, parameters_version, exclude_validations
            )


def validate_parameters(obj, parameters: dict, saved_version: str = None) -> None:
    """
    Perform validation on the individual parameters.

    Parameters
    ----------
    obj : object
        The object to perform validation on, which can be a node or a parameter group.

    Raises
    ------
    ValueError
        If a parameter violates its validator.
    """
    saved_version = Version.parse_version(saved_version)
    _validate_parameters(obj, parameters["model"], saved_version)


def _validate_parameters(obj, parameters: dict, saved_version: Version) -> None:
    for name, param_obj in _get_parameters(obj).items():
        # NB: If the settings were saved with a version that did not have this parameter
        # yet we do not need to validate it because it will be set to the default value
        if param_obj._since_version <= saved_version:
            if name in parameters:
                param_obj._validate(parameters[name], saved_version)
            else:
                raise ValueError(f'Parameter missing for key "{name}".')


def _combine_validators(new_validator, old_validator):
    def combined_validator(value):
        old_validator(value)
        new_validator(value)

    return combined_validator


def validate_specs(obj, specs) -> None:
    """
    Checks if the provided specs are compatible with the parameter of obj.

    Parameters
    ----------
    obj : object
        The object whose parameter compatibility is being checked.
    specs : any
        The specifications that need to be compatible with the parameter.

    Returns
    -------
    bool
        True if the specs are compatible with the parameter, False otherwise.
    """
    for param in _get_parameters(obj).values():
        param._validate_specs(specs)


def extract_schema(
    obj, extension_version: str = None, dialog_creation_context=None
) -> dict:
    """
    Extracts the schema of an object.

    Parameters
    ----------
    obj : object
        The object from which to extract the schema.
    extension_version : str, optional
        The version of the extension. Default is None.
    dialog_creation_context : object, optional
        The context of the dialog creation. Default is None.

    Returns
    -------
    dict
        A dictionary representing the schema of the input object.

    """
    extension_version = Version.parse_version(extension_version)
    return {
        "type": "object",
        "properties": {
            "model": _extract_schema(obj, extension_version, dialog_creation_context)
        },
    }


def _extract_schema(
    obj, extension_version: Version, dialog_creation_context=None
) -> dict:
    properties = {
        name: param_obj._extract_schema(
            extension_version=extension_version,
            dialog_creation_context=dialog_creation_context,
        )
        for name, param_obj in _get_parameters(obj).items()
        # the schema needs to be versioned because it is not only needed for the
        # dialog but also to load settings that were stored with earlier versions
        # of the extension
        if param_obj._since_version <= extension_version
    }
    return {"type": "object", "properties": properties}


def extract_ui_schema(obj, dialog_creation_context) -> dict:
    return _UISchemaExtractor(obj, dialog_creation_context).extract_ui_schema()


def extract_parameter_descriptions(obj) -> dict:
    """
    Extract parameter descriptions from an object.

    Parameters
    ----------
    obj : object
        The object from which to extract the parameter descriptions.

    Returns
    -------
    tuple
        A tuple containing a list of parameter descriptions and a boolean value indicating if there are group options present.

    Raises
    ------
    None

    Notes
    -----
    The `obj` parameter should be a dictionary-like object.

    Examples
    --------
    >>> obj = {'param1': {'name': 'Parameter 1', 'description': 'Description 1'},
               'param2': {'name': 'Parameter 2', 'description': 'Description 2', 'options': {...}}}
    >>> extract_parameter_descriptions(obj)
    ([{'name': 'Options', 'description': '', 'options': [{'name': 'Parameter 2', 'description': 'Description 2', 'options': {...}}]},
      {'name': 'Parameter 1', 'description': 'Description 1'}], True)
    """
    descriptions = _extract_parameter_descriptions(obj, _Scope("#/properties"))
    if any(map(_is_group, _get_parameters(obj).values())):
        # a top-level parameter_group is represented as tab in the dialog
        # tab descriptions are the only descriptions with nested options
        tabs = [
            tab_description
            for tab_description in descriptions
            if "options" in tab_description
        ]
        top_level_options = [
            description for description in descriptions if "options" not in description
        ]
        if len(top_level_options) > 0:
            options_tab = {
                "name": "Options",
                "description": "",
                "options": top_level_options,
            }
            tabs.insert(0, options_tab)
        return tabs, True
    else:
        return descriptions, False


def _extract_parameter_descriptions(obj, scope: "_Scope"):
    result = []
    for param_name, param_obj in _get_parameters(obj).items():
        result.append(param_obj._extract_description(param_name, scope))
    return result


def determine_compatability(
    obj, saved_version: str, current_version: str, saved_parameters: dict
) -> None:
    """
    Determine if backward/forward compatibility is taking place based on the
    saved and current version of the extension, and provide appropriate feedback
    to the user.

    When this function is called, the saved and current versions are already known to be different.
    """
    saved_version = Version.parse_version(saved_version)
    current_version = Version.parse_version(current_version)
    saved_parameters = saved_parameters["model"]
    _determine_compatability(obj, saved_version, current_version, saved_parameters)


def _determine_compatability(
    obj, saved_version: Version, current_version: Version, saved_parameters: dict
) -> None:
    missing_params = _detect_missing_parameters(obj, saved_parameters, current_version)
    if saved_version < current_version:
        # backward compatibilitiy
        LOGGER.debug(
            f" The node was previously configured with an older version of the extension, {saved_version}, while the current version is {current_version}."
        )
        if missing_params:
            LOGGER.debug(
                " The following parameters have since been added, and are configured with their default values:"
            )
            for param in missing_params:
                LOGGER.debug(f' - "{param}"')
    else:
        # forward compatibility (not supported)
        LOGGER.error(
            f" The node was previously configured with a newer version of the extension, {saved_version}, while the current version is {current_version}."
        )
        LOGGER.error(" The node might not work as expected without being reconfigured.")


def _detect_missing_parameters(
    obj, saved_parameters: dict, current_version: Version
) -> list:
    """
    Returns a list containing the names of the parameters available in the installed version of the extension
    that could not be found in the saved parameters of the node.
    """
    result = []
    for name, param_obj in _get_parameters(obj).items():
        if param_obj._since_version <= current_version:
            if _is_group(param_obj):
                group_params = saved_parameters.get(name, {})
                result.extend(
                    _detect_missing_parameters(param_obj, group_params, current_version)
                )
            elif name not in saved_parameters:
                result.append(param_obj._label)

    return result


def _is_group(param):
    return hasattr(param, "__kind__") and param.__kind__ == "parameter_group"


def _is_array(param):
    return hasattr(param, "__kind__") and param.__kind__ == "parameter_array"


def _is_parameter_or_group(obj) -> bool:
    return hasattr(obj, "__kind__")


def _create_param_dict_if_not_exists(obj):
    """
    Create a parameter dictionary if it doesn't exist.
    """
    if not hasattr(obj, "__parameters__"):
        obj.__parameters__ = {}


class _Scope:
    def __init__(self, scopes) -> None:
        if isinstance(scopes, str):
            self._scopes = [scopes]
        else:
            self._scopes = scopes

    def __str__(self) -> str:
        return "/".join(self._scopes)

    def create_child(self, child_name, is_group=False) -> "_Scope":
        if is_group:
            child_name = child_name + "/properties"
        scopes_copy = self._scopes.copy()
        scopes_copy.append(child_name)
        return _Scope(scopes_copy)

    def level(self):
        # root has level 0
        return len(self._scopes) - 1


def _default_validator(t):
    pass


T = TypeVar("T")
DefaultValueProvider = Callable[[Version], T]
"""A DefaultValueProvider is a Callable that given a version produces the default value of its corresponding parameter for that version."""

# set by knime.extension_backend when an extension is loaded
_extension_version = None


def set_extension_version(version: str):
    global _extension_version
    _extension_version = Version.parse_version(version)


def _get_extension_version() -> Version:
    if _extension_version is None:
        raise RuntimeError("The extension version has not been set by the backend.")
    return _extension_version


class Condition(ABC):
    """Abstract base class for all condition types of parameter visibility rules."""

    @abstractmethod
    def to_dict(self, find_scope: Callable[[Any], _Scope], ctx):
        """Converts the Condition into a dict that is JSON serializable."""


class And(Condition):
    """
    A Condition that combines other Conditions with AND.
    """

    def __init__(self, *conditions: Condition) -> None:
        """ """
        if not conditions:
            raise ValueError("At least one condition is required in And condition.")

        super().__init__()
        self._conditions = conditions

    def to_dict(self, find_scope: Callable[[Any], _Scope], ctx):
        return {
            "type": "AND",
            "conditions": [c.to_dict(find_scope, ctx) for c in self._conditions],
        }


class Or(Condition):
    """
    A Condition that combines other Conditions with OR.
    """

    def __init__(self, *conditions: Condition) -> None:
        """ """
        if not conditions:
            raise ValueError("At least one condition is required in Or condition.")

        super().__init__()
        self._conditions = conditions

    def to_dict(self, find_scope: Callable[[Any], _Scope], ctx):
        return {
            "type": "OR",
            "conditions": [c.to_dict(find_scope, ctx) for c in self._conditions],
        }


class OneOf(Condition):
    """
    A Condition that evaluates to true if the value of the ``subject`` parameter is
    equal to one of the expected ``values``.
    """

    def __init__(self, subject: Any, values: List[Any]) -> None:
        """ """
        super().__init__()
        self._values = values
        self._subject = subject

    def to_dict(self, find_scope: Callable[[Any], _Scope], ctx):
        return {
            "scope": str(find_scope(self._subject)),
            "schema": {"oneOf": [{"const": value} for value in self._values]},
        }


class Contains(Condition):
    """
    A Condition that evaluates to true if one of the values of the ``subject`` parameter is equal to the
    expected ``value``.
    """

    def __init__(self, subject: Any, value: Any) -> None:
        """ """
        super().__init__()
        self._value = value
        self._subject = subject

    def to_dict(self, find_scope: Callable[[Any], _Scope], ctx):
        return {
            "scope": str(find_scope(self._subject)),
            "schema": {"contains": {"const": self._value}},
        }


class DialogContextCondition(Condition):
    """
    A Condition that evaluates to true if a user supplied predicate on the DialogCreationContext evaluates to true when
    the dialog is opened. Useful for rules that depend e.g. on the number and types of input ports in a node with dynamic ports.
    """

    def __init__(self, ctx_predicate: Callable[[Any], bool]) -> None:
        """
        Initializes a DialogContextCondition.

        Parameters
        ----------
        ctx_predicate : Callable[[DialogCreationContext], bool]
            Context predicate that is evaluated when the dialog is opened.
        """
        self._ctx_predicate = ctx_predicate

    def to_dict(self, find_scope: Callable[[Any], _Scope], ctx):
        return {
            "scope": "#",
            "schema": {"type": ["object"] if self._ctx_predicate(ctx) else ["null"]},
        }


class Effect(Enum):
    """
    Encodes the effect a rule may cause.
    """

    SHOW = "SHOW"
    """Show the parameter if the condition is true"""

    HIDE = "HIDE"
    """Hide the parameter if the condition is true"""

    ENABLE = "ENABLE"
    """Enable the parameter if the condition is true"""

    DISABLE = "DISABLE"
    """Disable the parameter if the condition is true"""


@dataclass
class Rule:
    """
    A rule checks a condition and enacts a corresponding effect if the condition is met.
    """

    condition: Condition
    effect: Effect


class _UISchemaExtractor:
    _root_scope = _Scope("#/properties/model/properties")

    def __init__(self, root, dialog_creation_context) -> None:
        self._root = root
        self._dialog_creation_context = dialog_creation_context

    def extract_ui_schema(
        self,
    ):
        return {
            "type": "VerticalLayout",
            "elements": self._extract_elements(self._root, self._root_scope),
        }

    def _extract_elements(self, obj, scope: _Scope):
        return [
            self._extract_element_schema(scope, name, param_obj)
            for name, param_obj in _get_parameters(obj).items()
        ]

    def _extract_element_schema(self, scope, name, param_obj):
        is_group = _is_group(param_obj)
        is_array = _is_array(param_obj)

        element_scope = scope.create_child(name, is_group)
        if is_group:
            element_schema = {
                "type": "Section",
                **param_obj._extract_ui_schema(),
                "elements": [
                    {
                        "type": param_obj._layout_direction,
                        "elements": self._extract_elements(param_obj, element_scope),
                    }
                ],
            }

        elif is_array:
            element_schema = {
                "type": "Section",
                "label": param_obj._label,
                "elements": [
                    {
                        "scope": str(element_scope),
                        **param_obj._extract_ui_schema(
                            dialog_creation_context=self._dialog_creation_context
                        ),
                    }
                ],
            }
            if param_obj._is_advanced:
                element_schema["options"] = {"isAdvanced": True}

        else:
            element_schema = {
                "scope": str(element_scope),
                **param_obj._extract_ui_schema(self._dialog_creation_context),
            }
        if param_obj._rule:
            element_schema["rule"] = self._create_rule_ui_schema(param_obj._rule)
        return element_schema

    def _create_rule_ui_schema(self, rule: Rule):
        return {
            "effect": rule.effect.value,
            "condition": rule.condition.to_dict(
                self.find_scope, self._dialog_creation_context
            ),
        }

    # it is intended that the cache is not shared across instances to prevent unexpected side-effects
    @lru_cache
    def find_scope(self, parameter) -> _Scope:
        parameter_scope = self._find_scope(self._root, parameter, self._root_scope)
        if parameter_scope:
            return parameter_scope
        raise ValueError(f"Can't find scope for parameter {parameter}")

    def _find_scope(self, obj, parameter, scope: _Scope):
        for name, param_obj in _get_parameters(obj).items():
            if parameter == param_obj:
                return scope.create_child(name)
            elif _is_group(param_obj):
                found_scope = self._find_scope(
                    param_obj, parameter, scope.create_child(name, True)
                )
                if found_scope:
                    return found_scope
        return None


class _BaseParameter(ABC):
    """
    Base class for parameter descriptors.
    "obj" in this context is a parameterised object.
    """

    __kind__ = "parameter"

    def __init__(
        self,
        label: str,
        description: str,
        default_value: Union[Any, DefaultValueProvider[Any]],
        validator: Optional[Callable[[Any], None]] = None,
        since_version: str = None,
        is_advanced: bool = False,
    ):
        """
        Parameters
        ----------
        label : str
            The label to display for the parameter in the dialog.
        description : str
            The description of the parameter that is shown in the node description and the dialog.
        default_value : Union[object, Callable]
            Either the default value for the parameter or a function that given a Version provides the default value for that version.
        validator : Optional[Callable]
            Optional validation function.
        since_version : Optional[str]
            The version at which this parameter was introduced. Can be omitted for the first version of a node.
        """
        self._label = label
        self._default_value = default_value
        self._validator = validator if validator is not None else _default_validator
        self.__doc__ = description if description is not None else ""
        self._since_version = Version.parse_version(since_version)
        self._is_advanced = is_advanced
        self._rule = None

    def __set_name__(self, owner, name):
        self._name = name
        if self._label is None:
            self._label = name

    def _get_value(self, obj, name, for_dialog=None):  # NOSONAR
        """
        If `self` is a descriptor, then the `name` parameter was passed down from the `__get__` method
        via `self._name`. Otherwise, the `name` parameter is passed from the overwritten `__getattribute__` of `GetSetBase`.

        The for_dialog parameter is needed to match the signature of the `_get_value` method for parameter groups.
        """
        if not hasattr(obj, "__kind__"):
            # obj is the root parameterised object
            _create_param_dict_if_not_exists(obj)

        if name in obj.__parameters__:
            return obj.__parameters__[name]

        def_value = self._get_default()
        obj.__parameters__[name] = def_value
        return def_value

    def _to_dict(self, value):
        """Subclasses can overwrite this method to convert value types to dictionaries."""
        return value

    def _from_dict(self, value):
        """Subclasses can overwrite this method to convert dictionaries to value types."""
        return value

    def _extract_value(self, obj, name, for_dialog=None):
        return self._to_dict(self._get_value(obj, name, for_dialog))

    def __get__(self, obj, objtype=None):
        """
        Only ever called when `self` is a descriptor, thus having the `_name` attribute.
        """
        if obj is None:
            # descriptor is called on the class e.g. by hasattr
            return self
        return self._get_value(obj, self._name)

    def _get_default(self, version: Version = None):
        if callable(self._default_value):
            if version is None:
                return self._default_value(_get_extension_version())
            else:
                return self._default_value(version)
        else:
            return self._default_value

    def _set_default_for_version(
        self, obj, name, version: Version, exclude_validations: bool = False
    ):
        self._set_value(obj, self._get_default(version), name, exclude_validations)

    def _inject(
        self,
        obj,
        value,
        name,
        parameters_version: Version = None,
        exclude_validations: bool = False,
    ):  # NOSONAR
        # the parameters_version parameter are needed to match the signature of the
        # _inject method for parameter groups
        self._set_value(obj, self._from_dict(value), name, exclude_validations)

    def _set_value(self, obj, value, name, exclude_validations: bool = False):
        if not hasattr(obj, "__kind__"):
            # obj is the root parameterised object
            _create_param_dict_if_not_exists(obj)
        # Exclude validation for LocalPathParameter when dialog is opened
        # In the future, we may want to exclude additional parameters
        if not exclude_validations or not isinstance(self, LocalPathParameter):
            # perform individual validation
            self._validate(value)
        obj.__parameters__[name] = value

    def __set__(self, obj, value):
        self._set_value(obj, value, self._name)

    def __str__(self):
        param_name = self._label if not hasattr(self, "_name") else self._name
        return f"\n\t - name: {param_name}\n\t - label: {self._label}\n\t"

    def _validate(self, value, version=None):  # NOSONAR
        # the version parameter is needed to match the signature of the
        # _validate method for parameter groups
        self._validator(value)

    def _validate_specs(self, specs):
        # can be overriden by parameter types that require a certain spec
        pass

    def validator(self, func):
        """
        To be used as a decorator for setting a validator function for a parameter.
        Note that 'func' will be encapsulated in '_validator' and will not be available in the namespace of the class.

        Examples
        --------

        >>> @knext.node(args)
        ... class MyNode:
        ...     num_repetitions = knext.IntParameter(
        ...         label="Number of repetitions",
        ...         description="How often to repeat an action",
        ...         default_value=42
        ...     )
        ...     @num_repetitions.validator
        ...     def validate_reps(value):
        ...         if value > 100:
        ...             raise ValueError("Too many repetitions!")
        ...
        ...     def configure(args):
        ...         pass
        ...
        ...     def execute(args):
        ...         pass
        """
        self._validator = func

    def rule(self, condition: Condition, effect: Effect):
        """
        Add a rule that conditionally sets whether this parameter is visible or enabled in the dialog.
        This can be useful if this parameter should only be accessible if another parameter has a certain
        value.

        Note
        ----
        Rules can only depend on parameters on the same level, not in a child or parent parameter group.

        Examples
        --------

        >>> @knext.node(args)
        ... class MyNode:
        ...     string_param = knext.StringParameter(
        ...         "String Param Title",
        ...         "String Param Title Description",
        ...         "default value"
        ...     )
        ...
        ...     # this parameter gets disabled if string_param is "foo" or "bar"
        ...     int_param = knext.IntParameter(
        ...         "Int Param Title",
        ...         "Int Param Description",
        ...     ).rule(knext.OneOf(string_param, ["foo", "bar"]), knext.Effect.DISABLE)
        """
        self._rule = Rule(condition, effect)
        return self

    def _extract_schema(
        self,
        extension_version: Version = None,  # NOSONAR
        dialog_creation_context=None,  # NOSONAR
    ):
        return {"title": self._label, "description": self.__doc__}

    def _extract_ui_schema(
        self,
        dialog_creation_context,
    ):
        options = self._get_options(dialog_creation_context)
        if self._is_advanced:
            options["isAdvanced"] = True

        return {
            "type": "Control",
            "label": self._label,
            "options": options,
        }

    @abstractmethod
    def _get_options(self, dialog_creation_context) -> dict:
        pass

    def _extract_description(self, name, parent_scope: _Scope):  # NOSONAR
        return {"name": self._label, "description": self.__doc__}


class _NumericParameter(_BaseParameter):
    """
    Note
    ------
    Subclasses of this class must implement the `check_type` method for the `_default_validator`.
    """

    def _default_validator(self, value):
        self.check_type(value)
        self.check_range(value)

    @abstractmethod
    def check_type(self, value):
        pass

    def __init__(
        self,
        label,
        description,
        default_value,
        validator=None,
        min_value=None,
        max_value=None,
        since_version=None,
        is_advanced=False,
    ):
        self.min_value = min_value
        self.max_value = max_value
        if validator is None:
            validator = self._default_validator
        super().__init__(
            label,
            description,
            default_value,
            validator,
            since_version,
            is_advanced,
        )

    def check_range(self, value):
        if self.min_value is not None and value < self.min_value:
            raise ValueError(
                f"{value} is smaller than the minimal value {self.min_value}"
            )
        if self.max_value is not None and value > self.max_value:
            raise ValueError(f"{value} is > the max value {self.max_value}")

    def _extract_ui_schema(
        self,
        dialog_creation_context,
    ):
        options = self._get_options(dialog_creation_context)

        validation = {}
        if self.min_value is not None:
            validation["min"] = {
                "parameters": {"min": self.min_value, "isExclusive": False},
                "errorMessage": f"The value must be at least {self.min_value}.",
            }
        if self.max_value is not None:
            validation["max"] = {
                "parameters": {"max": self.max_value, "isExclusive": False},
                "errorMessage": f"The value must be at most {self.max_value}.",
            }
        if validation:
            options["validation"] = validation

        if self._is_advanced:
            options["isAdvanced"] = True

        return {
            "type": "Control",
            "label": self._label,
            "options": options,
        }


class IntParameter(_NumericParameter):
    """
    Parameter class for primitive integer types.
    """

    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        default_value: Union[int, DefaultValueProvider[int]] = 0,
        validator: Optional[Callable[[int], None]] = None,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        since_version: Optional[Union[Version, str]] = None,
        is_advanced: bool = False,
    ):
        super().__init__(
            label,
            description,
            default_value,
            validator,
            min_value,
            max_value,
            since_version,
            is_advanced,
        )

    def check_type(self, value):
        if not isinstance(value, int):
            # TODO Type or ValueError?
            raise TypeError(
                f"{value} is of type {type(value)}, but should be of type int."
            )

    def _extract_schema(self, extension_version=None, dialog_creation_context=None):
        prop = super()._extract_schema(dialog_creation_context=dialog_creation_context)
        prop["type"] = "integer"
        prop["format"] = "int32"
        return prop

    def _get_options(self, dialog_creation_context) -> dict:
        return {"format": "integer"}


class DoubleParameter(_NumericParameter):
    """
    Parameter class for primitive float types.
    """

    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        default_value: Union[float, DefaultValueProvider[float]] = 0.0,
        validator: Optional[Callable[[float], None]] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        since_version: Optional[Union[str, Version]] = None,
        is_advanced: bool = False,
    ):
        super().__init__(
            label,
            description,
            default_value,
            validator,
            min_value,
            max_value,
            since_version,
            is_advanced,
        )

    def check_type(self, value):
        if not isinstance(value, numbers.Number):
            raise TypeError(
                f"{value} is of type {type(value)}, but should be a number."
            )

    def _extract_schema(self, extension_version=None, dialog_creation_context=None):
        schema = super()._extract_schema(
            dialog_creation_context=dialog_creation_context
        )
        schema["format"] = "double"
        schema["type"] = "number"
        return schema

    def _get_options(self, dialog_creation_context) -> dict:
        return {"format": "number"}


class _BaseMultiChoiceParameter(_BaseParameter):
    """
    Base class for Multi Choice-based parameter types (and StringParameter for backward compatibility),
    which allow for single or multiple selection from the provided list of options.
    """

    def __init__(
        self,
        label=None,
        description=None,
        default_value="",
        validator=None,
        since_version=None,
        is_advanced=False,
    ):
        super().__init__(
            label,
            description,
            default_value,
            validator,
            since_version,
            is_advanced,
        )

    def _get_options(self, dialog_creation_context) -> dict:
        if (
            self._enum is None
            or len(self._enum) > 4
            or (hasattr(self, "_choices") and callable(self._choices))
        ):
            return {"format": "string"}
        else:
            return {"format": "radio"}


class StringParameter(_BaseMultiChoiceParameter):
    """
    Parameter class for primitive string types.

    Dynamic Choices
    ---------------
    The optional ``choices`` callable may return either a list/tuple of plain strings
    or ``StringParameter.Choice`` objects. Plain strings are interpreted as choices
    with identical id and label and an empty description. ``Choice`` objects allow
    specifying a user-facing label and an optional description while persisting only
    the stable ``id``. Descriptions (if any) are aggregated into the parameter
    description under an "Available options" section. The section is included only
    if at least one dynamic choice supplies a non-empty description.

    Examples
    --------
    >>> def basic(ctx):
    ...     return ["A", "B"]
    ...
    >>> def rich(ctx):
    ...     return [
    ...         StringParameter.Choice("A", "Option A", "Description for A"),
    ...         StringParameter.Choice("B", "Option B"),  # no description
    ...     ]
    ...
    >>> param = knext.StringParameter(
    ...     label="Example",
    ...     description="Pick an option.",
    ...     choices=rich, # alternatively: choices=basic
    ...     default_value="A",
    ... )
    """

    @dataclass(frozen=True)
    class Choice:
        id: str
        label: str
        description: str = ""

        def __post_init__(self):  # basic validation
            if not self.id:
                raise ValueError("Choice id must be non-empty")
            if (
                not isinstance(self.id, str)
                or not isinstance(self.label, str)
                or not isinstance(self.description, str)
            ):
                raise TypeError("Choice id, label and description must be strings")

    def _default_validator(self, value):
        if not isinstance(value, str):
            raise TypeError(
                f"{value} is of type {type(value)}, but should be of type string."
            )

    # --- internal helpers -------------------------------------------------
    def _normalize_choice_sequence(
        self,
        seq,
        element_error_msg: str,
        duplicate_error_template: str,
    ) -> List["StringParameter.Choice"]:
        """Normalize a sequence of raw choice items into Choice objects.

        Parameters
        ----------
        seq : Sequence
            Sequence of either str or Choice objects.
        element_error_msg : str
            Error message raised (TypeError) when an element has an invalid type.
        duplicate_error_template : str
            Template for duplicate id ValueError; should contain '{id}'.
        """
        norm: List[StringParameter.Choice] = []
        seen = set()
        for item in seq:
            if isinstance(item, str):
                c = self.Choice(item, item)
            elif isinstance(item, self.Choice):
                c = item
            else:
                raise TypeError(element_error_msg)
            if c.id in seen:
                raise ValueError(duplicate_error_template.format(id=c.id))
            seen.add(c.id)
            norm.append(c)
        return norm

    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        default_value: Union[str, DefaultValueProvider[str]] = "",
        enum: Optional[List[str]] = None,
        validator: Optional[Callable[[str], None]] = None,
        since_version: Optional[Union[Version, str]] = None,
        is_advanced: bool = False,
        choices: Optional[Callable] = None,
    ):
        if validator is None:
            validator = self._default_validator
        if enum is not None and not isinstance(enum, list):
            raise TypeError("The enum parameter must be a list.")
        # Backward compatibility: historically 'enum' provided static list of strings.
        # We now allow 'choices' to be either a callable (dynamic) or a static sequence (list/tuple) of
        # str or Choice objects. If both enum and a static sequence for choices are provided we raise as before.
        # If enum and a callable are provided we also raise (ambiguous sources).
        if enum and choices and callable(choices):
            raise ValueError("Cannot specify both enum and a dynamic choices callable")
        if enum and choices and not callable(choices):
            raise ValueError("Cannot specify both enum and static choices sequence")

        self._choices = None  # dynamic callable (cached) or None
        self._static_choice_objs: Optional[List[StringParameter.Choice]] = None

        if choices is not None:
            if callable(choices):
                # dynamic choices
                self._choices = lru_cache(maxsize=1)(choices)
            else:
                # static sequence provided via 'choices'
                if not isinstance(choices, (list, tuple)):
                    raise TypeError("'choices' must be a callable, list, or tuple.")
                # normalize into Choice objects (unified helper)
                norm = self._normalize_choice_sequence(
                    choices,
                    "Static 'choices' sequence elements must be str or StringParameter.Choice",
                    "Duplicate choice id '{id}' in static choices",
                )
                self._static_choice_objs = norm

        # if only enum is provided, we may later wrap it into static choices for description purposes
        if self._static_choice_objs is None and enum is not None:
            # create lightweight Choice objects (ids == labels, empty description)
            self._static_choice_objs = [self.Choice(e, e, "") for e in enum]
        super().__init__(
            label,
            description,
            default_value,
            validator,
            since_version,
            is_advanced,
        )

        # Auto-set default for static sequences only (no existence validation enforced)
        if self._static_choice_objs and (
            (self._default_value is None or self._default_value == "")
            and len(self._static_choice_objs) > 0
        ):
            self._default_value = self._static_choice_objs[0].id

    def _fetch_normalized_choices(self, dialog_creation_context):
        """Return list of Choice objects (normalize str -> Choice)."""
        raw = self._choices(dialog_creation_context)
        if not isinstance(raw, (list, tuple)):
            raise TypeError("choices() must return a list or tuple of str or Choice")
        return self._normalize_choice_sequence(
            raw,
            "choices() elements must be str or Choice",
            "Duplicate choice id '{id}'",
        )

    def _append_choice_descriptions(
        self, base_doc: str, choices: List["StringParameter.Choice"]
    ):
        """Append a bullet list of available options to the parameter description.

        Mirrors the formatting used in EnumParameterOptions._generate_options_description:
        A heading "**Available options:**" followed by list items of the form
        "- <label>: <description>". If no choice supplies a non-empty description
        we still list all choices but omit the trailing colon/description part.

        Rationale: Align style across enum-like parameter descriptions and avoid wide
        markdown tables which render poorly in narrow layouts.
        """

        # Show section only if at least one non-empty description (retain original gating expectation)
        if not (choices and any(c.description.strip() for c in choices)):
            return base_doc

        indent = self._extract_indent(base_doc)
        appendix = f"\n\n{indent}**Available options:**\n\n"
        for c in choices:
            if c.description.strip():
                appendix += f"{indent}- {c.label}: {c.description.strip()}\n"
            else:
                appendix += f"{indent}- {c.label}\n"

        return base_doc.expandtabs() + appendix

    def _normalized_choices(self, dialog_ctx):
        if callable(self._choices) and dialog_ctx:
            # We pass the current DialogCreationContext to the _choices callable
            # and expect that the node developers write a lambda such that it
            # passes this parameter as "self" to the respective methods of the context.
            normalized_choices = self._fetch_normalized_choices(dialog_ctx)
            if len(normalized_choices) > 0 and not self._default_value:
                self._default_value = normalized_choices[0].id
            return normalized_choices
        elif self._static_choice_objs is not None:
            return self._static_choice_objs
        else:
            return None

    def _extract_schema(self, extension_version=None, dialog_creation_context=None):
        schema = super()._extract_schema(
            dialog_creation_context=dialog_creation_context
        )

        normalized_choices = self._normalized_choices(dialog_creation_context)

        if normalized_choices:
            schema["oneOf"] = [
                {"const": c.id, "title": c.label} for c in normalized_choices
            ]
            # add aggregated description only if at least one choice has description
            schema["description"] = self._append_choice_descriptions(
                schema.get("description", self.__doc__ or ""), normalized_choices
            )
        else:
            schema["type"] = "string"

        return schema

    # Overwrites method from _BaseMultiChoiceParameter
    def _get_options(self, dialog_creation_context) -> dict:
        """Returns the jsonforms options for the parameter"""

        normalized_choices = self._normalized_choices(dialog_creation_context)

        if normalized_choices is None or len(normalized_choices) > 0:
            return {"format": "string"}
        else:
            return {"format": "dropDown", "placeholder": "No values present"}

    def _extract_description(self, name, parent_scope: _Scope):  # NOSONAR
        """Include static choices in the node description if present.
        Dynamic (callable) choices are only available when a dialog creation context is supplied;
        during description extraction we can't evaluate them. Static sequences (enum or choices sequence)
        are context independent and therefore always appended (with descriptions if provided via Choice objects).
        """
        desc = self.__doc__ or ""

        normalized_choices = self._normalized_choices(dialog_ctx=None)
        if normalized_choices:
            desc = self._append_choice_descriptions(desc, normalized_choices)
        return {"name": self._label, "description": desc}

    def _extract_indent(self, desc: str):
        if not desc:
            return ""
        lines = desc.expandtabs().splitlines()
        indent_lvl = _get_indent_level(lines)
        return " " * indent_lvl


class LocalPathParameter(StringParameter):
    """
    Parameter class for local file path types. The path is represented as string.

    Raises
    ------
    TypeError
        If the value is not a string.
    ValueError
        If the value is not a valid file path.

    """

    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        placeholder_text: str = "",
        validator: Optional[Callable[[str], None]] = None,
        since_version: Optional[Union[Version, str]] = None,
        is_advanced: bool = False,
    ):
        super().__init__(
            label=label,
            description=description,
            validator=validator,
            since_version=since_version,
            is_advanced=is_advanced,
        )
        self.placeholder_text = placeholder_text

    def _default_validator(self, value):
        if value and not isinstance(value, str):
            raise TypeError(
                f"{value} is of type {type(value)}, but should be of type string."
            )
        # check if value is a local path
        if value and not os.path.exists(value):
            raise ValueError(f"{value} is not a valid file path.")

    def _extract_schema(self, extension_version=None, dialog_creation_context=None):
        schema = super()._extract_schema(
            dialog_creation_context=dialog_creation_context
        )
        schema["type"] = "string"

        return schema

    def _get_options(self, dialog_creation_context) -> dict:
        options = {
            "format": "stringFileChooser",
            "fileSystem": "LOCAL",
            "placeholder": self.placeholder_text,
        }

        return options


class MultilineStringParameter(_BaseParameter):
    """
    Parameter class for string type with multiline supported.
    """

    def _default_validator(self, value):
        if not isinstance(value, str):
            raise TypeError(
                f"{value} is of type {type(value)}, but should be of type string."
            )

    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        default_value: Union[str, DefaultValueProvider[str]] = "",
        validator: Optional[Callable[[str], None]] = None,
        since_version: Optional[Union[Version, str]] = None,
        is_advanced: bool = False,
        number_of_lines: Optional[int] = 3,
    ):
        if validator is None:
            validator = self._default_validator

        if not isinstance(number_of_lines, int):
            raise TypeError(
                f"Number of lines is of type {type(number_of_lines)}, but should be of type integer."
            )
        elif number_of_lines <= 0:
            raise ValueError("Number of lines should be of type positive integer.")

        self._number_of_lines = number_of_lines

        super().__init__(
            label,
            description,
            default_value,
            validator,
            since_version,
            is_advanced,
        )

    def _extract_schema(self, extension_version=None, dialog_creation_context=None):
        schema = super()._extract_schema(
            dialog_creation_context=dialog_creation_context
        )
        schema["type"] = "string"

        return schema

    def _extract_ui_schema(
        self,
        dialog_creation_context,
    ):
        options = self._get_options(dialog_creation_context)
        if self._is_advanced:
            options["isAdvanced"] = True

        return {
            "type": "Control",
            "label": self._label,
            "options": options,
        }

    def _get_options(self, dialog_creation_context) -> dict:
        return {"format": "textArea", "rows": self._number_of_lines}


class EnumParameterOptions(Enum):
    """
    A helper class for creating EnumParameter options, based on Python's Enum class.

    Developers should subclass this class, and provide enumeration options as class attributes of the subclass,
    of the form ``OPTION_NAME = (OPTION_LABEL, OPTION_DESCRIPTION)``.

    Enum option objects can be accessed as attributes of the EnumParameterOptions subclass, e.g. ``MyEnum.OPTION_NAME``.
    Each option object has the following attributes:

    - name: the name of the class attribute, e.g. "OPTION_NAME", which is used as the selection constant;
    - label: the label of the option, displayed in the configuration dialogue of the node;
    - description: the description of the option, used along with the label to generate a list of the available options in the Node Description and in the configuration dialogue of the node.

    Examples
    --------

    >>> class CoffeeOptions(EnumParameterOptions):
    ...     CLASSIC = ("Classic", "The classic chocolatey taste, with notes of bitterness and wood.")
    ...     FRUITY = ("Fruity", "A fruity taste, with notes of berries and citrus.")
    ...     WATERY = ("Watery", "A watery taste, with notes of water and wetness.")
    """

    def __init__(self, label, description):
        self.label = label
        self.description = description

    @classmethod
    def _generate_options_description(cls, docstring: str, visible_members=None):
        """Generate options description.

        Parameters
        ----------
        docstring : str
            The parameter docstring.
        visible_members : list, optional
            List of member names to include. If None, all members are included.

        Returns
        -------
        str
            The formatted options description including the available options.
        """
        # ensure that the options description is indented correctly
        if docstring:
            lines = docstring.expandtabs().splitlines()
            indent_lvl = _get_indent_level(lines)
            indent = " " * indent_lvl
        else:
            indent = ""

        options_description = f"\n\n{indent}**Available options:**\n\n"
        members_to_show = (
            visible_members if visible_members is not None else cls._member_names_
        )
        for member in members_to_show:
            options_description += (
                f"{indent}- {cls[member].label}: {cls[member].description}\n"
            )

        return docstring.expandtabs() + options_description

    @classmethod
    def _get_default_option(cls):
        """
        Return a subclass of this class containing a single default option.
        """

        class DefaultEnumOption(EnumParameterOptions):
            DEFAULT_OPTION = (
                "Default",
                "This is the default option, since additional options have not been provided.",
            )

        return DefaultEnumOption

    @classmethod
    def get_all_options(cls):
        """
        Returns a list of all options defined in the EnumParameterOptions subclass.
        """
        return cls._member_names_


class EnumParameter(_BaseMultiChoiceParameter):
    """
    Parameter class for multiple-choice parameter types. Replicates and extends the enum functionality
    previously implemented as part of `StringParameter`.

    A subclass of `EnumParameterOptions` should be provided as the enum parameter, which should contain
    class attributes of the form `OPTION_NAME = (OPTION_LABEL, OPTION_DESCRIPTION)`. The corresponding
    option attributes can be accessed via `MyOptions.OPTION_NAME.name`, `.label`, and `.description`
    respectively.

    The `.name` attribute of each option is used as the selection constant, e.g.
    `MyOptions.OPTION_NAME.name == "OPTION_NAME"`.

    Examples
    --------

    >>> class CoffeeOptions(EnumParameterOptions):
    ...     CLASSIC = ("Classic", "The classic chocolatey taste, with notes of bitterness and wood.")
    ...     FRUITY = ("Fruity", "A fruity taste, with notes of berries and citrus.")
    ...     WATERY = ("Watery", "A watery taste, with notes of water and wetness.")
    ...
    ... coffee_selection_param = knext.EnumParameter(
    ...     label="Coffee Selection",
    ...     description="Select the type of coffee you like to drink.",
    ...     default_value=CoffeeOptions.CLASSIC.name,
    ...     enum=CoffeeOptions,
    ... )

    Dynamic Filtering
    -----------------
    The optional ``hidden_choices`` callable allows filtering which enum members are hidden in the
    dialog and node description based on the runtime context. This is useful for hiding options that
    are not applicable given the current input data.

    The callable receives a ``DialogCreationContext`` (or ``None`` if the node is not connected or
    during node description generation at startup) and must return a list of enum members to hide.
    If ``None`` or an empty list is returned, all options are shown. If the callable returns only
    invalid members or all members, a warning is logged.

    **Important:** Validation accepts any enum member regardless of filtering. This ensures that saved
    workflows remain valid even when the context changes and different options are filtered.

    The ``DialogCreationContext`` parameter can be ``None`` in two scenarios:

    - During node description generation at KNIME startup
    - When the node has no input connections

    Your callable should handle ``None`` gracefully. By returning different options based on whether
    the context is ``None``, you can control what appears in the node description versus the dialog.
    For example, returning items to hide when ``ctx is None`` effectively provides static filtering in
    the description while still allowing dynamic filtering in the dialog based on actual input data.

    >>> class ModelOptions(EnumParameterOptions):
    ...     LINEAR = ("Linear Regression", "Fits a linear model")
    ...     RANDOM_FOREST = ("Random Forest", "Ensemble tree model")
    ...     NEURAL_NET = ("Neural Network", "Deep learning model")
    ...
    ... def hide_by_model_support(context):
    ...     # Handle None context (no connection or description generation)
    ...     if context is None:
    ...         # Hide advanced option in description
    ...         return [ModelOptions.NEURAL_NET]
    ...
    ...     # Get input specifications for dialog filtering
    ...     specs = context.get_input_specs()
    ...     if not specs:
    ...         return []  # Hide nothing, show all
    ...
    ...     # Filter based on model capabilities from input
    ...     model_spec = specs[0]  # Assuming model is first input
    ...     supported = model_spec.get_supported_options()  # Hypothetical method
    ...
    ...     return [opt for opt in ModelOptions if opt.name not in supported]
    ...
    ... model_param = knext.EnumParameter(
    ...     label="Model Type",
    ...     description="Select the model to use.",
    ...     default_value=ModelOptions.LINEAR,  # Can use enum member directly
    ...     enum=ModelOptions,
    ...     hidden_choices=hide_by_model_support,
    ... )
    """

    class Style(Enum):
        RADIO = "radio"
        VALUE_SWITCH = "valueSwitch"
        DROPDOWN = "string"

    def _default_validator(self, value):
        if value not in self._enum.get_all_options():
            raise ValueError(
                f"""The selection '{value}' for parameter '{self._label}' is not one of the available options: {"'" + "', '".join(self._enum.get_all_options()) + "'"}."""
            )

    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        default_value: Union[
            str,
            EnumParameterOptions,
            DefaultValueProvider[Union[str, EnumParameterOptions]],
        ] = None,
        enum: Optional[EnumParameterOptions] = None,
        validator: Optional[Callable[[str], None]] = None,
        since_version: Optional[Union[Version, str]] = None,
        is_advanced: bool = False,
        style: Optional[Style] = None,
        hidden_choices: Optional[
            Callable[[Optional[Any]], List[EnumParameterOptions]]
        ] = None,
    ):
        """
        Parameters
        ----------
        hidden_choices : Optional[Callable[[Optional[DialogCreationContext]], List[EnumParameterOptions]]]
            Optional callable that filters which enum members are hidden in the dialog.
            The callable receives a DialogCreationContext (or None) and must return a list
            of enum members to hide. If None, an empty list, or not provided, all enum members are shown.
        """
        if validator is None:
            validator = self._default_validator
        else:
            validator = self._add_default_validator(validator)

        if enum is None or len(enum.get_all_options()) == 0:
            self._enum = EnumParameterOptions._get_default_option()
            default_value = self._enum.DEFAULT_OPTION.name
        else:
            self._enum = enum
            if default_value is None:
                default_value = self._enum.get_all_options()[0].name
            elif hasattr(default_value, "name"):
                # Support enum member as default_value
                default_value = default_value.name

        self._style = style

        # Store hidden_choices callable wrapped with cache
        # Cache size of 2 to handle both None (description) and actual context (schema)
        if hidden_choices is not None:
            self._hidden_choices = lru_cache(maxsize=2)(hidden_choices)
        else:
            self._hidden_choices = None

        super().__init__(
            label,
            description,
            default_value,
            validator,
            since_version,
            is_advanced,
        )

    def _get_visible_options(self, dialog_creation_context):
        """Get the list of enum members to display in the dialog.

        Returns the full enum if no hidden_choices callable is set, otherwise
        calls the callable and validates the returned members to hide, then returns
        the remaining members.
        """
        if self._hidden_choices is None:
            return list(self._enum)

        # Call the cached callable with context (can be None)
        try:
            hidden_members = self._hidden_choices(dialog_creation_context)
        except Exception as e:
            LOGGER.warning(
                f"Error calling hidden_choices for parameter '{self._label}': {e}. "
                f"Showing all options."
            )
            return list(self._enum)

        if not isinstance(hidden_members, (list, tuple)):
            LOGGER.warning(
                f"hidden_choices for parameter '{self._label}' must return a list or tuple, "
                f"got {type(hidden_members).__name__}. Showing all options."
            )
            return list(self._enum)

        # Handle empty list - show all options (nothing hidden)
        if not hidden_members:
            return list(self._enum)

        # Validate that all returned members exist in the enum
        valid_names = set(self._enum._member_names_)
        validated_hidden = []
        invalid_members = []

        for member in hidden_members:
            if hasattr(member, "name") and member.name in valid_names:
                validated_hidden.append(member)
            else:
                invalid_members.append(member)

        # Warn about invalid members
        if invalid_members:
            valid_options = ", ".join(self._enum._member_names_)
            LOGGER.warning(
                f"hidden_choices for parameter '{self._label}' returned invalid members: "
                f"{invalid_members}. Valid options are: {valid_options}"
            )

        # If all members are hidden or all returned members were invalid, show empty
        all_members = list(self._enum)
        if len(validated_hidden) >= len(all_members):
            LOGGER.warning(
                f"hidden_choices for parameter '{self._label}' would hide all options. "
                f"Showing empty options."
            )
            return []

        # Return members that are not in the hidden list
        hidden_names = {member.name for member in validated_hidden}
        visible_members = [member for member in all_members if member.name not in hidden_names]

        return visible_members

    def _get_options(self, dialog_creation_context) -> dict:
        if self._style:
            return {"format": self._style.value}
        return super()._get_options(dialog_creation_context)

    def _generate_description(self, visible_options=None):
        """Generate description with optional visible options filtering.

        Parameters
        ----------
        visible_options : list of EnumParameterOptions, optional
            List of enum members to include in description. If None, all members
            from self._enum are included. If provided, only these members appear
            in the description.

        Returns
        -------
        str
            A formatted description string containing the available options,
            optionally restricted to the provided ``visible_options``.
        """
        if visible_options is None:
            # No filtering - generate description for all options
            return self._enum._generate_options_description(self.__doc__)

        # Generate description for filtered options
        visible_member_names = [opt.name for opt in visible_options]
        return self._enum._generate_options_description(
            self.__doc__, visible_member_names
        )

    def _extract_schema(self, extension_version=None, dialog_creation_context=None):
        schema = super()._extract_schema(
            dialog_creation_context=dialog_creation_context
        )

        # Use filtered options for dialog UI
        visible_options = self._get_visible_options(dialog_creation_context)
        schema["description"] = self._generate_description(visible_options)
        schema["oneOf"] = [{"const": e.name, "title": e.label} for e in visible_options]

        # Warn if default value is not in visible options
        if visible_options and self._default_value:
            visible_names = {e.name for e in visible_options}
            if self._default_value not in visible_names:
                LOGGER.warning(
                    f"Default value '{self._default_value}' for parameter '{self._label}' "
                    f"is not in the currently visible options: {', '.join(visible_names)}"
                )

        return schema

    def _extract_description(self, name, parent_scope: _Scope):
        # Get visible options with None context for node description
        visible_options = (
            self._get_visible_options(None) if self._hidden_choices else None
        )
        return {
            "name": self._label,
            "description": self._generate_description(visible_options),
        }

    def validator(self, func):
        # we retain the default validator to ensure that value is always one of the available options
        self._validator = _combine_validators(func, self._default_validator)


class EnumSetParameter(_BaseMultiChoiceParameter):
    """
    Parameter class for multiple-choice parameter types. Expands the EnumParameter by enabling to select
    multiple enum constants.

    A subclass of `EnumParameterOptions` should be provided as the enum parameter, which should contain
    class attributes of the form `OPTION_NAME = (OPTION_LABEL, OPTION_DESCRIPTION)`. The corresponding
    option attributes can be accessed via `MyOptions.OPTION_NAME.name`, `.label`, and `.description`
    respectively.

    The `.name` attribute of each option is used as the selection constant, e.g.
    `MyOptions.OPTION_NAME.name == "OPTION_NAME"`.

    Examples
    --------

    >>> class CoffeeOptions(EnumParameterOptions):
    ...     CLASSIC = ("Classic", "The classic chocolatey taste, with notes of bitterness and wood.")
    ...     FRUITY = ("Fruity", "A fruity taste, with notes of berries and citrus.")
    ...     WATERY = ("Watery", "A watery taste, with notes of water and wetness.")
    ...
    ... coffee_selection_param = knext.EnumSetParameter(
    ...     label="Coffee Selection",
    ...     description="Select the types of coffee you like to drink.",
    ...     default_value=[CoffeeOptions.CLASSIC.name, CoffeeOptions.FRUITY.name],
    ...     enum=CoffeeOptions,
    ... )
    """

    def _default_validator(self, value):
        if not isinstance(value, list):
            raise TypeError(
                f"""The selection '{value}' for parameter '{self._label}' is not a list"""
            )
        if not set(value).issubset(self._enum.get_all_options()):
            raise ValueError(
                f"""The selection '{value}' for parameter '{self._label}' is not a subset of the available options: {"'" + "', '".join(self._enum.get_all_options()) + "'"}."""
            )

    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        default_value: Union[List[str], DefaultValueProvider[List[str]]] = None,
        enum: Optional[EnumParameterOptions] = None,
        validator: Optional[Callable[[str], None]] = None,
        since_version: Optional[Union[Version, str]] = None,
        is_advanced: bool = False,
    ):
        if validator is None:
            validator = self._default_validator
        else:
            validator = self._add_default_validator(validator)

        if enum is None or len(enum.get_all_options()) == 0:
            self._enum = EnumParameterOptions._get_default_option()
            default_value = [self._enum.DEFAULT_OPTION.name]
        else:
            self._enum = enum
            if default_value is None:
                default_value = self._enum.get_all_options()

        super().__init__(
            label,
            description,
            default_value,
            validator,
            since_version,
            is_advanced,
        )

    def _get_options(self, dialog_creation_context) -> dict:
        options = {
            "format": "twinList",
            "possibleValues": [
                {"id": option, "text": self._enum[option].label}
                for option in self._enum.get_all_options()
            ],
        }
        return options

    def _generate_description(self):
        return self._enum._generate_options_description(self.__doc__)

    def _extract_schema(
        self,
        extension_version=None,
        dialog_creation_context=None,
    ):
        schema = super()._extract_schema(
            dialog_creation_context=dialog_creation_context
        )
        schema["type"] = "array"
        schema["items"] = {"type": "string"}
        schema["description"] = self._generate_description()
        return schema

    def _extract_description(self, name, parent_scope: _Scope):
        return {"name": self._label, "description": self._generate_description()}

    def _add_default_validator(self, func):
        def combined_validator(value):
            # we retain the default validator to ensure that value is always one of the available options
            self._default_validator(value)
            func(value)

        return combined_validator

    def validator(self, func):
        self._validator = self._add_default_validator(func)


class _BaseColumnParameter(_BaseParameter):
    """
    Base class for single and multi column selection parameters.
    """

    def __init__(
        self,
        label,
        description,
        port_index: Union[int, Tuple[int, int]],
        column_filter: Optional[Callable[[ks.Column], bool]] = None,
        default_value: Optional[Union[Any, DefaultValueProvider[Any]]] = None,
        since_version=None,
        is_advanced=False,
        schema_provider=None,
    ):
        """
        Parameters
        ----------
        label : str
            Label of the parameter in the dialog
        description : str
            Description of the parameter in the node description and dialog
        port_index : int or (int, int)
            The input port to select columns from. If the input port is a dynamic port group, provide a tuple of (port index, port index within group).
            Each dynamic port group counts as one in port index numbering, so if you have e.g. table port, dynamic port group, table port, accessing the
            last table would have index 2.
        column_filter : function
            A function for prefiltering columns
        since_version : str, optional
            The version at which this parameter was introduced. Can be omitted if the parameter is part of the first version of the node.
        """
        super().__init__(
            label,
            description,
            default_value=default_value,
            since_version=since_version,
            is_advanced=is_advanced,
        )
        if column_filter is None:

            def column_filter(c):
                return True

        self._column_filter = column_filter
        if schema_provider is None:
            self._port_index = port_index

            def default_schema_provider(dialog_creation_context):
                return _pick_spec(dialog_creation_context.get_input_specs(), port_index)

            schema_provider = default_schema_provider
        else:
            self._port_index = None
        self._schema_provider = schema_provider

    def _validate_specs(self, specs):
        if self._port_index is None:
            return
        if isinstance(self._port_index, int) and len(specs) <= self._port_index:
            raise ValueError(
                f"There are too few input specs. The column selection '{self._name}' requires a table input at index {self._port_index}"
            )
        elif isinstance(self._port_index, tuple) and (
            len(specs) <= self._port_index[0]
            or len(specs[self._port_index[0]]) < self._port_index[1]
        ):
            raise ValueError(
                f"There are too few input specs. The column selection '{self._name}' requires a table input at dynamic port index {self._port_index}"
            )
        elif not isinstance(_pick_spec(specs, self._port_index), ks.Schema):
            raise TypeError(
                f"The port index ({self._port_index}) of the column selection '{self._name}' does not point to a table input."
            )


class ColumnParameter(_BaseColumnParameter):
    """
    Parameter class for single columns.
    """

    NONE = "<none>"

    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        port_index: int = 0,
        column_filter: Callable[[ks.Column], bool] = None,
        include_row_key: bool = False,
        include_none_column: bool = False,
        since_version: Optional[str] = None,
        is_advanced: bool = False,
        schema_provider=None,
        default_value: Optional[Union[str, DefaultValueProvider[str]]] = None,
    ):
        """
        Parameters
        ----------
        label : str
            Label of the parameter in the dialog
        description : str
            Description of the parameter in the node description and dialog
        port_index : int
            The input port to select columns. Ignored if a schema_provider is specified.
        column_filter : function
            A function for prefiltering columns
        include_row_key : bool
            Whether to include the row keys as selectable column
        include_none_column : bool
            Whether to allow to select no column
        since_version : str, optional
            The version at which this parameter was introduced. Can be omitted if the parameter is part of the first version of the node.
        schema_provider: function, optional
            A function that takes a DialogCreationContext and extracts a Schema from it.
        """
        super().__init__(
            label,
            description,
            port_index,
            column_filter,
            default_value,
            since_version,
            is_advanced,
            schema_provider,
        )
        self._include_row_key = include_row_key
        self._include_none_column = include_none_column

    def _extract_schema(
        self,
        extension_version=None,
        dialog_creation_context=None,
    ):
        schema = super()._extract_schema(
            dialog_creation_context=dialog_creation_context
        )
        schema["type"] = "string"
        return schema

    def _get_options(self, dialog_creation_context) -> dict:
        options = {
            "format": "dropDown",
            "showRowKeys": self._include_row_key,
            "showNoneColumn": self._include_none_column,
            "possibleValues": _possible_values(
                self._schema_provider(dialog_creation_context), self._column_filter
            ),
        }
        # following condition was added to integrate change in knime-core-ui
        # to enable selection of "None" columns
        if self._include_none_column:
            options["possibleValues"].append({"id": self.NONE, "text": "None"})

        return options

    # overriding these functions to allow none types change in ui schema (change applicable starting 5.9).
    def _to_dict(self, value):
        return "" if value is None else value

    def _from_dict(self, value):
        return None if value == "" else value


class MultiColumnParameter(_BaseColumnParameter):
    """
    Parameter class for multiple columns.
    """

    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        port_index: Optional[int] = 0,
        column_filter: Optional[Callable[[ks.Column], bool]] = None,
        since_version: Optional[Union[str, Version]] = None,
        is_advanced: bool = False,
    ):
        """

        Parameters
        ----------
        label : str, optional
            A string label for the object (default is None).
        description : str, optional
            A string description of the object (default is None).
        port_index : int, optional
            An integer representing the port index (default is 0).
        column_filter : callable, optional
            A function that takes a ks.Column object and returns a boolean value (default is None).
        since_version : Union[str, Version], optional
            A string or Version object representing the version since when the object is available (default is None).
        is_advanced : bool, optional
            A boolean indicating if the object is advanced (default is False).

        """
        super().__init__(
            label,
            description,
            port_index,
            column_filter,
            None,
            since_version,
            is_advanced,
        )

    def _extract_schema(
        self,
        extension_version=None,
        dialog_creation_context=None,
    ):
        schema = super()._extract_schema(
            dialog_creation_context=dialog_creation_context
        )
        schema["type"] = "array"
        schema["items"] = {"type": "string"}
        return schema

    def _get_options(self, dialog_creation_context) -> dict:
        options = {
            "format": "twinList",
            "possibleValues": _possible_values(
                self._schema_provider(dialog_creation_context),
                self._column_filter,
            ),
        }
        return options

    def _get_value(self, obj: Any, name, for_dialog: bool = False):
        value = super()._get_value(obj, name, for_dialog)
        if for_dialog and value is None:
            return []
        else:
            return value

    def _inject(self, obj, value, name, version, exclude_validations: bool = False):
        # if there are no columns then the empty string is used as placeholder and we need to filter it out here
        if value is not None:
            value = [c for c in value if c != ""]
        return super()._inject(obj, value, name, version, exclude_validations)

    def _get_default(self, version: Version = None):
        default_value = []
        return default_value


def _pick_spec(specs: List[ks.PortObjectSpec], port_index: Union[int, Tuple[int, int]]):
    try:
        if isinstance(port_index, tuple):
            if not isinstance(port_index[0], int) and isinstance(port_index[1], int):
                raise ValueError(
                    f"Invalid port index, must be int or (int, int), but got ({type(port_index[0])}, {type(port_index[1])})."
                )
            port_group_specs = specs[port_index[0]]
            if not isinstance(port_group_specs, list):
                raise TypeError(
                    f"The port at index {port_index[0]} is not a port group. Access this port with a single int instead."
                )

            if port_index[1] >= len(port_group_specs):
                return ks.Schema.from_columns([])
            spec = port_group_specs[port_index[1]]
        else:
            if not isinstance(port_index, int):
                raise ValueError(
                    f"Invalid port index, must be int or (int, int), but got {type(port_index)}."
                )
            spec = specs[port_index]
    except IndexError:
        raise IndexError(
            f"The port index {port_index} is not contained in the spec list with length {len(specs)}. "
            f"Maybe the port_index does not match the index of the corresponding input table? "
        ) from None
    if spec is None:
        # no input connected at this port
        return ks.Schema.from_columns([])
    if not isinstance(spec, ks.Schema):
        raise TypeError(
            f"The port at index {port_index} is not a table. "
            f"The ColumnFilter can only be used for table ports. "
            f"Available specs are: {specs}"
        )
    return spec


def _possible_values(
    spec: ks.Schema,
    column_filter: Callable[[ks.Column], bool],
) -> List[Dict[str, str]]:
    def entry(name, type=None, compatible_types=None):
        entry = {"id": name, "text": name}
        if type is not None:
            entry["type"] = {"id": type[0], "text": type[1]}
        if compatible_types is not None:
            entry["compatibleTypes"] = compatible_types
        return entry

    if not isinstance(spec, ks.Schema):
        raise TypeError("The given input is not a table.")

    filtered = [
        entry(
            column.name,
            (
                column.metadata["preferred_value_type"],
                column.metadata["displayed_column_type"],
            ),
            # TODO: add more compatible types in https://knime-com.atlassian.net/browse/AP-20608
            [column.metadata["preferred_value_type"]],
        )
        for column in spec
        if column_filter(column)
    ]
    if len(filtered) > 0:
        return filtered
    else:
        return []


class ColumnFilterMode(Enum):
    """
    The modes that can be selected in the ColumnFilterParameter.
    """

    MANUAL = "Manual"
    REGEX = "Regex"
    WILDCARD = "Wildcard"
    TYPE = "Type"


class PatternFilterConfig:
    """
    The pattern configuration for the ColumnFilterParameter, used for both, regex and wildcard patterns.
    """

    def __init__(self, pattern="", case_sensitive=True, inverted=False):
        self.case_senstive = case_sensitive
        self.inverted = inverted
        self.pattern = pattern

    def _extract_schema(self):
        return {
            "type": "object",
            "properties": {
                "isCaseSensitive": {
                    "type": "boolean",
                    "default": self.case_senstive,
                },
                "isInverted": {"type": "boolean", "default": self.inverted},
                "pattern": {"type": "string", "default": self.pattern},
            },
        }

    def _apply(self, schema: ks.Schema, regex: bool) -> List[str]:
        import re

        pattern_str = self.pattern

        if not regex:
            pattern_str = re.escape(pattern_str)
            pattern_str = pattern_str.replace("\\*", ".*")
            pattern_str = pattern_str.replace("\\?", ".")

        flags = 0 if self.case_senstive else re.IGNORECASE
        pattern = re.compile(pattern_str, flags)

        filtered_column_names = []
        for column in schema:
            pattern_matches = pattern.search(column.name) is not None
            # The ^ is xor, so we include the column only if
            # the pattern is matching or it is NOT matching but
            # we have inverted the search
            if pattern_matches ^ self.inverted:
                filtered_column_names.append(column.name)
        return filtered_column_names

    @classmethod
    def _from_dict(cls, value: Dict):
        return cls(
            value["pattern"],
            value["isCaseSensitive"],
            value["isInverted"],
        )

    def _to_dict(self) -> Dict:
        return {
            "pattern": self.pattern,
            "isCaseSensitive": self.case_senstive,
            "isInverted": self.inverted,
        }


class TypeFilterConfig:
    """
    The type filter configuration for the ColumnFilterParameter.
    """

    def __init__(self, selected_types=None, type_displays=None):
        if selected_types is None:
            selected_types = []
        if type_displays is None:
            type_displays = []

        self._selected_types = selected_types
        self._type_displays = type_displays

    def _extract_schema(self):
        return {
            "type": "object",
            "properties": {
                "selectedTypes": {
                    "default": self._selected_types,
                    "type": "array",
                    "items": {"type": "string"},
                },
                "typeDisplays": {
                    "default": self._type_displays,
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
        }

    def _apply(self, schema: ks.Schema) -> List[str]:
        filtered_column_names = []

        for column in schema:
            if column.metadata is None or "preferred_value_type" not in column.metadata:
                LOGGER.warning(
                    f"Ignoring column '{column.name}' because it does not have a 'preferred_value_type' set."
                )
                continue

            column_type = column.metadata["preferred_value_type"]
            if column_type in self._selected_types:
                filtered_column_names.append(column.name)

        return filtered_column_names

    @classmethod
    def _from_dict(cls, value: Dict):
        return cls(
            value["selectedTypes"],
            value["typeDisplays"],
        )

    def _to_dict(self) -> Dict:
        return {
            "selectedTypes": self._selected_types,
            "typeDisplays": self._type_displays,
        }


class ManualFilterConfig:
    """
    The manual configuration of the ColumnFilterParameter, consisting of the included and excluded
    columns as well as the info whether unknown columns should be included or excluded.
    """

    def __init__(
        self,
        included: List[str] = None,
        excluded: List[str] = None,
        include_unknown_columns=True,
    ):
        self.include_unknown_columns = include_unknown_columns
        self.included = included if included is not None else []
        self.excluded = excluded if excluded is not None else []

    def _extract_schema(self):
        return {
            "type": "object",
            "properties": {
                "includeUnknownColumns": {
                    "type": "boolean",
                    "default": self.include_unknown_columns,
                },
                "manuallyDeselected": {
                    "type": "array",
                    "items": {"type": "string"},
                    "default": self.excluded,
                },
                "manuallySelected": {
                    "type": "array",
                    "items": {"type": "string"},
                    "default": self.included,
                },
            },
        }

    def _apply(self, schema: ks.Schema) -> List[str]:
        return [
            column.name
            for column in schema
            if column.name in self.included
            or (
                column.name not in self.included
                and column.name not in self.excluded
                and self.include_unknown_columns
            )
        ]

    @classmethod
    def _from_dict(cls, value: Dict):
        return cls(
            value["manuallySelected"],
            value["manuallyDeselected"],
            value["includeUnknownColumns"],
        )

    def _to_dict(self) -> Dict:
        return {
            "manuallySelected": self.included,
            "manuallyDeselected": self.excluded,
            "includeUnknownColumns": self.include_unknown_columns,
        }


class ColumnFilterConfig:
    """
    The value of a `ColumnFilterParameter` is a `ColumnFilterConfig` instance with a mode as well
    as configuration for the different modes.

    Use the `apply` method to filter schemas and tables according to this filter config

    Examples
    ---------

    >>> @knext.node(
    ...     name="Python Column Filter",
    ...     node_type=knext.NodeType.MANIPULATOR,
    ...     icon_path=...,
    ...     category=...,
    ... )
    ... @knext.input_table("Input Table", "Input table.")
    ... @knext.output_table("Output Table", "Output table.")
    ... class ColumnFilterNode:
    ...     column_filter = knext.ColumnFilterParameter("Column Filter", "Column Filter")
    ...
    ...     def configure(self, config_context, input_schema: knext.Schema):
    ...         return self.column_filter.apply(input_schema)
    ...
    ...     def execute(self, exec_context, input_table):
    ...         return self.column_filter.apply(input_table)
    """

    def __init__(
        self,
        mode=ColumnFilterMode.MANUAL,
        pattern_filter: PatternFilterConfig = None,
        type_filter: TypeFilterConfig = None,
        manual_filter: ManualFilterConfig = None,
        included_column_names: List[str] = None,
        pre_filter: Callable[[ks.Column], bool] = None,
    ):
        """
        Construct a `ColumnFilterConfig` with given `mode`, `pattern_filter`, `type_filter`, and `manual_filter`.

        If `included_column_names` are provided, `manual_filter` and `mode` will be ignored and will be configured
        such that only the explicitly `included_column_names` (and unknown columns) are selected.
        """
        self.pattern_filter = (
            pattern_filter if pattern_filter is not None else PatternFilterConfig()
        )
        self.type_filter = (
            type_filter if type_filter is not None else TypeFilterConfig()
        )

        if included_column_names is not None:
            self.manual_filter = ManualFilterConfig(included=included_column_names)
            self.mode = ColumnFilterMode.MANUAL
        else:
            self.manual_filter = (
                manual_filter if manual_filter is not None else ManualFilterConfig()
            )

            self.mode = mode

        self._pre_filter = pre_filter

    @classmethod
    def _from_dict(cls, value, pre_filter: Callable[[ks.Column], bool]):
        return cls(
            mode=ColumnFilterMode[value["mode"]],
            pattern_filter=PatternFilterConfig._from_dict(value["patternFilter"]),
            type_filter=TypeFilterConfig._from_dict(value["typeFilter"]),
            manual_filter=ManualFilterConfig._from_dict(value["manualFilter"]),
            pre_filter=pre_filter,
        )

    def _to_dict(self):
        return {
            "mode": self.mode.name,
            "patternFilter": self.pattern_filter._to_dict(),
            "typeFilter": self.type_filter._to_dict(),
            "manualFilter": self.manual_filter._to_dict(),
        }

    def __eq__(self, other) -> bool:
        return self._to_dict() == other._to_dict()

    def _extract_schema(self):
        return {
            "patternFilter": self.pattern_filter._extract_schema(),
            "typeFilter": self.type_filter._extract_schema(),
            "manualFilter": self.manual_filter._extract_schema(),
            "mode": {
                "oneOf": [
                    {"const": "MANUAL", "title": "Manual"},
                    {"const": "REGEX", "title": "Regex"},
                    {"const": "WILDCARD", "title": "Wildcard"},
                    {"const": "TYPE", "title": "Type"},
                ],
            },
            "selected": {
                "type": "array",
                "items": {"type": "string", "configKeys": ["selected_Internals"]},
                "configKeys": ["selected_Internals"],
            },
        }

    def apply(self, columnar: ks._Columnar) -> ks._Columnar:
        """
        Filter a table schema or a table according to this column filter configuration.
        """
        schema = columnar.schema if hasattr(columnar, "schema") else columnar
        prefiltered_schema = (
            schema[[column.name for column in schema if self._pre_filter(column)]]
            if self._pre_filter
            else schema
        )

        filtered_column_names = []
        if self.mode == ColumnFilterMode.MANUAL:
            filtered_column_names = self.manual_filter._apply(prefiltered_schema)
        elif (
            self.mode == ColumnFilterMode.REGEX
            or self.mode == ColumnFilterMode.WILDCARD
        ):
            filtered_column_names = self.pattern_filter._apply(
                prefiltered_schema, regex=self.mode == ColumnFilterMode.REGEX
            )
        elif self.mode == ColumnFilterMode.TYPE:
            filtered_column_names = self.type_filter._apply(prefiltered_schema)
        else:
            raise ValueError(f"Unknown column filter mode selected: {self.mode}")
        return columnar[filtered_column_names]


class ColumnFilterParameter(_BaseColumnParameter):
    """
    Parameter class that supports full column filtering for columns.
    """

    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        port_index: Optional[int] = 0,
        default_value: Optional[
            Union[ColumnFilterConfig, DefaultValueProvider[ColumnFilterConfig]]
        ] = None,
        column_filter: Callable[[ks.Column], bool] = None,
        since_version: Optional[Union[str, Version]] = None,
        is_advanced: bool = False,
        schema_provider=None,
    ):
        default_value = default_value if default_value else ColumnFilterConfig()
        super().__init__(
            label,
            description,
            port_index,
            column_filter,
            default_value,
            since_version,
            is_advanced,
            schema_provider,
        )

    def _extract_schema(
        self,
        extension_version=None,
        dialog_creation_context=None,
    ):
        schema = super()._extract_schema(
            dialog_creation_context=dialog_creation_context
        )

        schema["type"] = "object"
        default_column_filter = super()._get_default(extension_version)
        schema["properties"] = default_column_filter._extract_schema()

        return schema

    def _get_options(self, dialog_creation_context) -> dict:
        options = {
            "format": "typedStringFilter",
            "unknownValuesText": "Any unknown column",
            "emptyStateLabel": "No columns in this list.",
            "possibleValues": _possible_values(
                self._schema_provider(dialog_creation_context),
                self._column_filter,
            ),
        }

        return options

    def _from_dict(self, value: Dict[str, Any]) -> ColumnFilterConfig:
        return ColumnFilterConfig._from_dict(value, self._column_filter)

    def _to_dict(self, value: ColumnFilterConfig) -> Dict[str, Any]:
        return value._to_dict()


class BoolParameter(_BaseParameter):
    """
    Parameter class for primitive boolean types.
    """

    def _default_validator(self, value):
        if not isinstance(value, bool):
            raise TypeError(f"{value} is not a boolean")

    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        default_value: Union[bool, DefaultValueProvider[bool]] = False,
        validator: Optional[Callable[[bool], None]] = None,
        since_version: Optional[Union[Version, str]] = None,
        is_advanced: bool = False,
    ):
        if validator is None:
            validator = self._default_validator
        super().__init__(
            label,
            description,
            default_value,
            validator,
            since_version,
            is_advanced,
        )

    def _extract_schema(self, extension_version=None, dialog_creation_context=None):
        schema = super()._extract_schema(
            dialog_creation_context=dialog_creation_context
        )
        schema["type"] = "boolean"
        return schema

    def _get_options(self, dialog_creation_context) -> dict:
        return {"format": "boolean"}


class DateTimeParameter(_BaseParameter):
    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        default_value: Union[str, datetime.date] = None,
        validator=None,
        min_value: Union[str, datetime.date] = None,
        max_value: Union[str, datetime.date] = None,
        since_version=None,
        is_advanced: bool = False,
        show_date: bool = True,
        show_time: bool = False,
        show_seconds: bool = False,
        show_milliseconds: bool = False,
        timezone: str = None,
        date_format: str = None,
    ):
        """
        Parameter class for datetime types.

        Parameters
        ----------
        label : str
            The label of the parameter in the dialog.
        description : str
            The description of the parameter in the node description and dialog.
        default_value : str or datetime
            The default value of the parameter.
        validator : function
            A function which validates the value of the parameter.
        min_value : str or datetime
            The minimum value of the parameter.
        max_value : str or datetime
            The maximum value of the parameter.
        since_version : str, optional
            The version at which this parameter was introduced. Can be omitted if the parameter is part of the first version of the node.
        is_advanced : bool
            Whether the parameter is advanced.
        show_time : bool
            Whether to show the time.
        show_seconds : bool
            Whether to show the seconds, ignored if show_time is False.
        show_milliseconds : bool
            Whether to show the milliseconds, ignored if show_time is False.
        timezone : str
            The timezone in a string format.
        date_format : str
            The date format to parse the default value.
        """

        self.show_date = show_date
        self.show_time = show_time
        self.show_seconds = show_seconds
        self.show_milliseconds = show_milliseconds
        self.timezone = timezone
        self.date_format = date_format

        # convert allowed values to datetime
        self.min_value = self._to_datetime(min_value, user_input=True)
        self.max_value = self._to_datetime(max_value, user_input=True)

        if validator is None:
            validator = self._default_validator
        default_value = self._parse_date_time_default_value(default_value)

        super().__init__(
            label,
            description,
            default_value,
            validator,
            since_version,
            is_advanced,
        )

    def _default_validator(self, value: str):
        if not value:
            return

        # convert to dt
        value = self._to_datetime(value)
        self.check_type(value)
        self.check_range(value)

    def check_range(self, value):
        if self.min_value is not None and value < self.min_value:
            raise ValueError(
                f"{value} is smaller than the minimal value {self.min_value}"
            )
        if self.max_value is not None and value > self.max_value:
            raise ValueError(f"{value} is bigger than the max value {self.max_value}")

    def check_type(self, value):
        dt_type = datetime.datetime if self.show_time else datetime.date
        if value and not (isinstance(value, str) or isinstance(value, dt_type)):
            raise TypeError(
                f"{value} is of type {type(value)}, but should be of type date or datetime."
            )

    def _to_dict(self, value: datetime.datetime) -> Optional[str]:
        if not value:
            return None

        # If timezone is present, remove it from the datetime object before converting to ISO string
        # For this parameter the timezone cannot be configured by the user but is only set by the node
        # to a constant value. Therefore, the user sees a local date&time input field.
        if self.timezone is not None and value.tzinfo is not None:
            value = value.replace(tzinfo=None)

        return value.isoformat()

    def _from_dict(self, value) -> Optional[datetime.date]:
        """Parses the value to a datetime object.

        The value can be a string or a datetime object.
        """
        return self._to_datetime(value)

    def _to_datetime(
        self, value, user_input: bool = False
    ) -> Optional[Union[datetime.date, datetime.datetime]]:
        """
        Converts the value to a datetime object.

        Parameters
        ----------
        value : str or datetime
            Can be a string or a datetime object. If the value is a string and it's a user input, the date_format
            parameter is used to parse the string. If the date format is not set, the value is parsed automatically.
            This can lead to unexpected results, if the value is ambiguous (e.g. 01/02/03 can be parsed as 2001-02-03
            or 2003-01-02).
        user_input : bool
            Whether the value is a developer input or not. Set this to True if the value was provided by the developer
            as one of the parameters (min, max, default). In these cases the date_format parameter is used to parse
            the string.

        Returns
        -------
        None or datetime
            None if the value is None, otherwise a date or datetime object
        """
        if not value:
            return None
        if isinstance(value, datetime.date):
            return value

        # parse the value to a datetime object
        if isinstance(value, str):
            value = self._parse_dt_string(value, user_input)

        if not self.show_time:
            value = value.date()

        if self.timezone is not None and not value.tzinfo:
            # NOTE: We only set the tzinfo and do not shift the value (as datetime.astimezone would do)
            # because we want the user configured local date&time with the timezone
            value = pytz.timezone(self.timezone).localize(value)
        return value

    def _parse_dt_string(self, value: str, user_input: bool):
        """
        Parses the string value to a datetime object.
        """
        # user_input indicates that this string was provided by the developer
        # -> it uses the "date_format" that the developer provided
        if self.date_format and user_input:
            try:
                return datetime.datetime.strptime(value, self.date_format)
            except Exception as e:
                raise ValueError(
                    f"Could not parse {value} to a datetime object. Please provide a string or a "
                    f"datetime object. If you provide a string please also provide a date format."
                ) from e

        # The value comes from the settings
        try:
            # Backward compatibility (KNIME AP [5.2, 5.5.2) and 5.6.0)
            # We used to append "Z" to all settings to make the work in the dialog components.
            # "Z" indicates Zulu time and a +0 offset timzone. However, none of the settings really
            # represented zoned date-time values (note that the timezone is applied after the fact).
            # Therefore, we removed the "Z" suffix before loading the value.
            # Note: This will only be true for value saved in the affected versions because
            # future versions do not save datetime with a "Z" suffix
            if str.endswith(value, "Z"):
                value = datetime.datetime.fromisoformat(value.replace("Z", ""))

                # Reproduce the following logic from previous KNIME AP versions if timezone was set
                # 1. Remove "Z" when loading the value -> local datetime
                # 2. Shift the datetime to the timezone
                if self.timezone is not None:
                    # Notice that replacing the timezone later will have no effect
                    return value.astimezone(pytz.timezone(self.timezone))
                else:
                    return value
            else:
                # NOTE: This branch can be reached for values saved with the KNIME AP versions listed above.
                # If the timezone was set and the dialog was applied twice we saved values like
                # `2025-08-12T13:00:00+04:00`. The zone relates to the timezone parameter.
                # This does not need to be handled explicitly. We just replace the zone later but it won't
                # affect the value.
                return datetime.datetime.fromisoformat(value)

        except:  # NOSONAR
            # if the value is not in ISO format, we try to parse it automatically
            try:
                return parser.parse(value)
            except Exception as e:
                raise ValueError(
                    f"Could not parse {value} to a datetime object. Please provide a string in ISO format or a "
                    f"datetime object. If you don't provide the string in ISO format, please also provide a date "
                    f"format."
                ) from e

    def _extract_schema(self, extension_version=None, dialog_creation_context=None):
        prop = super()._extract_schema(dialog_creation_context=dialog_creation_context)
        prop["type"] = "string"
        prop["format"] = "date-time"
        return prop

    def _get_options(self, dialog_creation_context) -> dict:
        return {
            "format": "date-time",
            "showDate": self.show_date,
            "showTime": self.show_time,
            "showSeconds": self.show_seconds,
            "showMilliseconds": self.show_milliseconds,
            "timezone": self.timezone,
        }

    def _parse_date_time_default_value(self, default_value: Union[str, datetime.date]):
        """
        Parses the default value to a datetime object.

        The parsing is done differently as in _to_datetime, because if the default value is a string, it is not always in
        the ISO 8601 format.



        Parameters
        ----------
        default_value : str or datetime
            Can be a string or a datetime object. If the default value is a string, the date_format
            parameter is used to parse the string. If the date format is not set, the default value is
            parsed automatically. This can lead to unexpected results, if the default value is ambiguous
            (e.g. 01/02/03 can be parsed as 2001-02-03 or 2003-01-02).
        """
        if not default_value:
            # we either return the borders or the current date
            if self.max_value is not None:
                return self.max_value
            if self.min_value is not None:
                return self.min_value

            if self.show_time:
                return datetime.datetime.now()
            else:
                return datetime.datetime.now().date()

        if not (
            isinstance(default_value, datetime.date) or isinstance(default_value, str)
        ):
            raise ValueError(
                f"Cannot parse default value {default_value}. Please provide a string or datetime object."
                f"If you provide a string, please also provide a date format."
            )

        return self._to_datetime(default_value, user_input=True)


def _flatten(lst: list) -> list:
    flat = []
    for e in lst:
        if isinstance(e, list):
            flat = flat + _flatten(e)
        else:
            flat.append(e)
    return flat


def get_attr_from_instance(instance, attr):
    ancestor_classes = inspect.getmro(type(instance))[:-1]
    try:
        return instance.__dict__[attr]
    except KeyError:
        for cls in ancestor_classes:
            try:
                return cls.__dict__[attr]
            except KeyError:
                continue


class GetSetBase:
    """
    This base class is needed primarily to allow composition of parameters and parameter groups.

    Since in the case of composition the `__set__` and `__get__` methods don't get automatically called, we
    resort to calling them manually by catching the appropriate object type (e.g. `_BaseParameter`).
    """

    def __getattribute__(self, name):
        obj = super().__getattribute__(name)
        if isinstance(obj, _BaseParameter):
            return obj._get_value(self, name)

        return obj

    def __setattr__(self, name, value):
        if not hasattr(self, name):
            super().__setattr__(name, value)
        else:
            if name in self.__parameters__:
                obj = super().__getattribute__(name)
                if not isinstance(obj, _BaseParameter):
                    obj = get_attr_from_instance(self, name)
                obj._set_value(self, value, name)
            else:
                super().__setattr__(name, value)


class LayoutDirection(Enum):
    """
    LayoutDirection defines the possible layout directions
    for parameteter groups and parameter arrays. It specifies whether the layout should be
    arranged vertically or horizontally.
    """

    VERTICAL = "VerticalLayout"
    HORIZONTAL = "HorizontalLayout"


def parameter_group(
    label: str,
    since_version: Optional[Union[Version, str]] = None,
    is_advanced: bool = False,
    layout_direction: LayoutDirection = LayoutDirection.VERTICAL,
):
    """
    Decorator for classes implementing parameter groups. Parameter group classes can define parameters
    and other parameter groups both as class-level attributes and as instance-level attributed inside the `__init__` method.

    Parameter group classes can set values for their parameters inside the `__init__` method during the constructor call (e.g.
    from the node containing the group, or another group). Note: when declaring the keyword arguments for the `__init__` method
    of your parameter group class, you should refrain from using keywords from the following list of reserved keywords:
    `since_version`, `is_advanced`, and `validator`. These are used by the wrapper class in order to enable the backend functionality.

    Group validators need to raise an exception if a `values`-based condition is violated, where `values` is a dictionary
    of parameter names and values.
    Group validators can be set using either of the following methods:

    - By implementing the "validate(self, values)" method inside the class definition of the group.

    Examples
    ---------

    >>> def validate(self, values):
    ...     assert values['first_param'] + values['second_param'] < 100

    - By using the "@group_name.validator" decorator notation inside the class definition of the "parent" of the group.
      The decorator has an optional 'override' parameter, set to True by default, which overrides the "validate" method.
      If 'override' is set to False, the "validate" method, if defined, will be called first.


    >>> @hyperparameters.validator(override=False)
    ... def validate_hyperparams(values):
    ...     assert values['first_param'] + values['second_param'] < 100

    or

    >>> @knext.parameter_group(label="My Settings")
    ... class MySettings:
    ...     name = knext.StringParameter("Name", "The name of the person", "Bario")
    ...     num_repetitions = knext.IntParameter("NumReps", "How often do we repeat?", 1, min_value=1)
    ...
    ...     @num_repetitions.validator
    ...     def reps_validator(value):
    ...         if value == 2:
    ...             raise ValueError("I don't like the number 2")
    ...
    ... @knext.node(args)
    ... class MyNodeWithSettings:
    ...     settings = MySettings()
    ...     def configure(args):
    ...         pass
    ...
    ...     def execute(args):
    ...         pass
    """

    def decorate_class(original_class):
        class ParameterGroupHolder(GetSetBase, original_class):
            """
            A wrapper class that inherits from the custom parameter group class in order to retain
            its methods and attributes, while also adding a layer on top to enable the backend infrastructure.
            """

            __kind__ = "parameter_group"

            def __init__(self, *args, **kwargs):
                self._check_keyword_compliance()

                # The since_version and is_advanced parameters for a parameter group can be provided
                # either through the @parameter_group decorator, or directly through the constructor of its class.
                # The latter takes precedence over the former, if provided.
                self._since_version = Version.parse_version(
                    self._parse_kwarg(kwargs, "since_version", since_version)
                )
                self._is_advanced = self._parse_kwarg(
                    kwargs, "is_advanced", is_advanced
                )
                self._rule = None
                self._validator = self._parse_kwarg(kwargs, "validator", None)
                self._override_internal_validator = False
                self._label = label
                self._layout_direction = layout_direction.value
                self.__parameters__ = {}

                # preserve the docstring describing the parameter group
                self.__doc__ = original_class.__doc__

                super().__init__(*args, **kwargs)

            def _check_keyword_compliance(self):
                """
                The __init__ method of the wrapped class should not use keyword arguments that
                are reserved by the wrapper class to enable the backend functionality.
                """
                reserved_keywords_set = set(
                    ["since_version", "is_advanced", "validator"]
                )
                original_declared_params_set = set(
                    inspect.signature(super().__init__).parameters
                )

                if (
                    len(
                        reserved_keywords_set.intersection(original_declared_params_set)
                    )
                    > 0
                ):
                    raise SyntaxError(
                        "Please refrain from declaring reserved keyword arguments in the `__init__` method of your parameter group class."
                    )

            def _parse_kwarg(self, kwargs, arg_key, default):
                """
                Fetches the keyword argument from the provided dict, and deletes the entry in the dict afterwards
                to enable the kwargs to be provided to the super.__init__() method.
                """
                if arg_key in kwargs:
                    result = kwargs[arg_key]
                    del kwargs[arg_key]
                    return result

                return default

            def __set_name__(self, owner, name):
                self._name = name

            def _is_descriptor(self):
                """
                A parameter_group used as descriptor i.e. declared on class level needs to be
                handled differently than a parameter_group that is used via composition i.e. passed via __init__.
                Here we use the _name attribute set via __set_name__ to distinguish the two since __set_name__ is only
                called if the parameter_group is used as a descriptor, namely when it is declared in the class definition.
                """
                return hasattr(self, "_name")

            def __get__(self, obj, obj_type=None):
                """
                Descriptors: Create a deepcopy of the decorated class instance and inject the parameters.

                Composed: return this instance.
                """
                assert self._is_descriptor(), (
                    "__get__ should only be called if the paramter_group is used as a descriptor."
                )
                return self._get_param_holder(obj)

            def _get_value(self, obj, name, for_dialog: bool = False) -> Dict[str, Any]:
                param_holder = self._get_param_holder(obj)

                return {
                    name: param_obj._get_value(param_holder, name, for_dialog)
                    for name, param_obj in _get_parameters(param_holder).items()
                }

            def _extract_value(self, obj, name, for_dialog) -> Dict[str, Any]:
                param_holder = self._get_param_holder(obj)
                return {
                    name: param_obj._extract_value(param_holder, name, for_dialog)
                    for name, param_obj in _get_parameters(param_holder).items()
                }

            def _get_param_holder(self, obj):
                """
                When parameters are descriptor based, obj will always be either the root parameterized object
                (node), a ParameterGroupHolder-wrapped class, or a copied instance of it referencing a subdict of the root.__parameters__ dict.

                When parameters are composed, the instance self.__parameters__ dict is used instead, so the holder is self.
                """
                return self._copy_and_inject(obj) if self._is_descriptor() else self

            def _adjust_composed_children(self, copied_self):
                """
                During deepcopying, self's attributes also get copied. Since composed parameter groups are holders of their own
                __parameters__ dictionaries instead of referencing a subdict of their parent's __parameters__ dictionary, we need
                to "return" the composed parameter group instances of the deepcopied self to point to the original instances.
                This results in parameter injection and extraction accessing the same __parameters__ dictionaries of composed parameter groups.
                """
                for name, param_obj in _get_parameters(self).items():
                    if _is_group(param_obj) and not hasattr(param_obj, "_name"):
                        copied_self.__dict__[name] = param_obj

            def _copy_and_inject(self, obj):
                """
                Copies self and ensures that it has the parameters from obj. Additionally, composed parameter group
                children are adjusted to be that of the original self instead of new objects created during deepcopy to
                allow for parameter injection and extraction.
                """
                copied_instance = deepcopy(self)
                self._adjust_composed_children(copied_instance)

                if not hasattr(obj, "__kind__"):
                    # obj is the object that contains this parameter group
                    _create_param_dict_if_not_exists(obj)

                if self._name not in obj.__parameters__:
                    # values of parameters of the group were initialised during the constructor and act as defaults
                    if hasattr(copied_instance, "__parameters__"):
                        obj.__parameters__[self._name] = copied_instance.__parameters__
                    else:
                        parameter_dict = {}
                        obj.__parameters__[self._name] = parameter_dict
                        copied_instance.__parameters__ = parameter_dict
                else:
                    # inject the parameters stored in the object into the copied instance
                    copied_instance.__parameters__ = obj.__parameters__[self._name]

                return copied_instance

            def __set__(self, obj, values):
                raise RuntimeError("Cannot set parameter group values directly.")

            def _set_default_for_version(
                self, obj, name, version: Version, exclude_validations: bool = False
            ):
                param_holder = self._get_param_holder(obj)
                # Assumes that if a parameter group is new, so are its elements
                for name, param_obj in _get_parameters(param_holder).items():
                    param_obj._set_default_for_version(
                        param_holder, name, version, exclude_validations
                    )

            def _inject(
                self,
                obj,
                values,
                name,
                parameters_version: Version,
                exclude_validations: bool = False,
            ):
                param_holder = self._get_param_holder(obj)
                _inject_parameters(
                    param_holder, values, parameters_version, exclude_validations
                )

            def __str__(self):
                group_name = self._label if not hasattr(self, "_name") else self._name
                return f"\tGroup name: {group_name}\n\tGroup label: {self._label}"

            def _validate(self, values, version: Version):
                # validate individual parameters
                _validate_parameters(self, values, version)

                # use the "internal" group validator if exists
                if hasattr(self, "validate") and not self._override_internal_validator:
                    self.validate(values)

                # use the decorator-defined validator if exists
                if self._validator is not None:
                    self._validator(values)

            _validate_specs = validate_specs

            def validator(self, override=None):
                """
                To be used as a decorator for setting a validator function for a parameter.
                """
                if isinstance(override, bool):

                    def decorator(func):
                        self._validator = func
                        self._override_internal_validator = override

                    return decorator
                else:
                    func = override
                    self._validator = func
                    self._override_internal_validator = True

            def _extract_ui_schema(
                self,
            ):
                options = {}
                if self._is_advanced:
                    options["isAdvanced"] = True

                return {
                    "label": self._label,
                    "options": options,
                }

            def _extract_description(self, name, parent_scope: _Scope):
                scope = parent_scope.create_child(name, is_group=True)
                options = _extract_parameter_descriptions(
                    self,
                    scope,
                )

                if scope.level() == 1:
                    # this is a tab
                    return {
                        "name": self._label,
                        "description": self.__doc__,
                        "options": _flatten(options),
                    }
                else:
                    return options

            def _extract_schema(
                self, extension_version: Version, dialog_creation_context=None
            ):
                properties = {}
                for name, param_obj in _get_parameters(self).items():
                    if param_obj._since_version <= extension_version:
                        properties[name] = param_obj._extract_schema(
                            extension_version=extension_version,
                            dialog_creation_context=dialog_creation_context,
                        )

                return {"type": "object", "properties": properties}

        if hasattr(ParameterGroupHolder, "rule"):
            LOGGER.warning(
                f"Can't add the rule method to the parameter group {original_class} because it already has an attribute named 'rule'."
            )
        else:

            def rule(self, condition: Condition, effect: Effect):
                self._rule = Rule(condition, effect)
                return self

            ParameterGroupHolder.rule = rule

        return ParameterGroupHolder

    return decorate_class


class ParameterArray(_BaseParameter):
    """
    A parameter that represents an array of parameters. Each element in the array is an instance of a parameter group.

    Example
    --------

    >>> @knext.parameter_group(label="Coffee Selections")
    ... class CoffeeSelections:
    ...     coffee_options = knext.StringParameter(
    ...         "Coffee Options",
    ...         "Enter the type of coffee you like to drink.",
    ...         default_value="Watery",
    ...     )
    ...
    ...     number_of_cups = knext.IntParameter(
    ...         "Number of Cups",
    ...         "Enter the number of cups of coffee you usually drink.",
    ...         default_value=5,
    ...     )
    ...
    ... coffee_selection = knext.ParameterArray(
    ...     label="Coffee Selections",
    ...     description="Select the type of coffee you like to drink.",
    ...     parameters=CoffeeSelections(),
    ...     since_version="5.3.1",
    ...     button_text="Add new selection",
    ...     array_title="Selections",
    ... )

    """

    __kind__ = "parameter_array"

    def _default_validator(self, value):
        param_holder = self._parameters._get_param_holder(self._parameters)
        for _, param_obj in _get_parameters(param_holder).items():
            if isinstance(param_obj, ParameterArray):
                raise ValueError(
                    "ParameterArray instances cannot be nested within another ParameterArray."
                )
            if _is_group(param_obj):
                raise ValueError(
                    "ParameterGroup instances cannot be nested within another ParameterGroup."
                )

    def __init__(
        self,
        parameters,
        label: Optional[str] = None,
        description: Optional[str] = None,
        validator: Optional[Callable[[Any], None]] = None,
        since_version: Optional[Union[Version, str]] = None,
        is_advanced: bool = False,
        allow_reorder: bool = True,
        layout_direction: LayoutDirection = LayoutDirection.VERTICAL,
        button_text: Optional[str] = None,
        array_title: Optional[str] = None,
    ):
        """
        Parameters
        ----------
        parameters
            A parameter group instance, created using the @parameter_group decorator.
            This defines the structure of each item in the array. The parameter group
            should contain the settings that each item in the array will have.
        label : str, optional
            The label of the parameter in the dialog. It shows up as a headline of a section.
            If not specified, the name of the parameter will be used.
        description : str, optional
            The description of the parameter in the node description.
        validator : Optional[Callable]
            A function which by default checks if there are any nested ParameterArray or ParameterGroup
            instances defined.
        since_version : Union[Version, str], optional
            A string or Version object representing the version since when the object is available (default is None).
        is_advanced : bool, optional
            A boolean indicating if the object is advanced (default is False).
        allow_reorder : bool, optional
            Whether the order of parameters in the array can be changed. Defaults to True.
        layout_direction : LayoutDirection, VERTICAL or HORIZONTAL
            Layout direction for the array. Can be  HORIZONTAL or VERTICAL. Defaults to VERTICAL.
        button_text : str, optional
            Text to display on the button for adding new parameters.
        array_title : str, optional
            Title of the array of parameters. If not specified, the default "Group" will be used.
            An incremental suffix number is added to the title for each group added.
        """

        self._allow_reorder = allow_reorder
        validator = (
            _combine_validators(validator, self._default_validator)
            if validator
            else self._default_validator
        )

        self._parameters = parameters
        self._button_text = button_text
        self._layout_direction = layout_direction
        self._array_title = array_title if array_title else "Group"

        super().__init__(
            label,
            description,
            None,
            validator,
            since_version,
            is_advanced,
        )
        self._parameter_group_class = type(self._parameters)

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        values = self._get_value(obj, self._name)
        return [self._create_param_group_instance(value) for value in values]

    def validator(self, func):
        # we retain the default validator to ensure that we don't allow nested parameter arrays and groups
        self._validator = _combine_validators(func, self._default_validator)

    def _create_param_group_instance(self, value):
        instance = self._parameter_group_class()
        param_holder = instance._get_param_holder(instance)
        for param_name, param_value in value.items():
            setattr(param_holder, param_name, param_value)
        return param_holder

    def _generate_options_description(
        self, docstring: str, parent_scope: _Scope = None
    ):
        if docstring:
            lines = docstring.expandtabs().splitlines()
            indent_lvl = _get_indent_level(lines)
            indent = " " * indent_lvl
        else:
            indent = ""

        param_holder = self._parameters._get_param_holder(self._parameters)
        options_description = f"\n\n{indent}**{self._array_title}:**\n\n"
        for name, param_obj in _get_parameters(param_holder).items():
            parameter_description = param_obj._extract_description(name, parent_scope)
            options_description += f"{indent}- {parameter_description['name']} : {parameter_description['description']}\n"

        return docstring.expandtabs() + options_description

    def _generate_description(self, parent_scope):
        return self._generate_options_description(self.__doc__, parent_scope)

    def _extract_description(self, name, parent_scope: _Scope):
        return {
            "name": self._label,
            "description": self._generate_description(parent_scope),
        }

    def _extract_schema(
        self,
        extension_version=None,
        dialog_creation_context=None,
    ):
        schema = super()._extract_schema(
            dialog_creation_context=dialog_creation_context
        )
        param_holder = self._parameters._get_param_holder(self._parameters)
        item_properties = {}
        for name, param_obj in _get_parameters(param_holder).items():
            if not (_is_group(param_obj) or _is_array(param_obj)):
                # _to_dict fails if the parameter is a group or an array. The validator already
                # complains about these cases as we don't allow that. However, when the node is
                # instantiated, the schema is retrieved during configuration, so this method still
                # needs to work to allow developers to actually see the validation failure.
                param_schema = param_obj._extract_schema(
                    extension_version, dialog_creation_context
                )
                param_schema["default"] = param_obj._to_dict(param_obj._get_default())
                item_properties[name] = param_schema

        schema["type"] = "array"
        schema["items"] = {"type": "object", "properties": item_properties}
        return schema

    def _extract_ui_schema(self, dialog_creation_context):
        base_schema = super()._extract_ui_schema(dialog_creation_context)
        nested_elements = base_schema["options"]["detail"]["layout"]["elements"]
        param_holder = self._parameters._get_param_holder(self._parameters)
        for name, param_obj in _get_parameters(param_holder).items():
            element_schema = param_obj._extract_ui_schema(
                dialog_creation_context=dialog_creation_context
            )
            nested_elements.append(
                {"type": "Control", "scope": f"#/properties/{name}", **element_schema}
            )
        return base_schema

    def _get_options(self, dialog_creation_context) -> dict:
        options = {
            "addButtonText": self._button_text,
            "detail": {
                "layout": {
                    "type": (self._layout_direction.value),
                    "elements": [],
                }
            },
        }

        if self._array_title:
            options["arrayElementTitle"] = self._array_title
        if self._allow_reorder:
            options["showSortButtons"] = self._allow_reorder

        return options

    def _get_value(self, obj, name, for_dialog: bool = False):
        values = super()._get_value(obj, name, for_dialog)
        if values is None:
            return []
        else:
            return values

    def _to_dict(self, values):
        param_holder = self._parameters._get_param_holder(self._parameters)
        extracted_values = []
        for value in values:
            extracted_value = {}
            for param_name, param_obj in _get_parameters(param_holder).items():
                if param_name in value:
                    extracted_value[param_name] = param_obj._to_dict(value[param_name])
            extracted_values.append(extracted_value)
        return extracted_values

    def _from_dict(self, values):
        param_holder = self._parameters._get_param_holder(self._parameters)
        for value in values:
            for param_name, param_obj in _get_parameters(param_holder).items():
                if param_name in value:
                    value[param_name] = param_obj._from_dict(value[param_name])
        return values

    def _inject(self, obj, values, name, version, exclude_validations: bool = False):
        param_holder = self._parameters._get_param_holder(self._parameters)

        # Default values are provided by the first item (i.e. how parameters are set in the parameter group).
        # Therefore, we only set the first item here at the beginning, and not a list of values.
        if values is None:
            values = [{}]
            for param_name, param_obj in _get_parameters(param_holder).items():
                default_val = param_obj._to_dict(param_obj._get_default())
                values[0][param_name] = default_val
        return super()._inject(obj, values, name, version, exclude_validations)


def _get_indent_level(lines):
    indent = sys.maxsize
    for line in lines[1:]:
        stripped = line.lstrip()
        if stripped:
            indent = min(indent, len(line) - len(stripped))
    if indent == sys.maxsize:
        return 0
    return indent
