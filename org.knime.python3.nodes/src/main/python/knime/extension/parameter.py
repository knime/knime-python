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
import numbers
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union
from copy import deepcopy
import knime.api.schema as ks
import logging
import inspect
import sys

from knime.extension.version import Version

LOGGER = logging.getLogger("Python backend")


def _get_parameters(obj) -> Dict[str, "_BaseParameter"]:
    """
    Get all top-level parameter objects from obj, which can be the root parameterized
    object or a parameter group.
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
    result = dict()
    params = _get_parameters(obj)
    for name, param_obj in params.items():
        result[name] = param_obj._get_value(obj, name, for_dialog)

    return result


def inject_parameters(
    obj,
    parameters: dict,
    parameters_version: str = None,
) -> None:
    """
    This method injects the provided values into the parameter descriptors of the parameterised object,
    which can be a node or a parameter group.
    """
    parameters_version = Version.parse_version(parameters_version)
    _inject_parameters(obj, parameters["model"], parameters_version)


def _inject_parameters(
    obj,
    parameters: dict,
    parameters_version: Version,
) -> None:
    for name, param_obj in _get_parameters(obj).items():
        if param_obj._since_version <= parameters_version:
            if name in parameters:
                param_obj._inject(obj, parameters[name], name, parameters_version)
        else:
            # the parameter was introduced in a newer version but we might want to initialize
            # the default based on the version the workflow was created with
            param_obj._set_default_for_version(obj, parameters_version)


def validate_parameters(obj, parameters: dict, saved_version: str = None) -> None:
    """
    Perform validation on the individual parameters of obj, which can be a node or a parameter group.

    This method will raise a ValueError if a parameter violates its validator.
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


def validate_specs(obj, specs) -> None:
    """
    Checks if the provided specs are compatible with the parameter of obj.
    """
    for param in _get_parameters(obj).values():
        param._validate_specs(specs)


def extract_schema(
    obj, extension_version: str = None, specs=None, dialog_creation_context=None
) -> dict:
    extension_version = Version.parse_version(extension_version)
    return {
        "type": "object",
        "properties": {
            "model": _extract_schema(
                obj, extension_version, specs, dialog_creation_context
            )
        },
    }


def _extract_schema(
    obj, extension_version: Version, specs, dialog_creation_context=None
) -> dict:
    properties = {}
    if specs is None and dialog_creation_context is not None:
        specs = dialog_creation_context.get_input_specs()

    for name, param_obj in _get_parameters(obj).items():
        if param_obj._since_version <= extension_version:
            properties[name] = param_obj._extract_schema(
                extension_version=extension_version,
                specs=specs,
                dialog_creation_context=dialog_creation_context,
            )

    return {"type": "object", "properties": properties}


def extract_ui_schema(
    obj, extension_version: str = None, dialog_creation_context=None
) -> dict:
    extension_version = Version.parse_version(extension_version)
    elements = _extract_ui_schema_elements(
        obj, extension_version, dialog_creation_context
    )

    return {"type": "VerticalLayout", "elements": elements}


def _extract_ui_schema_elements(
    obj, extension_version: Version, dialog_creation_context=None, scope=None
) -> dict:
    if scope is None:
        scope = _Scope("#/properties/model/properties")
    elements = []
    for name, param_obj in _get_parameters(obj).items():
        if param_obj._since_version <= extension_version:
            elements.append(
                param_obj._extract_ui_schema(
                    name, scope, extension_version, dialog_creation_context
                )
            )

    return elements


def extract_parameter_descriptions(obj) -> dict:
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
            description for description in descriptions if not "options" in description
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
    Determine if backward/forward compatibility is taking place based on the saved and current version
    of the extension, and provide appropriate feedback to the user.

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
        Args:
            label: The label to display for the parameter in the dialog
            description: The description of the parameter that is shown in the node description and the dialog
            default_value: Either the default value for the parameter or a function that given a Version provides the default value for that version.
            validator: Optional validation function
            since_version: The version at which this parameter was introduced. Can be omitted for the first version of a node
        """
        self._label = label
        self._default_value = default_value
        self._validator = validator if validator is not None else _default_validator
        self.__doc__ = description if description is not None else ""
        self._since_version = Version.parse_version(since_version)
        self._is_advanced = is_advanced

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

    def __get__(self, obj, objtype=None):
        """
        Only ever called when `self` is a descriptor, thus having the `_name` attribute.
        """
        return self._get_value(obj, self._name)

    def _get_default(self, version: Version = None):
        if callable(self._default_value):
            if version is None:
                return self._default_value(_get_extension_version())
            else:
                return self._default_value(version)
        else:
            return self._default_value

    def _set_default_for_version(self, obj, version: Version):
        self.__set__(obj, self._get_default(version))

    def _inject(self, obj, value, name, parameters_version: Version = None):  # NOSONAR
        # the parameters_version parameter are needed to match the signature of the
        # _inject method for parameter groups
        self._set_value(obj, value, name)

    def _set_value(self, obj, value, name):
        if not hasattr(obj, "__kind__"):
            # obj is the root parameterised object
            _create_param_dict_if_not_exists(obj)

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

        **Example**::

                @knext.node(args)
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

                    def configure(args):
                        pass

                    def execute(args):
                        pass
        """
        self._validator = func

    def _extract_schema(
        self,
        extension_version: Version = None,  # NOSONAR
        specs: List[ks.Schema] = None,  # NOSONAR
        dialog_creation_context=None,  # NOSONAR
    ):
        return {"title": self._label, "description": self.__doc__}

    def _extract_ui_schema(
        self,
        name,
        parent_scope: _Scope,
        extension_version: Version = None,  # NOSONAR
        dialog_creation_context=None,
    ):
        # the extension_version parameter is needed to match the signature of the
        # _extract_ui_schema method for parameter groups
        options = self._get_options(dialog_creation_context)
        if self._is_advanced:
            options["isAdvanced"] = True

        return {
            "type": "Control",
            "label": self._label,
            "scope": str(parent_scope.create_child(name)),
            "options": options,
        }

    @abstractmethod
    def _get_options(self, dialog_creation_context=None) -> dict:
        pass

    def _extract_description(self, name, parent_scope: _Scope):  # NOSONAR
        return {"name": self._label, "description": self.__doc__}


class _NumericParameter(_BaseParameter):
    """
    Note: subclasses of this class must implement the check_type method for the default_validator.
    """

    def default_validator(self, value):
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
            validator = self.default_validator
        super().__init__(
            label, description, default_value, validator, since_version, is_advanced
        )

    def check_range(self, value):
        if self.min_value is not None and value < self.min_value:
            raise ValueError(
                f"{value} is smaller than the minimal value {self.min_value}"
            )
        if self.max_value is not None and value > self.max_value:
            raise ValueError(f"{value} is > the max value {self.max_value}")

    def _extract_schema(
        self, extension_version=None, specs=None, dialog_creation_context=None
    ):
        schema = super()._extract_schema(
            specs, dialog_creation_context=dialog_creation_context
        )
        schema["type"] = "number"
        if self.min_value is not None:
            schema["minimum"] = self.min_value
        if self.max_value is not None:
            schema["maximum"] = self.max_value
        return schema


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

    def _extract_schema(
        self, extension_version=None, specs=None, dialog_creation_context=None
    ):
        prop = super()._extract_schema(
            specs, dialog_creation_context=dialog_creation_context
        )
        prop["type"] = "integer"
        prop["format"] = "int32"
        return prop

    def _get_options(self, dialog_creation_context=None) -> dict:
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

    def _extract_schema(
        self, extension_version=None, specs=None, dialog_creation_context=None
    ):
        schema = super()._extract_schema(
            specs, dialog_creation_context=dialog_creation_context
        )
        schema["format"] = "double"
        return schema

    def _get_options(self, dialog_creation_context=None) -> dict:
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
            label, description, default_value, validator, since_version, is_advanced
        )

    def _get_options(self, dialog_creation_context=None) -> dict:
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
    """

    def default_validator(self, value):
        if not isinstance(value, str):
            raise TypeError(
                f"{value} is of type {type(value)}, but should be of type string."
            )

    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        default_value: Union[str, DefaultValueProvider[str]] = "",
        enum: Optional[List[str]] = None,
        validator: Optional[Callable[[str], None]] = None,
        since_version: Optional[Union[Version, str]] = None,
        is_advanced: bool = False,
        choices: Callable = None,
    ):
        if validator is None:
            validator = self.default_validator
        if enum is not None and not isinstance(enum, list):
            raise TypeError("The enum parameter must be a list.")
        if enum and choices:
            raise ValueError("Cannot specify both enum and choices")
        self._enum = enum
        self._choices = choices
        super().__init__(
            label, description, default_value, validator, since_version, is_advanced
        )

    def _extract_schema(
        self, extension_version=None, specs=None, dialog_creation_context=None
    ):
        schema = super()._extract_schema(
            specs, dialog_creation_context=dialog_creation_context
        )

        if self._choices and dialog_creation_context:
            schema["oneOf"] = [
                {"const": e, "title": e} for e in self._choices(dialog_creation_context)
            ]
        elif self._enum is None:
            schema["type"] = "string"
        else:
            schema["oneOf"] = [{"const": e, "title": e} for e in self._enum]
        return schema


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

    **Example**::

        class CoffeeOptions(EnumParameterOptions):
            CLASSIC = ("Classic", "The classic chocolatey taste, with notes of bitterness and wood.")
            FRUITY = ("Fruity", "A fruity taste, with notes of berries and citrus.")
            WATERY = ("Watery", "A watery taste, with notes of water and wetness.")
    """

    def __init__(self, label, description):
        self.label = label
        self.description = description

    @classmethod
    def _generate_options_description(cls, docstring: str):
        # ensure that the options description is indented correctly
        if docstring:
            lines = docstring.expandtabs().splitlines()
            indent_lvl = _get_indent_level(lines)
            indent = " " * indent_lvl
        else:
            indent = ""

        options_description = f"\n\n{indent}**Available options:**\n\n"
        for member in cls._member_names_:
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
    previously implemented as part of ``StringParameter``.

    A subclass of EnumParameterOptions should be provided as the enum parameter, which should contain
    class attributes of the form ``OPTION_NAME = (OPTION_LABEL, OPTION_DESCRIPTION)``. The corresponding
    option attributes can be accessed via ``MyOptions.OPTION_NAME.name``, ``.label``, and ``.description``
    respectively.

    The ``.name`` attribute of each option is used as the selection constant, e.g.
    ``MyOptions.OPTION_NAME.name == "OPTION_NAME"``.

    **Example**::

        class CoffeeOptions(EnumParameterOptions):
            CLASSIC = ("Classic", "The classic chocolatey taste, with notes of bitterness and wood.")
            FRUITY = ("Fruity", "A fruity taste, with notes of berries and citrus.")
            WATERY = ("Watery", "A watery taste, with notes of water and wetness.")

        coffee_selection_param = knext.EnumParameter(
            label="Coffee Selection",
            description="Select the type of coffee you like to drink.",
            default_value=CoffeeOptions.CLASSIC.name,
            enum=CoffeeOptions,
        )
    """

    def default_validator(self, value):
        if value not in self._enum.get_all_options():
            raise ValueError(
                f"""The selection '{value}' for parameter '{self._label}' is not one of the available options: {"'" + "', '".join(self._enum.get_all_options()) + "'"}."""
            )

    def __init__(
        self,
        label: Optional[str] = None,
        description: Optional[str] = None,
        default_value: Union[str, DefaultValueProvider[str]] = None,
        enum: Optional[EnumParameterOptions] = None,
        validator: Optional[Callable[[str], None]] = None,
        since_version: Optional[Union[Version, str]] = None,
        is_advanced: bool = False,
    ):
        if validator is None:
            validator = self.default_validator
        else:
            validator = self.add_default_validator(validator)

        if enum is None or len(enum.get_all_options()) == 0:
            self._enum = EnumParameterOptions._get_default_option()
            default_value = self._enum.DEFAULT_OPTION.name
        else:
            self._enum = enum
            if default_value is None:
                default_value = self._enum.get_all_options()[0].name

        super().__init__(
            label, description, default_value, validator, since_version, is_advanced
        )

    def _generate_description(self):
        return self._enum._generate_options_description(self.__doc__)

    def _extract_schema(
        self, extension_version=None, specs=None, dialog_creation_context=None
    ):
        schema = super()._extract_schema(
            specs, dialog_creation_context=dialog_creation_context
        )
        schema["description"] = self._generate_description()
        schema["oneOf"] = [{"const": e.name, "title": e.label} for e in self._enum]
        return schema

    def _extract_description(self, name, parent_scope: _Scope):
        return {"name": self._label, "description": self._generate_description()}

    def add_default_validator(self, func):
        def combined_validator(value):
            # we retain the default validator to ensure that value is always one of the available options
            self.default_validator(value)
            func(value)

        return combined_validator

    def validator(self, func):
        self._validator = self.add_default_validator(func)


class _BaseColumnParameter(_BaseParameter):
    """
    Base class for single and multi column selection parameters.
    """

    def __init__(
        self,
        label,
        description,
        port_index: int,
        column_filter: Callable[[ks.Column], bool],
        schema_option: str,
        since_version=None,
        is_advanced=False,
    ):
        """
        Args:
            label: Label of the parameter in the dialog
            description: Description of the parameter in the node description and dialog
            port_index: The input port to select columns from
            column_filter: A function for prefiltering columns
            since_version: The version at which this parameter was introduced. Can be omitted if the parameter is part of the first version of the node.
        """
        super().__init__(
            label,
            description,
            default_value=None,
            since_version=since_version,
            is_advanced=is_advanced,
        )
        self._port_index = port_index
        if column_filter is None:
            column_filter = lambda c: True
        self._column_filter = column_filter
        self._schema_option = schema_option

    def _extract_schema(
        self,
        extension_version=None,
        specs: List[ks.Schema] = None,
        dialog_creation_context=None,
    ):
        schema = super()._extract_schema(
            specs, dialog_creation_context=dialog_creation_context
        )
        values = _filter_columns(specs, self._port_index, self._column_filter)

        if self._schema_option != None:
            schema[self._schema_option] = values
        return schema

    def _validate_specs(self, specs):
        if len(specs) <= self._port_index:
            raise ValueError(
                f"There are too few input specs. The column selection '{self._name}' requires a table input at index {self._port_index}"
            )
        elif not isinstance(specs[self._port_index], ks.Schema):
            raise TypeError(
                f"The port index ({self._port_index}) of the column selection '{self._name}' does not point to a table input."
            )


class ColumnParameter(_BaseColumnParameter):
    """
    Parameter class for single columns.
    """

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
    ):
        """
        Args:
            label: Label of the parameter in the dialog
            description: Description of the parameter in the node description and dialog
            port_index: The input port to select columns from
            column_filter: A function for prefiltering columns
            include_row_key: Whether to include the row keys as selectable column
            include_none_column: Whether to allow to select no column
            since_version: The version at which this parameter was introduced. Can be omitted if the parameter is part of the first version of the node.
        """
        super().__init__(
            label,
            description,
            port_index,
            column_filter,
            "oneOf",
            since_version,
            is_advanced,
        )
        self._include_row_key = include_row_key
        self._include_none_column = include_none_column

    def _get_options(self, dialog_creation_context=None) -> dict:
        options = {
            "format": "dropDown",
            "showRowKeys": self._include_row_key,
            "showNoneColumn": self._include_none_column,
        }

        if dialog_creation_context is not None:
            options["possibleValues"] = _possible_values(
                dialog_creation_context.get_input_specs(),
                self._port_index,
                self._column_filter,
            )

        return options

    def _inject(self, obj, value, name, version):
        value = None if value == "" else value
        return super()._inject(obj, value, name, version)


def _filter_columns(
    specs: List[ks.PortObjectSpec],
    port_index: int,
    column_filter: Callable[[ks.Column], bool],
):
    try:
        if specs is None or specs[port_index] is None:
            return [_const("", preferred_type=None, displayed_type=None)]

        spec = specs[port_index]
    except IndexError:
        raise IndexError(
            f"The port index {port_index} is not contained in the Spec list with length {len(specs)}. "
            f"Maybe a port_index for a parameter does not match the index for an input table? "
        ) from None

    if not isinstance(spec, ks.Schema):
        raise TypeError(
            f"The port at index {port_index} is not a Table. "
            f"The ColumnParameter or MultiColumnParameter can only be used for Table ports."
        )

    filtered = [
        _const(
            column.name,
            column.metadata["preferred_value_type"],
            column.metadata["displayed_column_type"],
        )
        for column in spec
        if column_filter(column)
    ]
    if len(filtered) > 0:
        return filtered
    else:
        return [_const("", preferred_type=None, displayed_type=None)]


def _const(name, preferred_type, displayed_type):
    return {
        "const": name,
        "title": name,
        "columnType": preferred_type,
        "columnTypeDisplayed": displayed_type,
    }


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
        super().__init__(
            label,
            description,
            port_index,
            column_filter,
            "anyOf",
            since_version,
            is_advanced,
        )

    def _get_options(self, dialog_creation_context=None) -> dict:
        options = {"format": "twinList"}

        if dialog_creation_context is not None:
            options["possibleValues"] = _possible_values(
                dialog_creation_context.get_input_specs(),
                self._port_index,
                self._column_filter,
            )
        return options

    def _get_value(self, obj: Any, name, for_dialog: bool = False):
        value = super()._get_value(obj, name, for_dialog)
        if for_dialog and value is None:
            return []
        else:
            return value

    def _inject(self, obj, value, name, version):
        # if there are no columns then the empty string is used as placeholder and we need to filter it out here
        if value is not None:
            value = [c for c in value if c != ""]
        return super()._inject(obj, value, name, version)


def _possible_values(
    specs: List[ks.PortObjectSpec],
    port_index: int,
    column_filter: Callable[[ks.Column], bool],
):
    def entry(name, type, compatible_types):
        entry = {"id": name, "text": name}
        if type is not None:
            entry["type"] = {"id": type[0], "text": type[1]}
        if compatible_types is not None:
            entry["compatibleTypes"] = compatible_types
        return entry

    try:
        if specs is None or specs[port_index] is None:
            return [entry("", "", "")]

        spec = specs[port_index]
    except IndexError:
        raise IndexError(
            f"The port index {port_index} is not contained in the spec list with length {len(specs)}. "
            f"Maybe the port_index does not match the index of the corresponding input table? "
        ) from None

    if not isinstance(spec, ks.Schema):
        raise TypeError(
            f"The port at index {port_index} is not a table. "
            f"The ColumnFilter can only be used for table ports. "
            f"Available specs are: {specs}"
        )

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
        return [entry("", "", "")]


class ColumnFilterMode(Enum):
    """
    The modes that can be selected in the ColumnFilterParameter
    """

    MANUAL = "Manual"
    REGEX = "Regex"
    WILDCARD = "Wildcard"
    TYPE = "Type"


class PatternFilterConfig:
    """
    The pattern configuration for the ColumnFilterParameter, used for both, regex and wildcard patterns
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
            pattern_matches = pattern.search(column.name) != None
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
    The type filter configuration for the ColumnFilterParameter
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
    The value of a ``ColumnFilterParameter`` is a ``ColumnFilterConfig`` instance with a mode as well
    as configuration for the different modes.

    Use the ``apply`` method to filter schemas and tables according to this filter config

    **Example**::

        @knext.node(
            name="Python Column Filter",
            node_type=knext.NodeType.MANIPULATOR,
            icon_path=...,
            category=...,
        )
        @knext.input_table("Input Table", "Input table.")
        @knext.output_table("Output Table", "Output table.")
        class ColumnFilterNode:
            column_filter = knext.ColumnFilterParameter("Column Filter", "Column Filter")

            def configure(self, config_context, input_schema: knext.Schema):
                return self.column_filter.apply(input_schema)

            def execute(self, exec_context, input_table):
                return self.column_filter.apply(input_table)
    """

    def __init__(
        self,
        mode=ColumnFilterMode.MANUAL,
        pattern_filter: PatternFilterConfig = None,
        type_filter: TypeFilterConfig = None,
        manual_filter: ManualFilterConfig = None,
        included_column_names: List[str] = None,
    ):
        """
        Construct a ``ColumnFilterConfig`` with given ``mode``, ``pattern_filter``, ``type_filter`` and ``manual_filter``.

        If ``included_column_names`` are provided, ``manual_filter`` and ``mode`` will be ignored and will be configured
        such that only the explicitly ``included_column_names`` (and unknown columns) are selected.
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

    @classmethod
    def _from_dict(cls, value):
        return cls(
            mode=ColumnFilterMode[value["mode"]],
            pattern_filter=PatternFilterConfig._from_dict(value["patternFilter"]),
            type_filter=TypeFilterConfig._from_dict(value["typeFilter"]),
            manual_filter=ManualFilterConfig._from_dict(value["manualFilter"]),
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

        filtered_column_names = []
        if self.mode == ColumnFilterMode.MANUAL:
            filtered_column_names = self.manual_filter._apply(schema)
        elif (
            self.mode == ColumnFilterMode.REGEX
            or self.mode == ColumnFilterMode.WILDCARD
        ):
            filtered_column_names = self.pattern_filter._apply(
                schema, regex=self.mode == ColumnFilterMode.REGEX
            )
        elif self.mode == ColumnFilterMode.TYPE:
            filtered_column_names = self.type_filter._apply(schema)
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
        column_filter: Callable[[ks.Column], bool] = None,
        since_version: Optional[Union[str, Version]] = None,
        is_advanced: bool = False,
    ):
        super().__init__(
            label,
            description,
            port_index,
            column_filter,
            None,
            since_version,
            is_advanced,
        )

        self._column_filter_config = ColumnFilterConfig()

    def _extract_schema(
        self,
        extension_version=None,
        specs: List[ks.Schema] = None,
        dialog_creation_context=None,
    ):
        schema = super()._extract_schema(
            specs, dialog_creation_context=dialog_creation_context
        )

        schema["type"] = "object"
        schema["properties"] = self._column_filter_config._extract_schema()

        return schema

    def _get_options(self, dialog_creation_context=None) -> dict:
        options = {"format": "columnFilter", "showSearch": True, "showMode": True}

        if dialog_creation_context is not None:
            options["possibleValues"] = _possible_values(
                dialog_creation_context.get_input_specs(),
                self._port_index,
                self._column_filter,
            )

        return options

    def __get__(self, obj, objtype=None):
        value = super().__get__(obj, objtype)

        # Turn dict into ColumnFilterConfig for better use in Python nodes
        if value is not None:
            return ColumnFilterConfig._from_dict(value)
        else:
            return None

    def __set__(self, obj, value):
        # Turn ColumnFilterConfig into dict, because the UI-side needs a dict
        if value is not None:
            value = value._to_dict()
        return super().__set__(obj, value)

    def _inject(self, obj, value, name, version):
        if value is None:
            value = ColumnFilterConfig()._to_dict()

        return super()._inject(obj, value, name, version)


class BoolParameter(_BaseParameter):
    """
    Parameter class for primitive boolean types.
    """

    def default_validator(self, value):
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
            validator = self.default_validator
        super().__init__(
            label, description, default_value, validator, since_version, is_advanced
        )

    def _extract_schema(
        self, extension_version=None, specs=None, dialog_creation_context=None
    ):
        schema = super()._extract_schema(
            specs, dialog_creation_context=dialog_creation_context
        )
        schema["type"] = "boolean"
        return schema

    def _get_options(self, dialog_creation_context=None) -> dict:
        return {"format": "boolean"}


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

    Since in the case of composition the __set__ and __get__ methods don't get automatically called, we
    resort to calling them manually by catching the appropriate object type (e.g. _BaseParameter).
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


def parameter_group(
    label: str,
    since_version: Optional[Union[Version, str]] = None,
    is_advanced: bool = False,
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

    **Example**::

        def validate(self, values):
            assert values['first_param'] + values['second_param'] < 100

    - By using the "@group_name.validator" decorator notation inside the class definition of the "parent" of the group.
      The decorator has an optional 'override' parameter, set to True by default, which overrides the "validate" method.
      If 'override' is set to False, the "validate" method, if defined, will be called first.

    **Example**::

        @hyperparameters.validator(override=False)
        def validate_hyperparams(values):
            assert values['first_param'] + values['second_param'] < 100

    **Example**::

        @knext.parameter_group(label="My Settings")
        class MySettings:
            name = knext.StringParameter("Name", "The name of the person", "Bario")
            num_repetitions = knext.IntParameter("NumReps", "How often do we repeat?", 1, min_value=1)

            @num_repetitions.validator
            def reps_validator(value):
                if value == 2:
                    raise ValueError("I don't like the number 2")

        @knext.node(args)
        class MyNodeWithSettings:
            settings = MySettings()
            def configure(args):
                pass

            def execute(args):
                pass
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
                self._validator = self._parse_kwarg(kwargs, "validator", None)
                self._override_internal_validator = False
                self._label = label
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
                called if the parameter_group is used as descriptor, namely when it is declared in the class definition.
                """
                return hasattr(self, "_name")

            def __get__(self, obj, obj_type=None):
                """
                Descriptors: Create a deepcopy of the decorated class instance and inject the parameters.

                Composed: return this instance.
                """
                assert (
                    self._is_descriptor()
                ), "__get__ should only be called if the paramter_group is used as a descriptor."
                return self._get_param_holder(obj)

            def _get_value(self, obj, name, for_dialog: bool = False):
                param_holder = self._get_param_holder(obj)

                return {
                    name: param_obj._get_value(param_holder, name, for_dialog)
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

            def _set_default_for_version(self, obj, version: Version):
                param_holder = self._get_param_holder(obj)
                # Assumes that if a parameter group is new, so are its elements
                for param_obj in _get_parameters(param_holder).values():
                    param_obj._set_default_for_version(param_holder, version)

            def _inject(self, obj, values, name, parameters_version: Version):
                param_holder = self._get_param_holder(obj)
                _inject_parameters(param_holder, values, parameters_version)

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
                name,
                parent_scope: _Scope,
                version: Version = None,
                dialog_creation_context=None,
            ):
                scope = parent_scope.create_child(name, is_group=True)
                elements = _extract_ui_schema_elements(
                    self, version, dialog_creation_context, scope
                )

                options = {}
                if self._is_advanced:
                    options["isAdvanced"] = True

                if scope.level() == 1:
                    group_type = "Section"
                else:
                    group_type = "Group"

                return {
                    "type": group_type,
                    "label": self._label,
                    "options": options,
                    "elements": elements,
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
                self, extension_version: Version, specs, dialog_creation_context=None
            ):
                properties = {}
                for name, param_obj in _get_parameters(self).items():
                    if param_obj._since_version <= extension_version:
                        properties[name] = param_obj._extract_schema(
                            extension_version=extension_version,
                            specs=specs,
                            dialog_creation_context=dialog_creation_context,
                        )

                return {"type": "object", "properties": properties}

        return ParameterGroupHolder

    return decorate_class


def _get_indent_level(lines):
    indent = sys.maxsize
    for line in lines[1:]:
        stripped = line.lstrip()
        if stripped:
            indent = min(indent, len(line) - len(stripped))
    if indent == sys.maxsize:
        return 0
    return indent
