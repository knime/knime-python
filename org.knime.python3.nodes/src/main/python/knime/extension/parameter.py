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
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union
import knime.api.schema as ks
import logging

from knime.extension.version import Version

LOGGER = logging.getLogger("Python backend")


def _get_parameters(obj) -> Dict[str, "_BaseParameter"]:
    """
    Get all parameter objects from obj as a nested dict.
    """
    class_params = {
        name: param_obj
        for name, param_obj in type(obj).__dict__.items()
        if _is_parameter_or_group(param_obj)
    }
    instance_params = {
        name: param_obj
        for name, param_obj in obj.__dict__.items()
        if _is_parameter_or_group(param_obj)
    }

    return {**class_params, **instance_params}


def _get_custom_class_methods(cls) -> dict:
    """
    Get custom methods of class that are not parameters or parameter groups.
    """
    reserved_names = ["__dict__", "__init__"]

    return {
        method_name: method
        for method_name, method in cls.__dict__.items()
        if (not _is_parameter_or_group(method)) and (method_name not in reserved_names)
    }


def extract_parameters(obj, for_dialog: bool = False) -> dict:
    """
    Get all parameter values from obj as a nested dict.
    """
    return {"model": _extract_parameters(obj, for_dialog)}


def _extract_parameters(obj, for_dialog: bool) -> dict:
    result = dict()
    params = _get_parameters(obj)
    for name, param_obj in params.items():
        result[name] = param_obj._get_value(obj, for_dialog)

    return result


def inject_parameters(
    obj,
    parameters: dict,
    parameters_version: str = None,
    fail_on_missing: bool = True,
) -> None:
    """
    This method injects the provided values into the parameter descriptors of the parameterised object,
    which can be a node or a parameter group.
    """
    parameters_version = Version.parse_version(parameters_version)
    _inject_parameters(obj, parameters["model"], parameters_version, fail_on_missing)


def _inject_parameters(
    obj,
    parameters: dict,
    parameters_version: Version,
    fail_on_missing: bool,
) -> None:
    for name, param_obj in _get_parameters(obj).items():
        if param_obj._since_version <= parameters_version:
            if name in parameters:
                param_obj._inject(
                    obj, parameters[name], parameters_version, fail_on_missing
                )
            elif fail_on_missing:
                # TODO: fail_on_missing is never set to True - remove it altogether?
                raise ValueError(f"No value available for parameter '{name}'")
        else:
            # the parameter was introduced in a newer version but we might want to initialize
            # the default based on the version the workflow was created with
            param_obj._set_default_for_version(obj, parameters_version)


def validate_parameters(obj, parameters: dict, version: str = None) -> None:
    """
    Perform validation on the individual parameters of obj, which can be a node or a parameter group.

    This method will raise a ValueError if a parameter violates its validator.
    """
    version = Version.parse_version(version)
    _validate_parameters(obj, parameters["model"], version)


def _validate_parameters(obj, parameters: dict, version: Version) -> None:
    for name, param_obj in _get_parameters(obj).items():
        if param_obj._since_version <= version:
            if name in parameters:
                param_obj._validate(parameters[name], version)


def validate_specs(obj, specs) -> None:
    """
    Checks if the provided specs are compatible with the parameter of obj.
    """
    for param in _get_parameters(obj).values():
        param._validate_specs(specs)


def extract_schema(obj, extension_version: str = None, specs=None) -> dict:
    extension_version = Version.parse_version(extension_version)
    return {
        "type": "object",
        "properties": {"model": _extract_schema(obj, extension_version, specs)},
    }


def _extract_schema(obj, extension_version: Version, specs):
    properties = {}
    for name, param_obj in _get_parameters(obj).items():
        if param_obj._since_version <= extension_version:
            properties[name] = param_obj._extract_schema(
                extension_version=extension_version, specs=specs
            )

    return {"type": "object", "properties": properties}


def extract_ui_schema(obj, extension_version: str = None) -> dict:
    extension_version = Version.parse_version(extension_version)
    elements = _extract_ui_schema_elements(obj, extension_version)

    return {"type": "VerticalLayout", "elements": elements}


def _extract_ui_schema_elements(obj, extension_version: Version, scope=None) -> dict:
    if scope is None:
        scope = _Scope("#/properties/model/properties")
    elements = []
    for name, param_obj in _get_parameters(obj).items():
        if param_obj._since_version <= extension_version:
            elements.append(
                param_obj._extract_ui_schema(name, scope, extension_version)
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
    obj = _get_parameterized_object(obj)
    _determine_compatability(obj, saved_version, current_version, saved_parameters)


def _determine_compatability(
    obj, saved_version: Version, current_version: Version, saved_parameters: dict
) -> None:
    missing_params = _detect_missing_parameters(obj, saved_parameters, current_version)
    if saved_version < current_version:
        # backward compatibilitiy
        LOGGER.warning(
            f" The node was previously configured with an older version of the extension, {saved_version}, while the current version is {current_version}."
        )
        if missing_params:
            LOGGER.warning(
                f" The following parameters have since been added, and are configured with their default values:"
            )
            for param in missing_params:
                LOGGER.warning(f' - "{param}"')
    else:
        # forward compatibility (not supported)
        LOGGER.error(
            f" The node was previously configured with a newer version of the extension, {saved_version}, while the current version is {current_version}."
        )
        LOGGER.error(
            f" Please note that the node might not work as expected without being reconfigured."
        )


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
                group_params = (
                    saved_parameters[name] if name in saved_parameters else {}
                )
                result.extend(
                    _detect_missing_parameters(param_obj, group_params, current_version)
                )
            elif name not in saved_parameters:
                result.append(param_obj._label)

    return result


def _get_parameterized_object(obj):
    """
    obj can either be a node, or a ParameterGroupHolder/GroupView instance.
    The latter are wrappers holding an instance of the original parameter group class
    referencing the section of the root __parameters__ dict linked to this parameter group.
    """
    try:
        return obj._original_class_instance
    except AttributeError:
        return obj


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

    def _get_value(self, obj, for_dialog=None):
        # the for_dialog parameter is needed to match the signature of the
        # _get_value method for parameter groups
        return getattr(obj, self._name)

    def __get__(self, obj, objtype=None):
        if not hasattr(obj, "__kind__"):
            # obj is the root parameterised object
            _create_param_dict_if_not_exists(obj)

        if self._name in obj.__parameters__:
            return obj.__parameters__[self._name]
        else:
            def_value = self._get_default()
            obj.__parameters__[self._name] = def_value
            return def_value

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

    def _inject(
        self, obj, value, parameters_version: Version = None, fail_on_missing=True
    ):
        # the parameters_version and fail_on_missing parameters are needed to match the signature of the
        # _inject method for parameter groups
        self.__set__(obj, value)

    def __set__(self, obj, value):
        if not hasattr(obj, "__kind__"):
            # obj is the root parameterised object
            _create_param_dict_if_not_exists(obj)

        # perform individual validation
        self._validate(value)
        obj.__parameters__[self._name] = value

    def __str__(self):
        return f"\n\t - name: {self._name}\n\t - label: {self._label}\n\t"

    def _validate(self, value, version=None):
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
        self, extension_version: Version = None, specs: List[ks.Schema] = None
    ):
        return {"title": self._label, "description": self.__doc__}

    def _extract_ui_schema(
        self, name, parent_scope: _Scope, extension_version: Version = None
    ):
        # the extension_version parameter is needed to match the signature of the
        # _extract_ui_schema method for parameter groups
        options = self._get_options()
        if self._is_advanced:
            options["isAdvanced"] = True

        return {
            "type": "Control",
            "label": self._label,
            "scope": str(parent_scope.create_child(name)),
            "options": options,
        }

    @abstractmethod
    def _get_options(self) -> dict:
        pass

    def _extract_description(self, name, parent_scope: _Scope):
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

    def _extract_schema(self, extension_version=None, specs=None):
        schema = super()._extract_schema(specs)
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

    def _extract_schema(self, extension_version=None, specs=None):
        prop = super()._extract_schema(specs)
        prop["type"] = "integer"
        prop["format"] = "int32"
        return prop

    def _get_options(self) -> dict:
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
        if not isinstance(value, float):
            raise TypeError(
                f"{value} is of type {type(value)}, but should be of type float."
            )

    def _extract_schema(self, extension_version=None, specs=None):
        schema = super()._extract_schema(specs)
        schema["format"] = "double"
        return schema

    def _get_options(self) -> dict:
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

    def _get_options(self) -> dict:
        if self._enum is None or len(self._enum) > 4:
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
    ):
        if validator is None:
            validator = self.default_validator
        if enum is not None and not isinstance(enum, list):
            raise TypeError("The enum parameter must be a list.")
        self._enum = enum
        super().__init__(
            label, description, default_value, validator, since_version, is_advanced
        )

    def _extract_schema(self, extension_version=None, specs=None):
        schema = super()._extract_schema(specs)
        if self._enum is None:
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

    def __init__(self, label, dscription):
        self.label = label
        self.description = dscription

    @classmethod
    def _generate_options_description(cls):
        options_description = "\n\n**Available options:**\n\n"
        for member in cls._member_names_:
            options_description += f"- {cls[member].label}: {cls[member].description}\n"

        return options_description

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
        return self.__doc__ + self._enum._generate_options_description()

    def _extract_schema(self, extension_version=None, specs=None):
        schema = super()._extract_schema(specs)
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

    def _extract_schema(self, extension_version=None, specs: List[ks.Schema] = None):
        schema = super()._extract_schema(specs)
        values = _filter_columns(specs, self._port_index, self._column_filter)
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

    def _get_options(self) -> dict:
        return {
            "format": "columnSelection",
            "showRowKeys": self._include_row_key,
            "showNoneColumn": self._include_none_column,
        }

    def _inject(self, obj, value, version, fail_on_missing):
        value = None if value == "" else value
        return super()._inject(obj, value, version, fail_on_missing)


def _filter_columns(
    specs: List[ks.PortObjectSpec],
    port_index: int,
    column_filter: Callable[[ks.Column], bool],
):
    try:
        if specs is None or specs[port_index] is None:
            return [_const("")]

        spec = specs[port_index]
    except IndexError as ex:
        raise IndexError(
            f"The port index {port_index} is not contained in the Spec list with length {len(specs)}. "
            f"Maybe a port_index for a parameter does not match the index for an input table? "
        ) from None

    if not isinstance(spec, ks.Schema):
        raise TypeError(
            f"The port at index {port_index} is not a Table. "
            f"The ColumnParameter or MultiColumnParameter can only be used for Table ports."
        )

    filtered = [_const(column.name) for column in spec if column_filter(column)]
    if len(filtered) > 0:
        return filtered
    else:
        return [_const("")]


def _const(name):
    return {"const": name, "title": name}


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

    def _get_options(self) -> dict:
        return {"format": "columnFilter"}

    def _get_value(self, obj: Any, for_dialog: bool):
        value = super()._get_value(obj, for_dialog)
        if for_dialog and value is None:
            return []
        else:
            return value

    def _inject(self, obj, value, version, fail_on_missing):
        # if there are no columns then the empty string is used as placeholder and we need to filter it out here
        if value is not None:
            value = [c for c in value if c != ""]
        return super()._inject(obj, value, version, fail_on_missing)


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

    def _extract_schema(self, extension_version=None, specs=None):
        schema = super()._extract_schema(specs)
        schema["type"] = "boolean"
        return schema

    def _get_options(self) -> dict:
        return {"format": "boolean"}


def _flatten(lst: list) -> list:
    flat = []
    for e in lst:
        if isinstance(e, list):
            flat = flat + _flatten(e)
        else:
            flat.append(e)
    return flat


def _override_getattr(self, name):
    """
    If attribute name is not found in an instance of a class whose __getattr__ is
    overridden by this function, we check if it is available in the original
    parameter group class instance, which is expected to be available in self.
    """
    # check whether attr is available in the owner class
    if hasattr(self.__class__, name):
        return getattr(self.__class__, name)

    # check whether attr is available in the instance of the original class
    if hasattr(self._original_class_instance, name):
        attr = getattr(self._original_class_instance, name)
        # bind class methods to the original class instance
        if hasattr(attr, "__get__"):
            attr.__get__(self, name)

        return attr

    raise AttributeError(
        f"'{self.__class__.__name__}' object has no attribute '{name}'"
    )


def _override_setattr(self, name, value):
    """
    In the case of nested composed parameters, the __set__ method does not get called
    automatically. We override __setattr__ in order to intercept assignment operations,
    e.g. node.group.subgroup.param = 42, in which case the .__set__ method of the
    non-descriptor param object will manually be called, with its group being the owner object.
    """
    if "_original_class_instance" not in self.__class__.__dict__:
        super(self.__class__, self).__setattr__(name, value)

    if name in self._original_class_instance.__class__.__dict__:
        attr = self._original_class_instance.__class__.__dict__[name]
        if hasattr(attr, "__set__"):
            attr.__set__(self, value)


def parameter_group(
    label: str,
    since_version: Optional[Union[Version, str]] = None,
    is_advanced: bool = False,
):
    """
    Decorator for classes implementing parameter groups. Parameter group classes can define parameters
    and other parameter groups both as class-level attributes and as instance-level attributed inside the `__init__` method.

    Parameter groups classes can set values to their parameters inside the `__init__` method during the constructor call (e.g.
    from the node containing the group, or another group).

    Group validators need to raise an exception if a values-based condition is violated, where values is a dictionary
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
        class ParameterGroupHolder:
            """
            This class implements the methods required for parameter groups, while also retaining an
            instance of the original class for access to its methods (including __init__).
            """

            __kind__ = "parameter_group"

            def __init__(
                self, validator=None, is_advanced=is_advanced, *args, **kwargs
            ):
                # The since_version for a parameter group can be provided either through the
                # @parameter_group decorator, or directly through the constructor of its class.
                # The latter takes precedence over the former, if provided.
                if "since_version" in kwargs:
                    provided_since_version = kwargs["since_version"]
                    del kwargs["since_version"]
                else:
                    provided_since_version = since_version

                self._original_class_instance = original_class(*args, **kwargs)
                self._label = label

                # if parameter values of the parameter group were set during its constructor,
                # the original class instance will have its own __parameters__ dict, which we
                # then need to reference
                self.__parameters__ = (
                    {}
                    if not hasattr(self._original_class_instance, "__parameters__")
                    else self._original_class_instance.__parameters__
                )
                self._since_version = Version.parse_version(provided_since_version)
                self._is_advanced = is_advanced
                self._validator = validator
                self._override_internal_validator = False

                # preserve @classmethods and parameters of the original class
                self._transfer_original_cls_methods(self.__class__)
                self._transfer_parameters(self.__class__)

            def _transfer_original_cls_methods(self, target_class):
                """
                Preserve class-level methods of the original class directly, since such method calls
                seem to forgo __getattribute__ and thus never end up in the overridden __getattr__ method,
                e.g. `NestedComposed.create_default_dict()`, where `create_default_dict()` is a @classmethod.
                """
                for name, method in _get_custom_class_methods(
                    self._original_class_instance.__class__
                ).items():
                    setattr(target_class, name, method)

            def _transfer_parameters(self, target_class):
                for name, parameter in _get_parameters(
                    self._original_class_instance
                ).items():
                    setattr(target_class, name, parameter)

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
                Descriptors: generate a new GroupView class every time, and return an instance of the class
                which references a value/subdict of the __parameters__ dict of obj that belongs
                to this group, as well as an instance of the original wrapped class for custom
                method access.

                Composed: return this instance.
                """
                return self._get_param_holder(obj)

            def _get_value(self, obj, for_dialog: bool = False):
                param_holder = self._get_param_holder(obj)

                return {
                    name: param_obj._get_value(param_holder, for_dialog)
                    for name, param_obj in _get_parameters(param_holder).items()
                }

            def _get_param_holder(self, obj):
                """
                When parameters are descriptor based, obj will always be either the root parameteretized object
                (node), a ParameterGroupHolder, or a GroupView referencing a subdict of the root.__parameters__ dict.

                When parameters are composed, the instance self.__parameters__ dict is used instead, so the holder is self.
                """
                return self._create_group_view(obj) if self._is_descriptor() else self

            def _create_group_view(self, obj):
                """
                A factory that generates GroupView classes containing class and instance-level
                attributes of the original parameter group class
                """
                if not hasattr(obj, "__kind__"):
                    # obj is the root parameterised object
                    _create_param_dict_if_not_exists(obj)

                if self._name not in obj.__parameters__:
                    # values of parameters of the group were initialised during the constructor
                    if hasattr(self._original_class_instance, "__parameters__"):
                        obj.__parameters__[self._name] = self.__parameters__
                    else:
                        obj.__parameters__[self._name] = {}

                class GroupView:
                    def __init__(self, group_params_dict, original_class_instance):
                        self.__parameters__ = group_params_dict
                        self._original_class_instance = original_class_instance

                self._transfer_original_cls_methods(GroupView)
                self._transfer_parameters(GroupView)

                # ensure access to methods and attributes of the wrapped parameter group class
                GroupView.__getattr__ = _override_getattr
                GroupView.__kind__ = "parameter_group"

                return GroupView(
                    obj.__parameters__[self._name], self._original_class_instance
                )

            def __set__(self, obj, values):
                raise RuntimeError("Cannot set parameter group values directly.")

            def _set_default_for_version(self, obj, version: Version):
                param_holder = self._get_param_holder(obj)
                # Assumes that if a parameter group is new, so are its elements
                for param_obj in _get_parameters(param_holder).values():
                    param_obj._set_default_for_version(param_holder, version)

            def _inject(
                self, obj, values, parameters_version: Version, fail_on_missing: bool
            ):
                param_holder = self._get_param_holder(obj)
                _inject_parameters(
                    param_holder, values, parameters_version, fail_on_missing
                )

            def __str__(self):
                return f"\tGroup name: {self._name}\n\tGroup label: {self._label}"

            def _validate(self, values, version: Version):
                # validate individual parameters
                _validate_parameters(self._original_class_instance, values, version)

                # use the "internal" group validator if exists
                if (
                    hasattr(self._original_class_instance, "validate")
                    and not self._override_internal_validator
                ):
                    self._original_class_instance.validate(values)

                # use the decorator-defined validator if exists
                if self._validator is not None:
                    self._validator(values)

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
                self, name, parent_scope: _Scope, version: Version = None
            ):
                scope = parent_scope.create_child(name, is_group=True)
                elements = _extract_ui_schema_elements(
                    self._original_class_instance, version, scope
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
                obj = _get_parameterized_object(self)
                options = _extract_parameter_descriptions(
                    obj,
                    scope,
                )

                if scope.level() == 1:
                    # this is a tab
                    return {
                        "name": self._label,
                        "description": self._original_class_instance.__doc__,
                        "options": _flatten(options),
                    }
                else:
                    return options

            def _extract_schema(self, extension_version: Version, specs):
                properties = {}
                for name, param_obj in _get_parameters(
                    self._original_class_instance
                ).items():
                    if param_obj._since_version <= extension_version:
                        properties[name] = param_obj._extract_schema(
                            extension_version=extension_version, specs=specs
                        )

                return {"type": "object", "properties": properties}

        ParameterGroupHolder._validate_specs = validate_specs
        ParameterGroupHolder.__getattr__ = _override_getattr
        ParameterGroupHolder.__setattr__ = _override_setattr

        return ParameterGroupHolder

    return decorate_class
