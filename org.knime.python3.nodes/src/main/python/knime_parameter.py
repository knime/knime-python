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
from typing import Any, Callable, Dict, List
import knime_schema as ks


from utils import parse_version, Version


def _get_parameters(obj) -> Dict[str, "_BaseParameter"]:
    """
    Get all parameter objects from obj as a nested dict.
    """
    class_params = {
        name: param
        for name, param in type(obj).__dict__.items()
        if _is_parameter_or_group(param)
    }
    instance_params = {
        name: param
        for name, param in obj.__dict__.items()
        if _is_parameter_or_group(param)
    }

    return {**class_params, **instance_params}


def extract_parameters(obj, extension_version=None, for_dialog=False) -> dict:
    """
    Get all parameter values from obj as a nested dict.
    """
    extension_version = parse_version(extension_version)
    return {"model": _extract_parameters(obj, extension_version, for_dialog)}


def _extract_parameters(obj, extension_version: Version, for_dialog: bool) -> dict:
    result = dict()
    params = _get_parameters(obj)
    for name, param_obj in params.items():
        if param_obj._since_version <= extension_version:
            if _is_group(param_obj):
                result[name] = _extract_parameters(
                    param_obj, extension_version, for_dialog
                )
            else:
                result[name] = param_obj._get_value(obj)

    return result


def _is_group(param):
    return hasattr(param, "__kind__") and param.__kind__ == "parameter_group"


def inject_parameters(
    obj, parameters: dict, extension_version=None, fail_on_missing: bool = True
) -> None:
    extension_version = parse_version(extension_version)
    _inject_parameters(obj, parameters["model"], extension_version, fail_on_missing)


def _inject_parameters(
    obj, parameters: dict, extension_version: Version, fail_on_missing: bool
) -> None:
    for name, param_obj in _get_parameters(obj).items():
        if _is_group(param_obj):
            if name in parameters:
                group_params = parameters[name]
            else:
                group_params = _get_parameters(param_obj).items()
            _inject_parameters(
                param_obj, group_params, extension_version, fail_on_missing
            )
        else:
            if name in parameters:
                param_value = parameters[name]
            elif param_obj._since_version <= extension_version:
                if fail_on_missing:
                    raise ValueError(f"No value available for parameter '{name}'")
                param_value = param_obj._default_value
            param_obj._inject(obj, param_value)


def validate_parameters(obj, parameters: dict) -> str:
    """
    Perform validation on the individual parameters of obj.
    """
    return _validate_parameters(obj, parameters["model"])


def _validate_parameters(obj, parameters: dict) -> str:
    for name, param in _get_parameters(obj).items():
        # TODO test if that also works for parameter groups
        if name in parameters:
            param._validate(parameters[name])


def validate_specs(obj, specs) -> None:
    """
    Checks if the provided specs are compatible with the parameter of obj.
    """
    for param in _get_parameters(obj).values():
        param._validate_specs(specs)


def extract_schema(obj, extension_version=None, specs=None) -> dict:
    extension_version = parse_version(extension_version)
    return {
        "type": "object",
        "properties": {"model": _extract_schema(obj, extension_version, specs)},
    }


def _extract_schema(obj, extension_version, specs=None):
    properties = {}
    for name, param in _get_parameters(obj).items():
        if _is_group(param):
            properties[name] = _extract_schema(param, extension_version, specs)
        else:
            if param._since_version <= extension_version:
                properties[name] = param._extract_schema(specs)

    return {"type": "object", "properties": properties}


def extract_ui_schema(obj, extension_version=None) -> dict:
    extension_version = parse_version(extension_version)
    elements = _extract_ui_schema_elements(obj, extension_version)

    return {"type": "VerticalLayout", "elements": elements}


def _extract_ui_schema_elements(obj, extension_version, scope=None) -> dict:
    # TODO discuss with UIEXT if we can unpack the dicts
    if scope is None:
        scope = _Scope("#/properties/model/properties")
    elements = []
    for name, param_obj in _get_parameters(obj).items():
        if param_obj._since_version <= extension_version:
            elements.append(param_obj._extract_ui_schema(name, scope))

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
    params = _get_parameters(obj).values()
    return [param._extract_description(scope) for param in params]


def _is_parameter_or_group(obj) -> bool:
    return hasattr(obj, "__kind__")


def _create_param_dict_if_not_exists(obj):
    """
    Create a parameter dictionary if it doesn't exist and populate it with the current parameter object.
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


class _BaseParameter(ABC):
    """
    Base class for parameter descriptors.
    "obj" in this context is a parameterised object.
    """

    __kind__ = "parameter"

    def __init__(
        self, label, description, default_value, validator=None, since_version=None
    ):
        self._label = label
        self._default_value = default_value
        self._validator = validator if validator is not None else _default_validator
        self.__doc__ = description if description is not None else ""
        self._since_version = parse_version(since_version)

    def __set_name__(self, owner, name):
        self._name = name
        if self._label is None:
            self._label = name

    def _get_value(self, obj: Any):
        return getattr(obj, self._name)

    def __get__(self, obj, objtype=None):
        if not hasattr(obj, "__kind__"):
            # obj is the root parameterised object
            _create_param_dict_if_not_exists(obj)

        if self._name in obj.__parameters__:
            return obj.__parameters__[self._name]
        else:
            obj.__parameters__[self._name] = self._default_value
            return self._default_value

    def _inject(self, obj, value):
        # TODO only set if the parameter was available in the version
        self.__set__(obj, value)

    def __set__(self, obj, value):
        # TODO: version support
        if not hasattr(obj, "__kind__"):
            # obj is the root parameterised object
            _create_param_dict_if_not_exists(obj)

        # perform individual validation
        self._validate(value)
        obj.__parameters__[self._name] = value

    def __str__(self):
        # return f"\n\t - name: {self._name}\n\t - label: {self._label}\n\t - value: {self._value}"
        return f"\n\t - name: {self._name}\n\t - label: {self._label}\n\t"

    # TODO does version make sense here?
    def _validate(self, value):
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

    def _extract_schema(self, specs: List[ks.Schema] = None):
        return {"title": self._label, "description": self.__doc__}

    def _extract_ui_schema(self, name, parent_scope: _Scope, extension_version=None):
        # note the extension_version parameter - not used but needed for conciseness;
        # see the general _extract_ui_schema method
        return {
            "type": "Control",
            "label": self._label,
            "scope": str(parent_scope.create_child(name)),
            "options": self._get_options(),
        }

    @abstractmethod
    def _get_options(self) -> dict:
        pass

    def _extract_description(self, parent_scope: _Scope):
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
    ):
        self.min_value = min_value
        self.max_value = max_value
        if validator is None:
            validator = self.default_validator
        super().__init__(label, description, default_value, validator, since_version)

    def check_range(self, value):
        if self.min_value is not None and value < self.min_value:
            raise ValueError(
                f"{value} is smaller than the minimal value {self.min_value}"
            )
        if self.max_value is not None and value > self.max_value:
            raise ValueError(f"{value} is > the max value {self.max_value}")

    def _extract_schema(self, specs):
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
        label=None,
        description=None,
        default_value=0,
        validator=None,
        min_value=None,
        max_value=None,
        since_version=None,
    ):
        super().__init__(
            label,
            description,
            default_value,
            validator,
            min_value,
            max_value,
            since_version,
        )

    def check_type(self, value):
        if not isinstance(value, int):
            # TODO Type or ValueError?
            raise TypeError(
                f"{value} is of type {type(value)}, but should be of type int."
            )

    def _extract_schema(self, specs):
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
        label=None,
        description=None,
        default_value=0.0,
        validator=None,
        min_value=None,
        max_value=None,
        since_version=None,
    ):
        super().__init__(
            label,
            description,
            default_value,
            validator,
            min_value,
            max_value,
            since_version,
        )

    def check_type(self, value):
        if not isinstance(value, float):
            raise TypeError(
                f"{value} is of type {type(value)}, but should be of type float."
            )

    def _extract_schema(self, specs):
        schema = super()._extract_schema(specs)
        schema["format"] = "double"
        return schema

    def _get_options(self) -> dict:
        return {"format": "number"}


class StringParameter(_BaseParameter):
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
        label=None,
        description=None,
        default_value: str = "",
        enum: List[str] = None,
        validator=None,
        since_version=None,
    ):
        if validator is None:
            validator = self.default_validator
        if enum is not None and not isinstance(enum, list):
            raise TypeError("The enum parameter must be a list.")
        self._enum = enum
        super().__init__(label, description, default_value, validator, since_version)

    def _extract_schema(self, specs):
        schema = super()._extract_schema(specs)
        if self._enum is None:
            schema["type"] = "string"
        else:
            schema["oneOf"] = [{"const": e, "title": e} for e in self._enum]
        return schema

    def _get_options(self) -> dict:
        if self._enum is None or len(self._enum) > 4:
            return {"format": "string"}
        else:
            return {"format": "radio"}


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
    ):
        super().__init__(
            label, description, default_value=None, since_version=since_version
        )
        self._port_index = port_index
        if column_filter is None:
            column_filter = lambda c: True
        self._column_filter = column_filter
        self._schema_option = schema_option

    def _extract_schema(self, specs: List[ks.Schema] = None):
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
        label=None,
        description=None,
        port_index=0,
        column_filter: Callable[[ks.Column], bool] = None,
        include_row_key=False,
        include_none_column=False,
        since_version=None,
    ):
        super().__init__(
            label, description, port_index, column_filter, "oneOf", since_version
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
        label=None,
        description=None,
        port_index=0,
        column_filter: Callable[[ks.Column], bool] = None,
        since_version=None,
    ):
        super().__init__(
            label, description, port_index, column_filter, "anyOf", since_version
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
        label=None,
        description=None,
        default_value=False,
        validator=None,
        since_version=None,
    ):
        if validator is None:
            validator = self.default_validator
        super().__init__(label, description, default_value, validator, since_version)

    def _extract_schema(self, specs):
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


def parameter_group(label: str):
    """
    Used for injecting descriptor protocol methods into a custom parameter group class.
    "obj" in this context is the parameterized object instance or a parameter group instance.

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

    def decorate_class(custom_cls):
        orig_init = custom_cls.__init__

        def __init__(
            self, label=label, since_version=None, validator=None, *args, **kwargs
        ):
            self._label = label
            self._since_version = parse_version(since_version)
            self._validator = validator
            self.__parameters__ = {}
            self._override_internal_validator = False

            orig_init(self, *args, **kwargs)

        def __set_name__(self, owner, name):
            self._name = name

        def _is_descriptor(parameter_group):
            """
            A parameter_group used as descriptor i.e. declared on class level needs to be
            handled differently than a parameter_group that is used via composition i.e. passed via __init__.
            Here we use the _name attribute set via __set_name__ to distinguish the two since __set_name__ is only
            called if the parameter_group is used as descriptor, namely when it is declared in the class definition.
            """
            return hasattr(parameter_group, "_name")

        def __get__(self, obj, obj_type=None):
            """
            Generate a new GroupView class every time, and return an instance of the class
            which references a value/subdict of the __parameters__ dict of obj that belongs
            to this group.
            """
            return _create_group_view(self, obj)

        def _get_value(self, obj, for_dialog=False):
            param_holder = _get_param_holder(self, obj)
            return {
                name: param._get_value(param_holder, for_dialog)
                for name, param in _get_parameters(param_holder).items()
            }

        def _get_param_holder(parameter_group, obj):
            return (
                _create_group_view(parameter_group, obj)
                if _is_descriptor(parameter_group)
                else parameter_group
            )

        def _create_group_view(self, obj):
            if not hasattr(obj, "__kind__"):
                # obj is the root parameterised object
                _create_param_dict_if_not_exists(obj)

            if self._name not in obj.__parameters__:
                obj.__parameters__[self._name] = {}

            class GroupView:
                def __init__(self, group_params_dict):
                    self.__parameters__ = group_params_dict

            for name, parameter in _get_parameters(self).items():
                setattr(GroupView, name, parameter)

            return GroupView(obj.__parameters__[self._name])

        def __set__(self, obj, values):
            raise RuntimeError("Cannot set parameter group values directly.")

        def _inject(self, obj, values, version=None, fail_on_missing=True):
            param_holder = _get_param_holder(self, obj)
            _inject_parameters(param_holder, values, version, fail_on_missing)

        def __str__(self):
            return f"\tGroup name: {self._name}\n\tGroup label: {self._label}"

        def _validate(self, values):
            # validate individual parameters
            _validate_parameters(self, values)

            # use the "internal" group validator if exists
            if (
                hasattr(custom_cls, "validate")
                and not self._override_internal_validator
            ):
                self.validate(values)

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

        def _extract_ui_schema(self, name, parent_scope: _Scope, version=None):
            scope = parent_scope.create_child(name, is_group=True)
            version = parse_version(version)
            elements = _extract_ui_schema_elements(self, version, scope)
            # TODO how do we get the name in case of composition? Pass name as parameter!
            # TODO treat first level groups as sections and deeper nested groups as groups
            # TODO can we have nested sections?
            if scope.level() == 1:
                group_type = "Section"
            else:
                group_type = "Group"
            return {"type": group_type, "label": self._label, "elements": elements}

        def _extract_description(self, parent_scope: _Scope):
            scope = parent_scope.create_child(self._name, is_group=True)
            options = _extract_parameter_descriptions(self, scope)
            if scope.level() == 1:
                # this is a tab
                return {
                    "name": self._label,
                    "description": self.__doc__,
                    "options": _flatten(options),
                }
            else:
                return options

        custom_cls.__init__ = __init__
        custom_cls.__set_name__ = __set_name__
        custom_cls.__get__ = __get__
        custom_cls.__set__ = __set__
        custom_cls.__str__ = __str__
        custom_cls._get_value = _get_value
        custom_cls._inject = _inject
        custom_cls._validate = _validate
        custom_cls._validate_specs = validate_specs
        custom_cls._extract_schema = _extract_schema
        custom_cls._extract_ui_schema = _extract_ui_schema
        custom_cls._extract_description = _extract_description
        custom_cls.validator = validator
        custom_cls.__kind__ = "parameter_group"

        return custom_cls

    return decorate_class
