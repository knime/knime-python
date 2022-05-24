"""
Contains the implementation of the Parameter Dialogue API for building native Python nodes in KNIME.

@author Ivan Prigarin, KNIME GmbH, Konstanz, Germany
@author Adrian Nembach, KNIME GmbH, Konstanz, Germany
"""
from abc import ABC, abstractmethod
from typing import Callable, Dict, List
import knime_schema as ks


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


def extract_parameters(obj, for_dialog=False) -> dict:
    """
    Get all parameter values from obj as a nested dict.
    """
    result = dict()
    params = _get_parameters(obj)
    for name, param_obj in params.items():
        if param_obj.__kind__ == "parameter":
            result[name] = getattr(obj, name)
        elif param_obj.__kind__ == "parameter_group":
            # TODO doesn't the getter handle this?
            result[name] = extract_parameters(param_obj)

    if for_dialog:
        return {"model": result}
    else:
        return result


# TODO version support
def inject_parameters(obj, parameters: dict, version) -> None:
    validate_parameters(obj, parameters, version)
    for name, parameter in _get_parameters(obj).items():
        # TODO can only set if the parameter was already available in version
        parameter._inject(obj, parameters[name], version)


# TODO version support
def validate_parameters(obj, parameters: dict, version=None) -> str:
    """
    Perform validation on the individual parameters of obj.
    """
    for name, param in _get_parameters(obj).items():
        # TODO test if that also works for parameter groups
        param._validate(parameters[name], version)


def extract_schema(obj, specs=None, for_dialog=False) -> dict:
    properties = {
        name: param._extract_schema(specs)
        for name, param in _get_parameters(obj).items()
    }
    schema = {"type": "object", "properties": properties}
    if for_dialog:
        return {"type": "object", "properties": {"model": schema}}
    else:
        return schema


def extract_ui_schema(obj) -> dict:
    # TODO discuss with UIEXT if we can unpack the dicts
    elements = [
        # name is provided for parameter_groups that are held as instance variables
        param._extract_ui_schema(name, _Scope("#/properties/model/properties"))
        for name, param in _get_parameters(obj).items()
    ]
    return {"type": "VerticalLayout", "elements": elements}


def extract_parameter_descriptions(obj) -> dict:
    descriptions = [
        param._extract_description(_Scope("#/properties"))
        for param in _get_parameters(obj).values()
    ]
    flattened = []
    for description in descriptions:
        if isinstance(description, list):
            flattened = flattened + description
        else:
            flattened.append(description)
    return _extract_parameter_descriptions(obj, _Scope("#/properties"))


def _extract_parameter_descriptions(obj, scope: "_Scope"):
    descriptions = [
        param._extract_description(scope) for param in _get_parameters(obj).values()
    ]
    flattened = []
    for description in descriptions:
        if isinstance(description, list):
            flattened = flattened + description
        else:
            flattened.append(description)
    return flattened


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
        self,
        label,
        description,
        default_value,
        validator=None,
    ):
        self._label = label
        self._default_value = default_value
        self._validator = validator if validator is not None else _default_validator
        self.__doc__ = description if description is not None else ""

    def __set_name__(self, owner, name):
        self._name = name
        if self._label is None:
            self._label = name

    def __get__(self, obj, objtype=None):
        if not hasattr(obj, "__kind__"):
            # obj is the root parameterised object
            _create_param_dict_if_not_exists(obj)

        if self._name in obj.__parameters__:
            return obj.__parameters__[self._name]
        else:
            obj.__parameters__[self._name] = self._default_value
            return self._default_value

    def _inject(self, obj, value, version):
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
        return f"\n\t - name: {self._name}\n\t - label: {self._label}\n\t - value: {self._value}"

    # TODO does version make sense here?
    def _validate(self, value, version=None):
        self._validator(value)

    def validator(self, func):
        """
        To be used as a decorator for setting a validator function for a parameter.
        Note that 'func' will be encapsulated in '_validator' and will not be available in the namespace of the class.
        """
        self._validator = func

    def _extract_schema(self, specs: List[ks.Schema] = None):
        return {"title": self._label, "description": self.__doc__}

    def _extract_ui_schema(self, name, parent_scope: _Scope):
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
    ):
        self.min_value = min_value
        self.max_value = max_value
        if validator is None:
            validator = self.default_validator
        super().__init__(label, description, default_value, validator)

    def check_range(self, value):
        if self.min_value is not None and value < self.min_value:
            raise ValueError(
                f"{value} is smaller than the minimal value {self.min_value}"
            )
        if self.max_value is not None and value >= self.max_value:
            raise ValueError(f"{value} is >= the max value {self.max_value}")

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
    ):
        super().__init__(
            label, description, default_value, validator, min_value, max_value
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
    ):
        super().__init__(
            label, description, default_value, validator, min_value, max_value
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
    ):
        if validator is None:
            validator = self.default_validator
        if enum is not None and not isinstance(enum, list):
            raise TypeError("The enum parameter must be a list.")
        self._enum = enum
        super().__init__(label, description, default_value, validator)

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


class ColumnParameter(_BaseParameter):
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
    ):
        super().__init__(label, description, default_value=None)
        self._port_index = port_index
        self._include_row_key = include_row_key
        self._include_none_column = include_none_column
        if column_filter is None:
            column_filter = lambda c: True
        self._column_filter = column_filter

    def _extract_schema(self, specs: List[ks.Schema] = None):
        schema = super()._extract_schema(specs)
        values = _filter_columns(specs, self._port_index, self._column_filter)
        schema["oneOf"] = values
        return schema

    def _get_options(self) -> dict:
        return {
            "format": "columnSelection",
            "showRowKeys": self._include_row_key,
            "showNoneColumn": self._include_none_column,
        }


def _filter_columns(
    specs: List[ks.Schema], port_index: int, column_filter: Callable[[ks.Column], bool]
):
    if specs is None:
        return list()
    else:
        spec = specs[port_index]
        return [
            {"const": column.name, "title": column.name}
            for column in spec
            if column_filter(column)
        ]


class MultiColumnParameter(_BaseParameter):
    """
    Parameter class for multiple columns.
    """

    def __init__(
        self,
        label=None,
        description=None,
        port_index=0,
        column_filter: Callable[[ks.Column], bool] = None,
    ):
        super().__init__(label, description, default_value=None)
        self._port_index = port_index
        if column_filter is None:
            column_filter = lambda c: True
        self._column_filter = column_filter

    def _extract_schema(self, specs: List[ks.Schema] = None):
        schema = super()._extract_schema(specs)
        values = _filter_columns(specs, self._port_index, self._column_filter)
        schema["anyOf"] = values
        return schema

    def _get_options(self) -> dict:
        return {"format": "columnFilter"}


class BoolParameter(_BaseParameter):
    """
    Parameter class for primitive boolean types.
    """

    def default_validator(self, value):
        if not isinstance(value, bool):
            raise TypeError(f"{value} is not a boolean")

    def __init__(
        self, label=None, description=None, default_value=False, validator=None
    ):
        if validator is None:
            validator = self.default_validator
        super().__init__(label, description, default_value, validator)

    def _extract_schema(self, specs):
        schema = super()._extract_schema(specs)
        schema["type"] = "boolean"
        return schema

    def _get_options(self) -> dict:
        return {"format": "boolean"}


def parameter_group(label: str):
    """
    Used for injecting descriptor protocol methods into a custom parameter group class.
    "obj" in this context is the parameterized object instance or a parameter group instance.

    Group validators need to raise an exception if a values-based condition is violated, where values is a dictionary
    of parameter names and values.
    Group validators can be set using either of the following methods:
    - By implementing the "validate(self, values)" method inside the class definition of the group. For example:

        def validate(self, values):
            assert values['first_param'] + values['second_param'] < 100

    - By using the "@group_name.validator" decorator notation inside the class definition of the "parent" of the group.
      The decorator has an optional 'override' parameter, set to True by default, which overrides the "validate" method.
      If 'override' is set to False, the "validate" method, if defined, will be called first. For example:

        @hyperparameters.validator(override=False)
        def validate_hyperparams(values):
            assert values['first_param'] + values['second_param'] < 100
    """

    def decorate_class(custom_cls):
        orig_init = custom_cls.__init__

        def __init__(self, label=label, validator=None, *args, **kwargs):
            self._label = label
            self._validator = validator
            self.__parameters__ = {}
            self._override_internal_validator = False

            orig_init(self, *args, **kwargs)

        def __set_name__(self, owner, name):
            self._name = name

        def __get__(self, obj, obj_type=None):
            """
            Generate a new GroupView class every time, and return an instance of the class
            which references a value/subdict of the __parameters__ dict of obj that belongs
            to this group.
            """
            return _create_group_view(self, obj)

        def _create_group_view(self, obj):
            if not hasattr(obj, "__kind__"):
                # obj is the root parameterised object
                _create_param_dict_if_not_exists(obj)

            if self._name not in obj.__parameters__:
                obj.__parameters__[self._name] = {}

            # store a "view" of the subdict associated with this parameter group
            self.__parameters__ = obj.__parameters__[self._name]

            class GroupView:
                def __init__(self, group_params_dict):
                    self.__parameters__ = group_params_dict

            for name, parameter in _get_parameters(self).items():
                setattr(GroupView, name, parameter)

            return GroupView(obj.__parameters__[self._name])

        def __set__(self, obj, values):
            raise RuntimeError("Cannot set parameter group values directly.")

        def _inject(self, obj, values, version):
            group_view = _create_group_view(self, obj)
            for name, parameter in _get_parameters(group_view).items():
                # TODO versioning
                parameter._inject(group_view, values[name], version)

        def __str__(self):
            return f"\tGroup name: {self._name}\n\tGroup label: {self._label}"

        def _validate(self, values, version=None):
            # validate individual parameters
            validate_parameters(self, values, version)

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

        def _extract_schema(self, specs=None):
            return extract_schema(self, specs)

        def _extract_ui_schema(self, name, parent_scope: _Scope):
            scope = parent_scope.create_child(name, is_group=True)
            # TODO how do we get the name in case of composition? Pass name as parameter!
            elements = [
                param._extract_ui_schema(name, scope)
                for name, param in _get_parameters(self).items()
            ]
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
                    "options": options,
                }
            else:
                # flatten group
                return options

        custom_cls.__init__ = __init__
        custom_cls.__set_name__ = __set_name__
        custom_cls.__get__ = __get__
        custom_cls.__set__ = __set__
        custom_cls.__str__ = __str__
        custom_cls._inject = _inject
        custom_cls._validate = _validate
        custom_cls._extract_schema = _extract_schema
        custom_cls._extract_ui_schema = _extract_ui_schema
        custom_cls._extract_description = _extract_description
        custom_cls.validator = validator
        custom_cls.__kind__ = "parameter_group"

        return custom_cls

    return decorate_class
