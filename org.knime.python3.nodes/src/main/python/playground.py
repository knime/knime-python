import knime_node as kn
from packaging.version import Version


class MyDecoratedNode(kn.PythonNode):
    def __init__(self) -> None:
        super().__init__()
        self._param1 = 42
        self._param2 = "awesome"
        self._backwards_compat_param = "Some parameter introduced in a later version"

    @kn.parameter
    def param2(self):
        return self._param2

    @param2.setter
    def param2(self, value):
        self._param2 = value

    @kn.rule(effect="ENABLE", scope=param2, schema={"enum": ["foo"]}) # not evaluated yet
    @kn.ui(label="My first parameter") # not evaluated yet
    @kn.parameter
    def param1(self):
        return self._param1

    @param1.setter
    def param1(self, value):
        if value < 0:
            raise ValueError("The value must be non-negative.")
        self._param1 = value

    @kn.parameter(since_version=Version("4.6.0"))
    def backwards_compatible_paramet(self):
        return self._backwards_compat_param

    @backwards_compatible_paramet.setter
    def backwards_compatible_parameter(self, value):
        self._backwards_compat_param = value

