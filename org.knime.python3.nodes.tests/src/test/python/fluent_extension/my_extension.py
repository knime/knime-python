import knime_node as kn


@kn.node(name="My Node", node_type="Learner", icon_path="icon.png", category="/")
class MyNode(kn.PythonNode):
    def __init__(self) -> None:
        super().__init__()
        self._some_param = 42

    @kn.Parameter
    def some_param(self):
        return self._some_param

    @some_param.setter
    def some_param(self, value):
        self._some_param = value
