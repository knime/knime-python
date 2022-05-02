from typing import List, Tuple
import knime_node as kn
import knime_table as kt
import knime_schema as ks


@kn.node(name="My Node", node_type="Learner", icon_path="icon.png", category="/")
class MyNode(kn.PythonNode):
    """My first node

    This node has a description
    
    """
    def __init__(self) -> None:
        super().__init__()
        self._some_param = 42

    @kn.Parameter
    def some_param(self):
        """The answer to everything"""
        return self._some_param

    @some_param.setter
    def some_param(self, value):
        self._some_param = value

    def configure(self, input_schemas: List[ks.Schema]) -> List[ks.Schema]:
        return input_schemas

    def execute(self, tables: List[kt.ReadTable], objects: List, exec_context) -> Tuple[List[kt.WriteTable], List]:
        return [kt.write_table(table) for table in tables], []
