from typing import List, Tuple
import knime_node as kn
import knime_schema as ks
import knime_table as kt
from packaging.version import Version
import logging

logger = logging.getLogger(__name__)


class MyDecoratedNode(kn.PythonNode):
    def __init__(self) -> None:
        super().__init__()
        self._num_columns: int = 5
        self._param2 = "awesome"
        self._backwards_compat_param = "Some parameter introduced in a later version"

    @kn.parameter
    def num_columns(self):
        return self._num_columns

    @num_columns.setter
    def num_columns(self, value):
        if value < 0:
            raise ValueError("The value must be non-negative.")
        self._num_columns = value

    @kn.parameter
    def param2(self):
        return self._param2

    @param2.setter
    def param2(self, value):
        self._param2 = value

    @kn.rule(
        effect="ENABLE", scope=param2, schema={"enum": ["foo"]}
    )  # not evaluated yet
    @kn.ui(label="My first parameter")  # not evaluated yet
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

    def configure(self, input_schemas: List[ks.Schema]) -> List[ks.Schema]:
        logger.warning(
            f"Configuring with values {self._param2} and {self._num_columns}"
        )
        table_schema = input_schemas[0]
        num_columns = self._num_columns + 1  # +1 for the RowKey
        output_table_schema = table_schema[0:num_columns]

        return [output_table_schema]

    def execute(
        self, tables: List[kt.ReadTable], objects: list, exec_context
    ) -> Tuple[List[kt.WriteTable], list]:
        logger.warning(f"Executing with values {self._param2} and {self._num_columns}")
        num_columns = self._num_columns + 1  # +1 for the RowKey
        t = kt.write_table(tables[0][0:num_columns].to_pyarrow())

        import time

        for i in range(4):
            time.sleep(1)
            exec_context.set_progress(0.25 * i)

        return ([t], objects)

