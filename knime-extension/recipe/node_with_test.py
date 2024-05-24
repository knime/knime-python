import knime.extension as knext
import knime.extension.testing as ktest
import pandas as pd

import unittest


@knext.node(
    "Test Node", knext.NodeType.MANIPULATOR, icon_path="./icon.png", category="/"
)
@knext.input_table("Table", "Input Table")
@knext.output_table("Table", "Input Table")
class TestNode(knext.PythonNode):
    column = knext.ColumnParameter("Column to duplicate", "Column to duplicate")

    def configure(self, config_context: knext.ConfigurationContext, input_schema):
        import knime.types.chemistry as cet

        if self.column is None:
            raise knext.InvalidParametersError("Must select a column")

        selected_col = input_schema[self.column]

        return input_schema.append(
            knext.Schema.from_columns(
                [
                    knext.Column(selected_col.ktype, selected_col.name + "_copy"),
                    knext.Column(knext.logical(cet.SmilesValue), "Smiles"),
                ]
            )
        )

    def execute(self, exec_context: knext.ExecutionContext, input_table):
        import knime.types.chemistry as cet

        df = input_table.to_pandas()
        df[self.column + "_copy"] = df[self.column]
        df["Smiles"] = [cet.SmilesValue("CCC")] * len(df)
        return knext.Table.from_pandas(df)


@knext.node(
    "Batch Test Node", knext.NodeType.MANIPULATOR, icon_path="./icon.png", category="/"
)
@knext.input_table("Table", "Input Table")
@knext.output_table("Table", "Input Table")
class BatchTestNode(knext.PythonNode):
    column = knext.ColumnParameter("Column to duplicate", "Column to duplicate")

    def configure(self, config_context: knext.ConfigurationContext, input_schema):
        if self.column is None:
            raise knext.InvalidParametersError("Must select a column")

        selected_col = input_schema[self.column]

        return input_schema.append(
            knext.Schema.from_columns(
                [
                    knext.Column(selected_col.ktype, selected_col.name + "_copy"),
                ]
            )
        )

    def execute(self, exec_context: knext.ExecutionContext, input_table):
        output_table: knext.BatchOutputTable = knext.BatchOutputTable.create()

        for batch in input_table.batches():
            df = batch.to_pandas()
            df[self.column + "_copy"] = df[self.column]
            output_table.append(df)

        return output_table


class TestCase(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()

        import os

        # TODO: below would be the nice way of always getting the up-to-date files. However,
        #       as knime-chemistry is not public (yet), we cannot download the files easily.
        #       So, for the time being, they are included in the test files anyways.

        # import requests

        # def download(url, file):
        #     r = requests.get(url, allow_redirects=True)
        #     with open(file, "wb") as f:
        #         f.write(r.content)

        # download(
        #     "https://bitbucket.org/KNIME/knime-chemistry/raw/master/org.knime.chem.types/plugin.xml",
        #     "plugin.xml",
        # )
        # # python/src/org is the location of the source file in knime-chemistry
        # os.makedirs("python/src/org/knime/types", exist_ok=True)
        # download(
        #     "https://bitbucket.org/KNIME/knime-chemistry/raw/master/org.knime.chem.types/python/src/org/knime/types/chemistry.py",
        #     "python/src/org/knime/types/chemistry.py",
        # )
        # ktest.register_extension("plugin.xml")

        # this also puts it on the Pythonpath
        path_to_plugin_xml = os.path.join(
            os.path.dirname(__file__), "test", "plugin.xml"
        )
        ktest.register_extension(path_to_plugin_xml)

    def test_node_configure(self):
        import knime.types.chemistry as cet

        schema = knext.Schema.from_columns(
            [
                knext.Column(knext.int64(), "Integers"),
                knext.Column(knext.double(), "Doubles"),
            ]
        )

        node = TestNode()

        config_context = ktest.TestingConfigurationContext()

        with self.assertRaises(knext.InvalidParametersError):
            # no column set -> should raise an error
            node.configure(config_context, schema)

        node.column = "Integers"
        output_schema = node.configure(config_context, schema)

        expected_schema = knext.Schema.from_columns(
            [
                knext.Column(knext.int64(), "Integers"),
                knext.Column(knext.double(), "Doubles"),
                knext.Column(knext.int64(), "Integers_copy"),
                knext.Column(knext.logical(cet.SmilesValue), "Smiles"),
            ]
        )

        self.assertEqual(expected_schema, output_schema)

    def test_node_execute(self):
        input_df = pd.DataFrame(
            {"Test": [1, 2, 3, 4], "Test Strings": ["asdf", "foo", "bar", "baz"]}
        )

        node = TestNode()
        node.column = "Test"

        input = knext.Table.from_pandas(input_df)
        exec_context = ktest.TestingExecutionContext()
        output = node.execute(exec_context, input)

        output_df = output.to_pandas()
        expected_df = input_df
        expected_df["Test_copy"] = expected_df["Test"]
        for column in expected_df.columns:
            self.assertEqual(
                input_df[column].to_list(),
                output_df[column].to_list(),
                f"Values not equal in {column}",
            )

    def test_table_schema(self):
        import knime.types.chemistry as cet

        df = pd.DataFrame(
            {
                "Test": [1, 2, 3, 4],
                "Doubles": [1.1, 2.2, 3.3, 4.4],
                "Smiles": [
                    cet.SmilesValue("C"),
                    cet.SmilesValue("CC"),
                    cet.SmilesValue("CCC"),
                    cet.SmilesValue("CCCC"),
                ],
            }
        )

        table = knext.Table.from_pandas(df)

        expected_schema = knext.Schema.from_columns(
            [
                knext.Column(knext.int64(), "Test"),
                knext.Column(knext.double(), "Doubles"),
                knext.Column(knext.logical(cet.SmilesValue), "Smiles"),
            ]
        )
        self.assertEqual(expected_schema, table.schema)

    def test_single_batch(self):
        """
        same as test_node_execute, but using a node that works with batches
        """
        input_df = pd.DataFrame({"Test": [1, 2, 3, 4]})

        node = BatchTestNode()
        node.column = "Test"

        input = knext.Table.from_pandas(input_df)
        exec_context = ktest.TestingExecutionContext()
        output = node.execute(exec_context, input)

        output_df = output.to_pandas()
        expected_df = input_df
        expected_df["Test_copy"] = expected_df["Test"]
        for column in expected_df.columns:
            self.assertEqual(
                input_df[column].to_list(),
                output_df[column].to_list(),
                f"Values not equal in {column}",
            )

    def test_multiple_batches(self):
        """
        same as test_node_execute, but using a node that works with batches
        """

        node = BatchTestNode()
        node.column = "Test"

        input = knext.BatchOutputTable.create()
        input.append(pd.DataFrame({"Test": [1, 2]}))
        input.append(pd.DataFrame({"Test": [3, 4]}))

        exec_context = ktest.TestingExecutionContext()
        output = node.execute(exec_context, input)

        output_df = output.to_pandas()
        expected_df = pd.DataFrame({"Test": [1, 2, 3, 4]})
        expected_df["Test_copy"] = expected_df["Test"]
        for column in expected_df.columns:
            self.assertEqual(
                expected_df[column].to_list(),
                output_df[column].to_list(),
                f"Values not equal in {column}",
            )


if __name__ == "__main__":
    unittest.main()
