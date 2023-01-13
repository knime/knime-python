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
@author Jonas Klotz, KNIME GmbH, Berlin, Germany
"""

import unittest
import knime_schema as ks
import knime.api.types as kt
import pandas as pd

from testing_utility import (
    _generate_test_data_frame,
    _register_extension_types,
)


def _register_rdkit_value_factories():
    chem_module = "chemistry"

    kt.register_python_value_factory(
        chem_module,
        "SmilesAdapterValueFactory",
          '{"type": "struct", "inner_types": ["string", "variable_width_binary"]}',
        """
        {
            "type": "struct", 
            "traits": {"logical_type": "{\\"value_factory_class\\":\\"org.knime.chem.types.SmilesAdapterCellValueFactory\\"}"},
            "inner": [
                {"type": "simple", "traits": {}},
                {"type": "simple", "traits": {}}
            ]
        }
        """,
        "chemistry.SmilesAdapterValue",
    )
    kt.register_python_value_factory(
        chem_module,
        "SmilesValueFactory",
        '{"type": "struct", "inner_types": ["string", "variable_width_binary"]}',
        """
        {
            "type": "struct", 
            "traits": {"logical_type": "{\\"value_factory_class\\":\\"org.knime.chem.types.SmilesCellValueFactory\\"}"},
            "inner": [
                {"type": "simple", "traits": {}},
                {"type": "simple", "traits": {}}
            ]
        }
        """,
        "chemistry.SmilesValue",
    )



class RdKitExtensionTypeTest(unittest.TestCase):
    """
    Tests for the RDKit extension types.
    """

    @classmethod
    def setUpClass(cls):
        try:
            import sys
            import os

            # we expect that the knime-chemistry git repository is located next to knime-python
            sys.path.append(
                os.path.join(
                    "..",
                    "..",
                    "..",
                    "..",
                    "..",
                    "..",
                    "knime-chemistry",
                    "org.knime.chem.types",
                    "python",
                    "src",
                    "org",
                    "knime",
                    "types",
                )
            )
            import chemistry as chemtypes
        except ImportError as e:
            raise unittest.SkipTest("Module is not installed:  \n", e)

        _register_extension_types()
        _register_rdkit_value_factories()

    def test_type_registration(self):
        """ Tests if the RDKit extension types are registered correctly.
        """
        df = _generate_test_data_frame(
            "rdkit.zip", columns=["smiles", "fingerprint", "countFingerprint"]
        )
        for col_key in df.columns:
            col_index = df.columns.get_loc(col_key)
            rdkit_fp_type = type(df.iloc[2, col_index])
            knime_type = ks.logical(rdkit_fp_type)
            pandas_type = knime_type.to_pandas()
            col_type = df.dtypes[col_index]
            self.assertEqual(pandas_type, col_type)

    def test_setting_in_rdkit_extension_array(self):
        """
        Tests if the RDKit extension array setitem method works as expected.
        """
        df = _generate_test_data_frame(
            "rdkit.zip", columns=["smiles", "fingerprint", "countFingerprint"]
        )
        df.reset_index(inplace=True, drop=True)  # drop index as it messes up equality

        df.loc[1, lambda dfu: [df.columns[0]]] = df.loc[2, lambda dfu: [df.columns[0]]]

        # test single item setting with int index for all columns
        for col_key in df.columns:
            col_index = df.columns.get_loc(col_key)
            df.iloc[1, col_index] = df.iloc[2, col_index]  # test iloc
            df.loc[1, col_key] = df.loc[2, col_key]

        self.assertTrue(df.iloc[1].equals(df.iloc[2]), msg="The rows are not equal")

        # test slice setting
        for col_key in df.columns:
            col_index = df.columns.get_loc(col_key)
            df.iloc[:3, col_index] = df.iloc[3:6, col_index]
            df.loc[:3, col_key] = df.loc[3:6, col_key]

        self.assertTrue(df.iloc[0].equals(df.iloc[2]), msg="The rows are not equal")

        # test concat setting
        df = pd.concat([df, df.iloc[2].to_frame().T])
        self.assertTrue(df.iloc[2].equals(df.iloc[-1]))


