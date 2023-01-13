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
import os
from codecs import ignore_errors
from os import pardir
from typing import Type, Union
import unittest
import pandas as pd
import pyarrow as pa
import numpy as np

import knime._arrow._backend as ka
import knime.scripting._deprecated._arrow_table as kat
import knime_node_arrow_table as knat

import knime._arrow._pandas as kap
import knime._arrow._types as katy
import knime._arrow._backend as knar
import knime_types as kt

from testing_utility import (
    DummyJavaDataSink,
    DummyWriter,
    TestDataSource,
    _generate_backends,
    _generate_test_data_frame,
    _generate_test_table,
    _register_extension_types,
)


def _register_rdkit_value_factories():
    chem_module = "chemistry"

    kt.register_python_value_factory(
        chem_module,
        "SmilesValueFactory",
        '{"type": "struct", "inner_types": ["variable_width_binary", "string"]}',
        """
        {
            "type": "struct", 
            "traits": { "logical_type": "{\\"value_factory_class\\":\\"org.knime.geospatial.core.data.cell.GeoCell$ValueFactory\\"}" }, 
            "inner": [
                {"type": "simple", "traits": {}},
                {"type": "simple", "traits": {}}
            ]
        }
        """,
        "knime.types.chemistry.SmilesValue",
    )


class RdKitExtensionTypeTest(unittest.TestCase):
    """
    Tests for the RDKit extension types.
    """

    @classmethod
    def setUpClass(cls):
        try:
            # from rdkit import Chem
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

    def test_rdkit_extension_types(self):
        """
        Tests the RDKit extension types.
        """
        df = _generate_test_data_frame(
            "rdkit.zip", columns=["smiles", "mol", "fingerprints"]
        )
        elem = df["fingerprints"][0]
        import knime_schema as ks

        rdkit_fp_type = type(elem)
        knime_type = ks.logical(rdkit_fp_type)
        fingerprint_type = knime_type.to_pandas()
        df["CountFingerprint"] = pd.Series(
            [elem], dtype=fingerprint_type, index=df.index
        )
        df["CountFingerprint"][0] = elem
