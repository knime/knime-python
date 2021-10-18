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
@author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
"""

import pyarrow as pa
import sys
from typing import Optional, Union

import knime_arrow_pandas
import knime_gateway as kg


class PythonKernel(kg.EntryPoint):
    def __init__(self):
        self._workspace = {}  # TODO: should we make this thread safe?

    def putTableIntoWorkspace(
        self,
        variable_name: str,
        java_table_data_source,
        num_rows: int,
        sentinel: Optional[Union[str, int]] = None,
    ) -> None:
        with kg.data_source_mapper(java_table_data_source) as table_data_source:
            table = table_data_source.to_arrow_table(num_rows)
            if sentinel is not None:
                for i, column in enumerate(table):
                    if pa.types.is_integer(column.type) and column.null_count != 0:
                        if sentinel == "min":
                            sentinel_value = (
                                -2147483648
                                if pa.types.is_int32(column.type)
                                else -9223372036854775808
                            )
                        elif sentinel == "max":
                            sentinel_value = (
                                2147483647
                                if pa.types.is_int32(column.type)
                                else 9223372036854775807
                            )
                        else:
                            sentinel_value = int(sentinel)
                        column = column.fill_null(sentinel_value)
                        table = table.set_column(i, table.field(i), column)
            data_frame = knime_arrow_pandas.arrow_table_to_pandas_df(table)
            # The first column of a KNIME table is interpreted as its index (row keys).
            data_frame.set_index(data_frame.columns[0], inplace=True)
            self._workspace[variable_name] = data_frame

    class Java:
        implements = ["org.knime.python3for2.Python3KernelBackendProxy"]


if __name__ == "__main__":
    try:
        # Hook into warning delivery.
        import warnings

        default_showwarning = warnings.showwarning

        def showwarning_hook(message, category, filename, lineno, file=None, line=None):
            """
            Copied from warnings.showwarning.
            We use this hook to prefix warning messages with "[WARN]". This makes them easier to identify on Java
            side and helps printing them at the correct log level.
            Providing a custom hook is supported as per the API documentations:
            https://docs.python.org/3/library/warnings.html#warnings.showwarning
            """
            try:
                if file is None:
                    file = sys.stderr
                    if file is None:
                        # sys.stderr is None when run with pythonw.exe - warnings get lost.
                        return
                try:
                    # Do not change the prefix. Expected on Java side.
                    file.write(
                        "[WARN]"
                        + warnings.formatwarning(
                            message, category, filename, lineno, line
                        )
                    )
                except OSError:
                    pass  # The file (probably stderr) is invalid - this warning gets lost.
            except Exception:
                # Fall back to the default implementation.
                return default_showwarning(
                    message, category, filename, lineno, file, line
                )

        warnings.showwarning = showwarning_hook
    except Exception:
        pass

    kernel = PythonKernel()
    kg.connect_to_knime(kernel)
