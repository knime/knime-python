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
Allows to load Jupyter notebooks as Python modules and to print the content of Jupyter notebooks to the console.

@author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
@author Christian Dietz, KNIME GmbH, Konstanz, Germany
"""

# Unfortunately, we have to redeclare those methods here (instead of just re-exporting them) because otherwise jedi
# wouldn't find the docstrings.

import JupyterSupport as __implementation__


def load_notebook(notebook_directory, notebook_name, notebook_version=None, only_include_tag=None):
    """
    Loads the Jupyter notebook located in the given directory and of the given file name and returns it as Python
    module. The module contains the content of each code cell of the notebook. In particular, top-level definitions
    (e.g., of classes or functions) and global variables can be accessed as with regular Python modules.
    :param notebook_directory: The path to the directory in which the notebook file is located.
                               The KNIME URL protocol (knime://) is supported. Should be a string.
    :param notebook_name: The name of the notebook file, including the file extension. Should be a string.
    :param notebook_version: The Jupyter notebook format version. Defaults to 'None' in which case the version is read
                             from file. Only use this option if you experience compatibility issues (e.g., if KNIME
                             doesn't understand the notebook format). Should be an integer.
    :param only_include_tag: Only load cells that are annotated with the given custom cell tag (since Jupyter 5.0.0).
                             This is useful to mark cells that are intended to be used in a Python module and
                             exclude other, unmarked, cells, e.g., ones that do visualization or contain demo code.
                             Defaults to 'None' in which case all code cells are included. Should be a string.
    :return: The Jupyter notebook as Python module.
    """
    return __implementation__.load_notebook(notebook_directory, notebook_name, notebook_version, only_include_tag)


def print_notebook(notebook_directory, notebook_name, notebook_version=None, only_include_tag=None):
    """
    Prints the type and textual content of each cell of the Jupyter notebook in the given directory and of the given
    file name to the console.
    :param notebook_directory: The path to the directory in which the notebook file is located.
                               The KNIME URL protocol (knime://) is supported. Should be a string.
    :param notebook_name: The name of the notebook file, including the file extension. Should be a string.
    :param notebook_version: The Jupyter notebook format version. Defaults to 'None' in which case the version is read
                             from file. Only use this option if you experience compatibility issues (e.g., if KNIME
                             doesn't understand the notebook format). Should be an integer.
    :param only_include_tag: Only print cells that are annotated with the given custom cell tag (since Jupyter 5.0.0).
                             Defaults to 'None' in which case all cells are included. Should be a string.
    :return: The string that was printed to the console.
    """
    return __implementation__.print_notebook(notebook_directory, notebook_name, notebook_version, only_include_tag)
