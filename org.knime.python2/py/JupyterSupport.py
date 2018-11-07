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
@author Christian Dietz, KNIME GmbH, Konstanz, Germany

# Some of the following code is copied/modified from:
# http://jupyter-notebook.readthedocs.io/en/stable/examples/Notebook/Importing%20Notebooks.html
# © Copyright 2015, Jupyter Team, https://jupyter.org. Revision 7674331e
"""

from __future__ import absolute_import

import io
import os
import posixpath
import sys
import types
import warnings

import EnvironmentHelper

_dependencies_import_errors = set()
try:
    from IPython import get_ipython
except Exception as ex:
    _dependencies_import_errors.add(str(ex))
try:
    from nbformat import read
except Exception as ex:
    _dependencies_import_errors.add(str(ex))
try:
    from nbformat import NO_CONVERT
except Exception as ex:
    _dependencies_import_errors.add(str(ex))
try:
    from IPython.core.interactiveshell import InteractiveShell
except Exception as ex:
    _dependencies_import_errors.add(str(ex))


def load_notebook(notebook_directory, notebook_name, notebook_version=None, only_include_tag=None):
    _check_availability()
    # User-facing; standardize input.
    notebook_path, notebook_version, only_include_tag = _standardize_input(notebook_directory, notebook_name,
                                                                           notebook_version,
                                                                           only_include_tag)
    return _NotebookLoader(notebook_path, notebook_version).load_notebook_module(only_include_tag=only_include_tag)


def print_notebook(notebook_directory, notebook_name, notebook_version=None, only_include_tag=None):
    _check_availability()
    # User-facing; standardize input.
    notebook_path, notebook_version, only_include_tag = _standardize_input(notebook_directory, notebook_name,
                                                                           notebook_version,
                                                                           only_include_tag)
    notebook = _load_notebook_from_path(notebook_path, notebook_version)
    cells = _get_notebook_cells(notebook)
    notebook_lines = ["Notebook at path '" + notebook_path + "':"]
    for i, cell in enumerate(cells):
        # If we are only including particular tags, check now.
        if _include_cell(cell, only_include_tag):
            notebook_lines.append("Cell #" + str(i) + " (" + cell.cell_type + "):")
            notebook_lines.append(_get_notebook_cell_source(cell))
    if len(notebook_lines) == 1:
        # Inform user that notebook is empty.
        if only_include_tag is not None:
            notebook_lines.append("<no included cells>")
        else:
            notebook_lines.append("<empty>")
    notebook_string = '\n'.join(notebook_lines)
    print(notebook_string)
    return notebook_string


def _check_availability():
    if len(_dependencies_import_errors) > 0:
        raise ValueError("Jupyter notebook support is not available due to missing dependencies.\nPlease make sure "
                         + "packages 'IPython' and 'nbformat' are installed in your local Python environment and "
                         + "re-execute the node/reopen the node dialog. Details:\n"
                         + "\n".join(_dependencies_import_errors))


def _standardize_input(notebook_directory, notebook_name, notebook_version, only_include_tag):
    if notebook_directory is None:
        raise ValueError("Notebook directory must not be None.")
    if notebook_name is None:
        raise ValueError("Notebook name must not be None.")

    notebook_directory = str(notebook_directory)
    notebook_name = str(notebook_name)
    notebook_path = None

    if notebook_directory.startswith("knime:"):
        exception = None
        try:
            notebook_path = posixpath.join(notebook_directory, notebook_name)
            notebook_path = _resolve_knime_url(notebook_path)
        except Exception as ex:
            exception = ex
        if exception is not None:
            # Raise exception outside of the original catch block to avoid polluting the console with the full
            # traceback.
            raise ValueError(str(exception))
        if not EnvironmentHelper.is_python3():
            # Java always returns a unicode string, Python 2 wants non-unicode.
            notebook_path = str(notebook_path)
    else:
        notebook_path = os.path.join(notebook_directory, notebook_name)

    if not os.path.isfile(notebook_path):
        raise ValueError("Notebook path '" + notebook_path + "' does not point to an existing file.")

    if notebook_version is None:
        notebook_version = NO_CONVERT
    else:
        try:
            notebook_version = int(notebook_version)
        except ValueError:
            notebook_version = None
        # Raise exception outside of the original catch block to avoid polluting the console with the full traceback.
        if notebook_version is None:
            raise ValueError("Notebook version must be an integer or castable to an integer.")

    if only_include_tag is not None:
        only_include_tag = str(only_include_tag)

    return notebook_path, notebook_version, only_include_tag


def _load_notebook_from_path(notebook_path, notebook_version):
    with io.open(notebook_path, 'r', encoding='utf-8') as f:
        return read(f, notebook_version)


def _get_notebook_cells(notebook):
    if hasattr(notebook, "cells"):
        return notebook.cells
    elif hasattr(notebook, "worksheets"):
        cells = []
        for worksheet in notebook.worksheets:
            cells.extend(worksheet.cells)
        return cells
    else:
        _raise_notebook_format_not_understood()


def _include_cell(cell, only_include_tag):
    return not (only_include_tag is not None and (
            'tags' not in cell.metadata or only_include_tag not in cell.metadata['tags']))


def _get_notebook_cell_source(cell):
    if hasattr(cell, "source"):
        return cell.source
    elif hasattr(cell, "input"):
        return cell.input
    else:
        _raise_notebook_format_not_understood()


def _raise_notebook_format_not_understood():
    raise RuntimeError(
        "Notebook format not understood. You may want to use a more recent Jupyter notebook format version.")


class _NotebookLoader(object):
    """
    Import a notebook as a module.
    """

    def __init__(self, notebook_path, notebook_version):
        self._shell = InteractiveShell.instance()
        # Work around NotImplementedError in InteractiveShell.
        self._shell.enable_gui = lambda x: False
        self._notebook_path = notebook_path
        self._notebook_version = notebook_version

    def load_notebook_module(self, only_include_tag=None):
        notebook = _load_notebook_from_path(self._notebook_path, self._notebook_version)

        # Create the module and add it to sys.modules.
        notebook_module = types.ModuleType(self._notebook_path)
        notebook_module.__file__ = self._notebook_path
        notebook_module.__loader__ = self
        notebook_module.__dict__['get_ipython'] = get_ipython
        sys.modules[self._notebook_path] = notebook_module

        # Extra work to ensure that magics that would affect the user_ns actually affect the notebook module's ns.
        save_user_ns = self._shell.user_ns
        self._shell.user_ns = notebook_module.__dict__
        module_is_populated = False
        try:
            cells = _get_notebook_cells(notebook)
            for cell in cells:
                if cell.cell_type == 'code':
                    # If we are only including particular tags, check now.
                    if _include_cell(cell, only_include_tag):
                        # Transform the input to executable Python code.
                        code = self._shell.input_transformer_manager.transform_cell(_get_notebook_cell_source(cell))
                        # Run the code.
                        try:
                            exec(code, notebook_module.__dict__)
                            module_is_populated = True
                        except Exception:
                            print("Failing noteboook code:\n" + code)
                            raise
        finally:
            self._shell.user_ns = save_user_ns
        if not module_is_populated:
            # Inform user that notebook module is empty.
            warnings.warn("Importing the Jupyter notebook resulted in an empty Python module.")
        return notebook_module


# Unary function: String -> String. Populated by Python kernel during startup.
_resolve_knime_url = None
