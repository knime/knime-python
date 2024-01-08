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
@author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
"""

import os
from pathlib import Path
import glob
import shutil

TARGET = Path(".")
PREFIX_CORE = Path("../org.knime.python3/src/main/python")
PREFIX_NODES = Path("../org.knime.python3.nodes/src/main/python")
PREFIX_VIEWS = Path("../org.knime.python3.views/src/main/python")


def copy_files_to_package(target, source_prefix, files):
    for file in files:
        target_file = target / file
        os.makedirs(target_file.parent, exist_ok=True)
        shutil.copy(source_prefix / file, target_file)


def is_private(file):
    return file.name.startswith("_") and file.name != "__init__.py"


def find_files(prefix, pattern):
    files = [Path(f) for f in glob.glob(str(prefix / pattern))]
    return [
        f.relative_to(prefix)  # relative to the prefix
        for f in files
        if not is_private(f)  # ignore private files
    ]


# Create knime-extension
MODULE_TARGET = TARGET / "knime-extension"
os.makedirs(MODULE_TARGET, exist_ok=True)

# Copy the knime_extension file
legacy_knime_extension_folder = MODULE_TARGET / "knime_extension"

os.makedirs(legacy_knime_extension_folder, exist_ok=True)
shutil.copy(
    PREFIX_NODES / "knime_extension.py", legacy_knime_extension_folder / "__init__.py"
)

# Copy the files from knime.extension
copy_files_to_package(
    MODULE_TARGET, PREFIX_NODES, find_files(PREFIX_NODES, "knime/extension/*.py")
)

# Copy the files from knime.api
copy_files_to_package(
    MODULE_TARGET, PREFIX_CORE, find_files(PREFIX_CORE, "knime/api/*.py")
)
copy_files_to_package(
    MODULE_TARGET, PREFIX_VIEWS, find_files(PREFIX_VIEWS, "knime/api/*.py")
)
