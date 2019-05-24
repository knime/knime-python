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
Importing this module should be the first statement in each module (except for __future__ statements) that makes
specific demands on the Python environment.

NOTE: Importing this module downstream may under no condition lead to errors since it's used by critical parts of the
integration (e.g., installation testing). That's why all the top-level import statements for 3rd party modules in this
module catch BaseException.

@author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
@author Christian Dietz, KNIME GmbH, Konstanz, Germany
"""

import sys

_is_python3 = sys.version_info >= (3, 0)
if not _is_python3:
    reload(sys)
    sys.setdefaultencoding('utf-8')  # Available after reloading.

import warnings

# Suppress FutureWarnings.
warnings.filterwarnings(action='ignore', category=FutureWarning)

try:
    import jedi

    _is_jedi_available = True
except BaseException:
    _is_jedi_available = False

_is_tslib_available = False
try:
    from pandas._libs.tslibs.timestamps import Timestamp
    from pandas._libs.tslibs.timestamps import NaT
    _is_tslib_available = True
except BaseException:
    pass
if not _is_tslib_available:
    try:
        from pandas.tslib import Timestamp
        from pandas.tslib import NaT
        _is_tslib_available = True
    except BaseException:
        pass
if not _is_tslib_available:
    try:
        # pandas 0.24+
        from pandas._libs.tslib import Timestamp
        from pandas._libs.tslibs import NaT
        _is_tslib_available = True
    except BaseException:
        pass


def is_python3():
    return _is_python3


def is_jedi_available():
    return _is_jedi_available


def is_tslib_available():
    return _is_tslib_available


def dummy_call():
    """
    Helps keeping the import statement of this module at the top of a module (when auto-formatting).
    Just call this method right after the import.
    Yes, this is a dirty hack.
    """
    pass
