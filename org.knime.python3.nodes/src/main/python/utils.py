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
Helper utilities for the KNIME Pure-Python Node Extension API.

@author Ivan Prigarin, KNIME GmbH, Konstanz, Germany
"""

from collections import namedtuple


def parse_version(version_string):
    """
    Parses a string of the form "0.1.2" into a Version namedtuple, which can then be compared with other Version objects.
    The constituent parts of the version (major, minor, patch) are only allowed to be non-negative integers.

    If the provided string is None, then the Version is set to "0.0.0".
    If the provided string is not of the correct format, then a ValueError is raised.
    """
    if version_string is None:
        return Version(0, 0, 0)

    if type(version_string) is Version:
        return version_string

    try:
        major, minor, patch = [int(part) for part in version_string.split(".")]
        assert major >= 0 and minor >= 0 and patch >= 0
        return Version(major, minor, patch)
    except (ValueError, AssertionError):
        raise ValueError(
            f"Incorrect version format: '{version_string}'. Must be of the form 'major.minor.patch', with non-negative integers for major, minor and patch."
        )


class Version(namedtuple("Version", ["major", "minor", "patch"])):
    """
    A Version namedtuple with a __repr__ method that returns a string of the form "0.1.2".
    Version objects can be compared with other Version objects.
    """

    def __repr__(self):
        return f"{self.major}.{self.minor}.{self.patch}"

    def __str__(self):
        return self.__repr__()
