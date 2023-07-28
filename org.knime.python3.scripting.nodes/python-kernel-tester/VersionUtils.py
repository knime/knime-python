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
Local implementation of the deprecated distutils.LooseVersion class.

@author Ivan Prigarin, KNIME GmbH, Konstanz, Germany
"""

import re

from collections import namedtuple


def LooseVersion(vstring):
    """
    A factory that parses the provided version string where components are separated by
    dots and returns a LooseVersion object that can be compared with other LooseVersion
    objects.

    The LooseVersion class replicates the string parsing and comparison behaviour of the
    distutils.LooseVersion class, which is deprecated.
    """
    version_components = []
    major_components = vstring.strip(".").split(".")

    for component in major_components:
        # split each major component into groups of numeric and alphabetic minor components
        minor_components = re.findall(r"[^\W\d_]+|\d+", component)
        minor_components = [int(c) if c.isdigit() else c for c in minor_components]

        version_components += minor_components

    class LooseVersion(
        namedtuple(
            "MyLooseVersion",
            ["component_" + str(i) for i in range(len(version_components))],
        )
    ):
        def __repr__(self):
            return self.vstring

        def __str__(self):
            return self.__repr__()

    loose_version = LooseVersion._make(version_components)
    loose_version.version = version_components
    loose_version.vstring = vstring
    return loose_version
