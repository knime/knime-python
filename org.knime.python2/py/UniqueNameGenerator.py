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
Python implementation of org.knime.core.util.UniqueNameGenerator.

@author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
@author Christian Dietz, KNIME GmbH, Konstanz, Germany
"""

import re

# Pre and suffix are needed to match the entire string (mimics Pattern.fullmatch which is only available in Python
# 3.4+).
_pattern = re.compile("(?:" + "(^.*) \\(#(\\d+)\\)" + r")\Z")


class UniqueNameGenerator(object):

    def __init__(self, names=None):
        self._name_hash = set() if names is None else set(names)

    def new_name(self, suggested):
        if suggested is None:
            raise ValueError("Argument must not be None.")

        trimmed_name = suggested.strip()

        if trimmed_name not in self._name_hash:
            self._name_hash.add(trimmed_name)
            return trimmed_name

        index = 1
        base_name = trimmed_name
        base_name_matcher = _pattern.match(base_name)
        if base_name_matcher is not None:
            base_name = base_name_matcher.group(1)
            try:
                index = int(base_name_matcher.group(2)) + 1
            except:
                pass

        while True:
            new_name = base_name + " (#" + str(index) + ")"
            index += 1
            if new_name not in self._name_hash:
                self._name_hash.add(new_name)
                return new_name
