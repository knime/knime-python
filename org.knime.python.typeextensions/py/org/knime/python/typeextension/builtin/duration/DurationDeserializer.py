# ------------------------------------------------------------------------
#  Copyright by KNIME GmbH, Konstanz, Germany
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
#  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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

import datetime
import debug_util

def deserialize(bytes):
    durationstr = bytes.decode("utf-8")
    hidx = durationstr.find('H');
    hours = 0
    if hidx >= 0:
        hours = int(durationstr[:hidx])
        durationstr = durationstr[hidx+1:]
    midx = durationstr.find('M')
    minutes = 0
    if midx >= 0:
        minutes = int(durationstr[:midx])
        durationstr = durationstr[midx+1:]
    sidx = durationstr.find('S')
    seconds = 0
    millis = 0
    if sidx >= 0:
        pidx = durationstr.find('.')
        if pidx > 0:
            seconds = int(durationstr[:pidx])
            millistr = durationstr[pidx+1:sidx]
            while len(millistr) < 3:
                millistr += '0'
            millis = int(millistr)
            if seconds < 0:
                millis *= -1
        else:
            seconds = int(durationstr[:sidx])
    debug_util.debug_msg('Decoded: ' + bytes.decode("utf-8") + ' to ' + str(datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds, milliseconds=millis)))
    return datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds, milliseconds=millis)
