# ------------------------------------------------------------------------
#  Copyright by KNIME GmbH, Konstanz, Germany
#  Website: http://www.knime.org; Email: contact@knime.org
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

# namespace: flatc

import flatbuffers
import struct

class BooleanColumn(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsBooleanColumn(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = BooleanColumn()
        x.Init(buf, n + offset)
        return x

    # BooleanColumn
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # BooleanColumn
    def Values(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.BoolFlags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 1))
        return 0

    # BooleanColumn
    def ValuesLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # BooleanColumn
    def Missing(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.BoolFlags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 1))
        return 0

    # BooleanColumn
    def MissingLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0
    
    # custom method
    # Puts all values in this flatbuffers-column into a dataframe column.
    # @param df        a dataframe (preinitialized)
    # @param colidx    the index of the column to set (in the dataframe)
    def AddValuesAsColumn(self, df, colidx):
        #import debug_util
        #debug_util.breakpoint()
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            start = self._tab.Vector(o)
            l = self._tab.VectorLen(o)
            #len(boolean) = 1
            end = start + l
            vals = list(self._tab.Bytes[i] > 0 for i in range(start,end))
            # Handle missing values
            o2 = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
            if o2 != 0:
                start2 = self._tab.Vector(o2)
                m = self._tab.VectorLen(o2)
                for i in range(m): 
                    if(self._tab.Bytes[start2+i]):
                        vals[i] = None
                df.iloc[:,colidx] = vals
                return True
            return False
        return False

def BooleanColumnStart(builder): builder.StartObject(2)
def BooleanColumnAddValues(builder, values): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(values), 0)
def BooleanColumnStartValuesVector(builder, numElems): return builder.StartVector(1, numElems, 1)
def BooleanColumnAddMissing(builder, missing): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(missing), 0)
def BooleanColumnStartMissingVector(builder, numElems): return builder.StartVector(1, numElems, 1)
def BooleanColumnEnd(builder): return builder.EndObject()
