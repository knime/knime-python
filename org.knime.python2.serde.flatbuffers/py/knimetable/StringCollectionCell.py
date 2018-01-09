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

# namespace: flatc

import flatbuffers

class StringCollectionCell(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsStringCollectionCell(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = StringCollectionCell()
        x.Init(buf, n + offset)
        return x

    # StringCollectionCell
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # StringCollectionCell
    def Value(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.String(a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4))
        return ""

    # StringCollectionCell
    def ValueLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # StringCollectionCell
    def Missing(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.BoolFlags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 1))
        return 0

    # StringCollectionCell
    def MissingLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # StringCollectionCell
    def KeepDummy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.BoolFlags, o + self._tab.Pos)
        return 0
    
    # custom method
    # Returns all values as collection. All strings are decoded (utf-8).
    # @pram islist    true - returns a list, false - returns a set
    def GetAllValues(self, islist):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            a = self._tab.Vector(o)
            l = self.ValueLength()
            if islist:
                buff = list(self._tab.String(a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4)).decode("utf-8") for j in range(l))
                # Handle missing values
                o2 = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
                if o2 != 0:
                    a2 = self._tab.Vector(o2)
                    m = self.MissingLength()
                    for j in range(m):
                        if self._tab.Get(flatbuffers.number_types.BoolFlags, a2 + j):
                            buff[j] = None
                    return buff
                return 0
            else:
                buff = set(self._tab.String(a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4)).decode("utf-8") for j in range(l))
                # Handle missing values
                if self.KeepDummy():
                    buff.add(None)
                return buff
        return 0

def StringCollectionCellStart(builder): builder.StartObject(3)
def StringCollectionCellAddValue(builder, value): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(value), 0)
def StringCollectionCellStartValueVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def StringCollectionCellAddMissing(builder, missing): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(missing), 0)
def StringCollectionCellStartMissingVector(builder, numElems): return builder.StartVector(1, numElems, 1)
def StringCollectionCellAddKeepDummy(builder, keepDummy): builder.PrependBoolSlot(2, keepDummy, 0)
def StringCollectionCellEnd(builder): return builder.EndObject()
