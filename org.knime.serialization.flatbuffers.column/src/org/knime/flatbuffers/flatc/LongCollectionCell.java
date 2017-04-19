// automatically generated by the FlatBuffers compiler, do not modify

package org.knime.flatbuffers.flatc;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class LongCollectionCell extends Table {
  public static LongCollectionCell getRootAsLongCollectionCell(ByteBuffer _bb) { return getRootAsLongCollectionCell(_bb, new LongCollectionCell()); }
  public static LongCollectionCell getRootAsLongCollectionCell(ByteBuffer _bb, LongCollectionCell obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public LongCollectionCell __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public long value(int j) { int o = __offset(4); return o != 0 ? bb.getLong(__vector(o) + j * 8) : 0; }
  public int valueLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer valueAsByteBuffer() { return __vector_as_bytebuffer(4, 8); }
  public boolean missing(int j) { int o = __offset(6); return o != 0 ? 0!=bb.get(__vector(o) + j * 1) : false; }
  public int missingLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer missingAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public boolean keepDummy() { int o = __offset(8); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }

  public static int createLongCollectionCell(FlatBufferBuilder builder,
      int valueOffset,
      int missingOffset,
      boolean keepDummy) {
    builder.startObject(3);
    LongCollectionCell.addMissing(builder, missingOffset);
    LongCollectionCell.addValue(builder, valueOffset);
    LongCollectionCell.addKeepDummy(builder, keepDummy);
    return LongCollectionCell.endLongCollectionCell(builder);
  }

  public static void startLongCollectionCell(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addValue(FlatBufferBuilder builder, int valueOffset) { builder.addOffset(0, valueOffset, 0); }
  public static int createValueVector(FlatBufferBuilder builder, long[] data) { builder.startVector(8, data.length, 8); for (int i = data.length - 1; i >= 0; i--) builder.addLong(data[i]); return builder.endVector(); }
  public static void startValueVector(FlatBufferBuilder builder, int numElems) { builder.startVector(8, numElems, 8); }
  public static void addMissing(FlatBufferBuilder builder, int missingOffset) { builder.addOffset(1, missingOffset, 0); }
  public static int createMissingVector(FlatBufferBuilder builder, boolean[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addBoolean(data[i]); return builder.endVector(); }
  public static void startMissingVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static void addKeepDummy(FlatBufferBuilder builder, boolean keepDummy) { builder.addBoolean(2, keepDummy, false); }
  public static int endLongCollectionCell(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

