package org.knime.python.kernel;

import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command;

import com.google.protobuf.InvalidProtocolBufferException;

public interface EditorObjectReader {

    Command getCommand();

    void read(byte[] readMessageBytes) throws InvalidProtocolBufferException;

}
