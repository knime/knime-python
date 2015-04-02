package org.knime.python.kernel;

import org.knime.python.kernel.proto.ProtobufKnimeSQLOutput;
import org.knime.python.kernel.proto.ProtobufKnimeSQLOutput.SQLOutput;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.GetSQL;

import com.google.protobuf.InvalidProtocolBufferException;

public class SQLEditorObjectReader implements EditorObjectReader {

	private final String m_name;
	private String m_query;

	public SQLEditorObjectReader(final String name) {
		m_name = name;
	}

	@Override
	public Command getCommand() {
		final Command.Builder commandBuilder = Command.newBuilder();
		final SQLOutput.Builder sqlBuilder = ProtobufKnimeSQLOutput.SQLOutput.newBuilder();
		sqlBuilder.setQuery("");
		commandBuilder.setGetSQL(GetSQL.newBuilder().setKey(m_name).setSql(sqlBuilder.build()));
		final Command command = commandBuilder.build();
		return command;
	}

	@Override
	public void read(final byte[] readMessageBytes) throws InvalidProtocolBufferException {
		final SQLOutput sql = SQLOutput.parseFrom(readMessageBytes);
		m_query = sql.getQuery();
	}

	public String getQuery() {
		return m_query;
	}

}
