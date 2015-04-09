package org.knime.python.kernel;

import org.knime.python.kernel.proto.ProtobufKnimeHiveOutput.HiveOutput;
import org.knime.python.kernel.proto.ProtobufKnimeSQLOutput.SQLOutput;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.GetSQL;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.GetSQL.Builder;

import com.google.protobuf.InvalidProtocolBufferException;

public class SQLEditorObjectReader implements EditorObjectReader {

	private final String m_name;
	private String m_query;
	private boolean m_hasHive = false;

	public SQLEditorObjectReader(final String name) {
		m_name = name;
	}

	@Override
	public Command getCommand() {
		final Command.Builder commandBuilder = Command.newBuilder();
		Builder setSql = GetSQL.newBuilder();
		setSql.setKey(m_name);
		commandBuilder.setGetSQL(setSql);
		final Command command = commandBuilder.build();
		return command;
	}

	@Override
	public void read(final byte[] readMessageBytes) throws InvalidProtocolBufferException {
		final SQLOutput sql = SQLOutput.parseFrom(readMessageBytes);
		m_query = sql.getQuery();
		HiveOutput hive = sql.getHive();
		if (hive != null) {
			m_hasHive = true;
		}
	}

	public String getQuery() {
		return m_query;
	}
	
	public boolean getHasHive() {
		return m_hasHive;
	}

}
