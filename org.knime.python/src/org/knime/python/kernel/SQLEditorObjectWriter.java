package org.knime.python.kernel;

import java.util.Collection;

import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.python.kernel.proto.ProtobufKnimeSQLInput;
import org.knime.python.kernel.proto.ProtobufKnimeSQLInput.SQLInput;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutSQL;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutSQL.Builder;

public class SQLEditorObjectWriter implements EditorObjectWriter {

	private final String m_inputName;
	private final DatabaseQueryConnectionSettings m_conn;
	private final CredentialsProvider m_cp;
	private final Collection<String> m_jars;

	public SQLEditorObjectWriter(final String inputName, final DatabaseQueryConnectionSettings conn,
			final CredentialsProvider cp, final Collection<String> jars) {
		if (inputName == null || inputName.isEmpty()) {
			throw new IllegalArgumentException("Empty sql input name found");
		}
		if (conn == null) {
			throw new IllegalArgumentException("Database connection settings are not available");
		}
		if (cp == null) {
			throw new IllegalArgumentException("Credential provider is not available");
		}
		m_inputName = inputName;
		m_conn = conn;
		m_cp = cp;
		m_jars = jars;
	}

	@Override
	public String getInputVariableName() {
		return m_inputName;
	}

	@Override
	public byte[] getMessage() throws Exception {
		final SQLInput sqlMessage = sqlToProtobuf(m_conn, m_cp, m_jars);
		final Command.Builder commandBuilder = Command.newBuilder();
		final Builder sqlBuilder = PutSQL.newBuilder();
		sqlBuilder.setKey(m_inputName);
		sqlBuilder.setSql(sqlMessage);
		commandBuilder.setPutSQL(sqlBuilder);
		final byte[] byteArray = commandBuilder.build().toByteArray();
		return byteArray;
	}

	private static SQLInput sqlToProtobuf(final DatabaseQueryConnectionSettings conSettings,
			final CredentialsProvider cp, final Collection<String> jars) {
		final SQLInput.Builder sqlBuilder = ProtobufKnimeSQLInput.SQLInput.newBuilder();
		final String driver = conSettings.getDriver();
		sqlBuilder.setDriver(driver);
		sqlBuilder.setJDBCUrl(conSettings.getJDBCUrl());
		sqlBuilder.setUserName(conSettings.getUserName(cp));
		sqlBuilder.setPassword(conSettings.getPassword(cp));
		sqlBuilder.setDbIdentifier(conSettings.getDatabaseIdentifier());
		sqlBuilder.setConnectionTimeout(DatabaseConnectionSettings.getDatabaseTimeout());
		sqlBuilder.setTimezone(conSettings.getTimezone());
// TK_TODO: get auto commit from connection settings
		sqlBuilder.setAutocommit(false);
		sqlBuilder.setQuery(conSettings.getQuery());
		sqlBuilder.addAllJars(jars);
		return sqlBuilder.build();
	}
}
