package org.knime.python.kernel;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseDriverLoader;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.python.kernel.proto.ProtobufKnimeRemoteFileInput;
import org.knime.python.kernel.proto.ProtobufKnimeRemoteFileInput.RemoteFileInput;
import org.knime.python.kernel.proto.ProtobufKnimeSQLInput;
import org.knime.python.kernel.proto.ProtobufKnimeSQLInput.SQLInput;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutSQL;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.PutSQL.Builder;

public class SQLEditorObjectWriter implements EditorObjectWriter {


	private final String m_inputName;
	private final DatabaseQueryConnectionSettings m_conn;
	private final CredentialsProvider m_cp;
	private final ConnectionInformation m_fileInfo;

	public SQLEditorObjectWriter(final String inputName, final DatabaseQueryConnectionSettings conn,
			final CredentialsProvider cp) {
		this(inputName, conn, cp, null);
	}

	public SQLEditorObjectWriter(final String inputName, final DatabaseQueryConnectionSettings conn,
			final CredentialsProvider cp, final ConnectionInformation fileInfo) {
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
		m_fileInfo = fileInfo;
	}

	@Override
	public String getInputVariableName() {
		return m_inputName;
	}

	@Override
	public byte[] getMessage() throws Exception {
		final SQLInput sqlMessage = sqlToProtobuf(m_conn, m_cp);
		final RemoteFileInput fileMessage = remoteFileToProtobuf(m_fileInfo);
		final Command.Builder commandBuilder = Command.newBuilder();
		final Builder sqlBuilder = PutSQL.newBuilder();
		sqlBuilder.setKey(m_inputName);
		sqlBuilder.setSql(sqlMessage);
		if (fileMessage != null) {
			sqlBuilder.setFile(fileMessage);
		}
		commandBuilder.setPutSQL(sqlBuilder);
		final byte[] byteArray = commandBuilder.build().toByteArray();
		return byteArray;
	}

	private SQLInput sqlToProtobuf(final DatabaseQueryConnectionSettings conSettings,
			final CredentialsProvider cp) throws IOException {
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
		sqlBuilder.setAutocommit(true);
		sqlBuilder.setQuery(conSettings.getQuery());
		//locate the jdbc jar files
		final Collection<String> jars = new LinkedList<>();
		try {
			final File driverFile = DatabaseDriverLoader.getDriverFileForDriverClass(driver);
			if (driverFile != null) {
				final String absolutePath = driverFile.getAbsolutePath();
				jars.add(absolutePath);
			}
		} catch (final Exception e) {
			throw new IOException(e);
		}
		sqlBuilder.addAllJars(jars);
		return sqlBuilder.build();
	}

	private RemoteFileInput remoteFileToProtobuf(final ConnectionInformation con) throws IOException {
		if (con == null) {
			return null;
		}
		final RemoteFileInput.Builder fileBuilder = ProtobufKnimeRemoteFileInput.RemoteFileInput.newBuilder();
		fileBuilder.setProtocol(con.getProtocol());
		fileBuilder.setHost(con.getHost());
		fileBuilder.setPort(con.getPort());
		fileBuilder.setUser(con.getUser());
		fileBuilder.setPassword(con.getPassword());
		final String keyfile = con.getKeyfile();
		if (keyfile != null) {
			fileBuilder.setKeyfile(keyfile);
		}
		final String hosts = con.getKnownHosts();
		if (hosts != null) {
			fileBuilder.setKnownHosts(hosts);
		}
		fileBuilder.setTimeout(con.getTimeout());
		return fileBuilder.build();
	}
}
