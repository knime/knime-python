/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python2.nodes.db;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.DatabaseReaderConnection;
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.kernel.PythonKernel;
import org.knime.python2.nodes.PythonNodeModel;

/**
 * This is the model implementation.
 *
 *
 * @author Tobias Koetter, KNIME.com, Zurich, Switzerland
 */
@SuppressWarnings("deprecation")
class PythonScriptDBNodeModel extends PythonNodeModel<PythonScriptDBNodeConfig> {

	/**
	 * Constructor for the node model.
	 */
	protected PythonScriptDBNodeModel() {
		super(new PortType[] { DatabasePortObject.TYPE }, new PortType[] { DatabasePortObject.TYPE });
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
		final PythonKernel kernel = new PythonKernel(getConfig().getUsePython3());
		final DatabasePortObject dbObj = (DatabasePortObject) inData[0];
		checkDBConnection(dbObj.getSpec());
		try {
			kernel.putFlowVariables(PythonScriptDBNodeConfig.getVariableNames().getFlowVariables(),
					getAvailableFlowVariables().values());
			final CredentialsProvider cp = getCredentialsProvider();
			final DatabaseQueryConnectionSettings connIn = dbObj.getConnectionSettings(cp);
			final Collection<String> jars = getJars(connIn);
			kernel.putSql(PythonScriptDBNodeConfig.getVariableNames().getGeneralInputObjects()[0], connIn, cp, jars);
			final String[] output = kernel.execute(getConfig().getSourceCode(), exec);
			setExternalOutput(new LinkedList<>(Arrays.asList(output[0].split("\n"))));
			setExternalErrorOutput(new LinkedList<>(Arrays.asList(output[1].split("\n"))));
			final Collection<FlowVariable> variables = kernel
					.getFlowVariables(PythonScriptDBNodeConfig.getVariableNames().getFlowVariables());
			addNewVariables(variables);
			final DatabaseQueryConnectionSettings connOut = new DatabaseQueryConnectionSettings(connIn,
					kernel.getSql(PythonScriptDBNodeConfig.getVariableNames().getGeneralInputObjects()[0]));
			final DatabaseReaderConnection dbCon = new DatabaseReaderConnection(connOut);
			final DataTableSpec resultSpec = dbCon.getDataTableSpec(cp);
			return new PortObject[] { new DatabasePortObject(new DatabasePortObjectSpec(resultSpec, connOut)) };
		} finally {
			kernel.close();
		}
	}

	static Collection<String> getJars(final DatabaseQueryConnectionSettings connectionSettings) throws IOException {
		// locate the jdbc jar files
		try {
			final Collection<String> jars = new LinkedList<>();
			final DatabaseUtility utility = connectionSettings.getUtility();
			final Collection<File> driverFiles = utility.getConnectionFactory().getDriverFactory()
					.getDriverFiles(connectionSettings);
			for (final File file : driverFiles) {
				final String absolutePath = file.getAbsolutePath();
				jars.add(absolutePath);
			}
			return jars;
		} catch (final Exception e) {
			throw new IOException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		final DatabasePortObjectSpec spec = (DatabasePortObjectSpec) inSpecs[0];
		checkDBConnection(spec);
		return new PortObjectSpec[] { null };
	}

	/**
	 * @param spec
	 * @throws InvalidSettingsException
	 */
	private void checkDBConnection(final DatabasePortObjectSpec spec) throws InvalidSettingsException {
		final DatabaseUtility utility = DatabaseUtility.getUtility(spec.getDatabaseIdentifier());
		if (!utility.supportsInsert() && !utility.supportsUpdate()) {
			throw new InvalidSettingsException("Database does not support insert or update");
		}
	}

	@Override
	protected PythonScriptDBNodeConfig createConfig() {
		return new PythonScriptDBNodeConfig();
	}

}
