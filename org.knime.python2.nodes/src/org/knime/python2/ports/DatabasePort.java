/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
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
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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
 * ---------------------------------------------------------------------
 *
 * History
 *   Oct 29, 2020 (marcel): created
 */
package org.knime.python2.ports;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.aggregation.DBAggregationFunctionFactory;
import org.knime.core.node.port.database.reader.DBReader;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.config.WorkspacePreparer;
import org.knime.python2.kernel.Python2KernelBackend;
import org.knime.python2.kernel.PythonKernel;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class DatabasePort implements InputPort, OutputPort {

    private final String m_variableName;

    private CredentialsProvider m_credentials;

    private DatabaseQueryConnectionSettings m_inDatabaseConnection;

    public DatabasePort(final String variableName) {
        m_variableName = variableName;
    }

    public void setCredentialsProvider(final CredentialsProvider credentials) {
        m_credentials = credentials;
    }

    // Input port & output port:

    @Override
    public PortType getPortType() {
        return DatabaseConnectionPortObject.TYPE;
    }

    @Override
    public String getVariableName() {
        return m_variableName;
    }

    @Override
    public double getExecuteProgressWeight() {
        return 0d;
    }

    // Input port:

    @Override
    public Collection<PythonModuleSpec> getRequiredModules() {
        return Arrays.asList(new PythonModuleSpec("jpype"));
    }

    @Override
    public void configure(final PortObjectSpec inSpec) throws InvalidSettingsException {
        checkDatabaseConnection((DatabasePortObjectSpec)inSpec);
    }

    @Override
    public WorkspacePreparer prepareInDialog(final PortObjectSpec inSpec) throws NotConfigurableException {
        final DatabasePortObjectSpec inDatabaseSpec = (DatabasePortObjectSpec)inSpec;
        if (inDatabaseSpec != null) {
            final DatabaseQueryConnectionSettings inDatabaseConnection;
            final Collection<String> jars;
            try {
                inDatabaseConnection = inDatabaseSpec.getConnectionSettings(m_credentials);
                jars = getDatabaseDriverJarPaths(inDatabaseConnection);
            } catch (final InvalidSettingsException | IOException e) {
                throw new NotConfigurableException(e.getMessage(), e);
            }
            return kernel -> {
                try {
                    @SuppressWarnings("resource") // Will be closed along with kernel.
                    final Python2KernelBackend backend = castBackendToLegacy(kernel);
                    backend.putSql(m_variableName, inDatabaseConnection, m_credentials, jars);
                } catch (final IOException ex) {
                    NodeLogger.getLogger(DatabasePort.class).debug(ex);
                }
            };
        } else {
            throw new NotConfigurableException("No database connection available.");
        }
    }

    @Override
    public WorkspacePreparer prepareInDialog(final PortObject inObject) throws NotConfigurableException {
        // Nothing more to prepare.
        return null;
    }

    @Override
    public void execute(final PortObject inObject, final PythonKernel kernel, final ExecutionMonitor monitor)
        throws Exception {
        final DatabasePortObject inDatabase = (DatabasePortObject)inObject;
        checkDatabaseConnection(inDatabase.getSpec());
        m_inDatabaseConnection = inDatabase.getConnectionSettings(m_credentials);
        final Collection<String> jars = getDatabaseDriverJarPaths(m_inDatabaseConnection);
        @SuppressWarnings("resource") // Back end will be closed along with kernel.
        final Python2KernelBackend backend = castBackendToLegacy(kernel);
        backend.putSql(m_variableName, m_inDatabaseConnection, m_credentials, jars);
    }

    private static void checkDatabaseConnection(final DatabasePortObjectSpec spec) throws InvalidSettingsException {
        final DatabaseUtility utility = DatabaseUtility.getUtility(spec.getDatabaseIdentifier());
        if (!utility.supportsInsert() && !utility.supportsUpdate()) {
            throw new InvalidSettingsException("Database does not support insert or update.");
        }
    }

    public static Collection<String> getDatabaseDriverJarPaths(final DatabaseQueryConnectionSettings connection)
        throws IOException {
        final Collection<String> jars = new LinkedList<>();
        final DatabaseUtility utility = connection.getUtility();
        final Collection<File> driverFiles =
            utility.getConnectionFactory().getDriverFactory().getDriverFiles(connection);
        for (final File file : driverFiles) {
            final String absolutePath = file.getAbsolutePath();
            jars.add(absolutePath);
        }
        return jars;
    }

    // Output port:

    @Override
    public PortObject execute(final PythonKernel kernel, final ExecutionContext exec) throws Exception {
        @SuppressWarnings("resource") // Back end will be closed along with kernel.
        final DatabaseQueryConnectionSettings outDatabaseConnection = new DatabaseQueryConnectionSettings(
            m_inDatabaseConnection, castBackendToLegacy(kernel).getSql(getVariableName()));
        final DBReader reader =
            new DatabaseUtility(null, null, (DBAggregationFunctionFactory[])null).getReader(outDatabaseConnection);
        return new DatabasePortObject(
            new DatabasePortObjectSpec(reader.getDataTableSpec(m_credentials), outDatabaseConnection));
    }

    // Utility:

    /**
     * Tries to cast the given kernel's back end to the legacy back end which is required by the Python Script (DB)
     * (legacy) node. Throws a descriptive error indicating that the node does not support the new back end, if the cast
     * fails.
     *
     * @param kernel The kernel.
     * @return The casted back end.
     * @throws IllegalStateException If the cast fails.
     */
    @SuppressWarnings("resource") // Back end will be closed along with kernel.
    public static Python2KernelBackend castBackendToLegacy(final PythonKernel kernel) {
        if (!(kernel.getBackend() instanceof Python2KernelBackend)) {
            throw new IllegalStateException("The Python Script (DB) (legacy) node only supports the legacy back end "
                + "(serialization libraries, etc.) of the Python kernel. Please change your settings accordingly.");
        }
        return (Python2KernelBackend)kernel.getBackend();
    }
}
