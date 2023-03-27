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
 *   Apr 22, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

import org.knime.conda.envbundling.environment.CondaEnvironmentRegistry;
import org.knime.python3.Activator;
import org.knime.python3.BundledPythonCommand;
import org.knime.python3.FreshPythonGatewayFactory;
import org.knime.python3.Python3SourceDirectory;
import org.knime.python3.PythonCommand;
import org.knime.python3.PythonEntryPointUtils;
import org.knime.python3.PythonGateway;
import org.knime.python3.PythonGatewayFactory;
import org.knime.python3.PythonGatewayFactory.EntryPointCustomizer;
import org.knime.python3.PythonGatewayFactory.PythonGatewayDescription;
import org.knime.python3.arrow.Python3ArrowSourceDirectory;
import org.knime.python3.types.PythonValueFactoryModule;
import org.knime.python3.types.PythonValueFactoryRegistry;
import org.knime.python3.views.Python3ViewsSourceDirectory;

/**
 * Creates {@link PythonGateway PythonGateways} for nodes written purely in Python.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PythonNodeGatewayFactory {

    private static final Path LAUNCHER = PythonNodesSourceDirectory.getPath()//
        .resolve("_node_backend_launcher.py");

    private static final PythonGatewayFactory FACTORY = Activator.GATEWAY_FACTORY;

    private static final PythonGatewayFactory DEBUG_FACTORY = new FreshPythonGatewayFactory();

    private final String m_module;

    private final Path m_modulePath;

    private final String m_extensionId;

    private final String m_environmentName;

    private final String m_extensionVersion;

    /**
     * @param extensionId the extension's id
     * @param environmentName the name of the environment the extension uses
     * @param extensionVersion the version of the extension
     * @param modulePath the absolute path to the module defining the extension
     */
    public PythonNodeGatewayFactory(final String extensionId, final String environmentName,
        final String extensionVersion, final Path modulePath) {
        m_module = modulePath.getFileName().toString();
        m_modulePath = modulePath.getParent();
        m_extensionId = extensionId;
        m_environmentName = environmentName;
        m_extensionVersion = extensionVersion;
    }

    /**
     * Creates a {@link PythonGateway} with the {@link KnimeNodeBackend} entry point.
     *
     * @return a {@link PythonGateway} for the provided extension
     * @throws IOException if creation fails due to I/O problems
     * @throws InterruptedException if the creation is interrupted
     */
    public PythonGateway<KnimeNodeBackend> create()
        throws IOException, InterruptedException {
        var command = createCommand(m_extensionId, m_environmentName);
        var gatewayDescriptionBuilder = PythonGatewayDescription.builder(command, LAUNCHER, KnimeNodeBackend.class)//
            .addToPythonPath(Python3SourceDirectory.getPath())//
            .addToPythonPath(Python3ArrowSourceDirectory.getPath()) //
            .addToPythonPath(Python3ViewsSourceDirectory.getPath())//
            .addToPythonPath(m_modulePath)//
            .withCustomizer(new KnimeNodeBackendCustomizer(m_extensionId, m_module, m_extensionVersion));
        PythonValueFactoryRegistry.getModules().stream().map(PythonValueFactoryModule::getParentDirectory)
            .forEach(gatewayDescriptionBuilder::addToPythonPath);
        // For debugging it is best to always start a new process, so that changes in the code are immediately reflected
        // in the node
        // The factory is not held as member, so that it is possible to toggle debug mode without a restart
        var factory = PythonExtensionPreferences.debugMode(m_extensionId) ? DEBUG_FACTORY : FACTORY;
        var gateway = factory.create(gatewayDescriptionBuilder.build());
        final var backend = gateway.getEntryPoint();
        PythonEntryPointUtils.registerPythonValueFactories(backend);
        return gateway;
    }

    private static PythonCommand createCommand(final String extensionId, final String environmentName) {
        return PythonExtensionPreferences.getCustomPythonCommand(extensionId)//
            .orElseGet(() -> getPythonCommandForEnvironment(environmentName));
    }

    private static PythonCommand getPythonCommandForEnvironment(final String environmentName) {
        final var environment = CondaEnvironmentRegistry.getEnvironment(environmentName);
        return new BundledPythonCommand(environment.getPath().toAbsolutePath().toString());
    }

    private static final class KnimeNodeBackendCustomizer implements EntryPointCustomizer<KnimeNodeBackend> {

        private final String m_extensionId;

        private final String m_extensionModule;

        private final String m_extensionVersion;

        KnimeNodeBackendCustomizer(final String extensionId, final String extensionModule,
            final String extensionVersion) {
            m_extensionId = extensionId;
            m_extensionModule = extensionModule;
            m_extensionVersion = extensionVersion;
        }

        @Override
        public void customize(final KnimeNodeBackend entryPoint) {
            PythonEntryPointUtils.registerPythonValueFactories(entryPoint);
            entryPoint.loadExtension(m_extensionId, m_extensionModule, m_extensionVersion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m_extensionId, m_extensionModule);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            } else if (obj instanceof KnimeNodeBackendCustomizer) {
                var other = (KnimeNodeBackendCustomizer)obj;
                return m_extensionId.equals(other.m_extensionId) //
                    && m_extensionModule.equals(other.m_extensionModule);
            } else {
                return false;
            }
        }

    }

}
