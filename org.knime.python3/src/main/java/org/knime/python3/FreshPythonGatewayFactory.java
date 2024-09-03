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
package org.knime.python3;

import java.io.IOException;
import java.util.Map;

import org.knime.core.node.NodeLogger;

import py4j.Py4JException;

/**
 * Always creates a fresh {@link PythonGateway}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class FreshPythonGatewayFactory implements PythonGatewayFactory {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(FreshPythonGatewayFactory.class);

    @Override
    @SuppressWarnings("resource")
    public <E extends PythonEntryPoint> PythonGateway<E> create(final PythonGatewayDescription<E> description)
        throws IOException, InterruptedException {
        var launcherPath = description.getLauncherPath().toAbsolutePath().toString();
        if (description.getCommand() instanceof CondaPythonCommand
            || description.getCommand() instanceof BundledPythonCommand) {
            PythonGatewayCreationGate.INSTANCE.awaitPythonGatewayCreationAllowedInterruptibly();
        }
        var processBuilder = description.getCommand().createProcessBuilder();
        addCertificates(processBuilder.environment());
        var gateway = DefaultPythonGateway.create(processBuilder, launcherPath,
            description.getEntryPointClass(), description.getExtensions(), description.getPythonPath());
        if (!description.getCustomizers().isEmpty()) {
            var entryPoint = gateway.getEntryPoint();
            try (var customizationOutputConsumer =
                PythonGatewayUtils.redirectGatewayOutput(gateway, LOGGER::debug, LOGGER::debug)) {
                for (var customizer : description.getCustomizers()) {
                    customizer.customize(entryPoint);
                }
            } catch (Py4JException ex) {
                gateway.close();
                PythonProcessTerminatedException.throwIfTerminated(gateway, ex);
                throw new IOException(ex.getMessage(), ex);
            } catch (Exception ex) {
                gateway.close();
                throw new IllegalStateException("Customization of entry point failed.", ex);
            }
        }
        return PythonGatewayTracker.INSTANCE.createTrackedGateway(gateway);
    }

    private static void addCertificates(final Map<String, String> environment) {
        var caCertMode = PythonCaCertsMode.fromProperty();
        LOGGER.debug("Using CA cert mode " + caCertMode);
        caCertMode.updateEnvironment(environment);
    }



}
