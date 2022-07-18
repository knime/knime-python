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
 *   Jan 31, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.knime.python3.PythonEntryPoint;
import org.knime.python3.PythonGateway;
import org.knime.python3.PythonSourceDirectoryLocator;
import org.knime.python3.nodes.modules.PythonNodesModule;
import org.knime.python3.nodes.proxy.PythonNodeProxy;

/**
 * Tests the communication with Python via Py4J.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class PythonNodeProxyTest {

    private static final String LAUNCHER =
        PythonSourceDirectoryLocator.getPathFor(PythonNodeProxyTest.class, "src/test/python")//
            .resolve("python_node_proxy_test_launcher.py")//
            .toString();

    private PythonNodeProxy m_proxy;

    private PythonGateway<PythonNodeProxyTestEntryPoint> m_gateway;

    @Before
    public void setup() throws IOException, InterruptedException {
        m_gateway = PythonNodeTestUtils.openPythonGateway(PythonNodeProxyTestEntryPoint.class, LAUNCHER,
            PythonNodesModule.values());
        m_proxy = m_gateway.getEntryPoint().getProxy();
    }

    @After
    public void shutdown() throws IOException {
        if (m_gateway != null) {
            m_gateway.close();
        }
    }

    @Test
    @Ignore
    public void testValidateSettingsMissingParameter() {
        var settings = "{\"param2\": \"foo\", \"param1\": 4}";
        var version = "4.6.0"; // version in nightly if org.knime.core is checked out
        var error = m_proxy.validateParameters(settings, version);
        assertEquals("Missing the parameter backwards_compatible_parameter.", error);
    }

    @Test
    @Ignore
    public void testBackwardsCompatibleValidate() {
        var settings = "{\"param2\": \"foo\", \"param1\": 4}";
        var version = "4.5.0";
        var error = m_proxy.validateParameters(settings, version);
        assertNull(error);
    }

    @Test
    @Ignore
    public void testValidateInvalidSettings() {
        // param1 has to be non-negative
        var settings = "{\"param2\": \"foo\", \"param1\": -1}";
        var version = "4.6.0";
        var error = m_proxy.validateParameters(settings, version);
        assertEquals("The value must be non-negative.", error);
    }

    public interface PythonNodeProxyTestEntryPoint extends PythonEntryPoint {

        PythonNodeProxy getProxy();
    }
}
