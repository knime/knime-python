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
 *   2 August 2024 (Ivan Prigarin): created
 */
package org.knime.python3.nodes.ports;

import java.util.HashMap;
import java.util.Map;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;

/**
 *
 * TODO
 *
 */
public final class PythonPortTypeRegistry {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonPortObjectTypeRegistry.class);

    private static class InstanceHolder {
        private static final PythonPortTypeRegistry INSTANCE = new PythonPortTypeRegistry();
    }

    private final Map<Class<? extends PortObject>, PortObjectConverter<? extends PortObject, ? extends PythonPortObject, ? extends PythonPortObject>> m_converterMap;

    private PythonPortTypeRegistry() {
        m_converterMap = new HashMap<>();

        m_converterMap.put(BufferedDataTable.class, new PortObjectConverters.TablePortObjectConverter());
    }


    /**
     * TODO
     */
    public static PythonPortObject convertToPythonPortObject(final PortObject portObject, final ConversionContext context) {
        if (portObject == null) {
            throw new IllegalStateException("Cannot convert null portObject from KNIME to Python");
        }
        var registry = InstanceHolder.INSTANCE;
        PortObjectConverter converter = registry.m_converterMap.get(portObject.getClass());

        if (converter == null) {
            throw new IllegalStateException("No converter found for " + portObject.getClass().getName());
        }

        return converter.toPython(portObject, context);
    }

    /**
     * TODO
     */
    public static void convertFromPythonPortObject(final PythonPortObject pythonPortObject, final ConversionContext context) {
        // nothing here yet.
        // the converter takes over the responsibility of implementing the fromPython method which
        // was previously implemented as part of the PythonPortObject interface implementation
    }
}
