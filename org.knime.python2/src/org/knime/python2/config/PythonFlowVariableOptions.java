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
 *   Mar 14, 2019 (marcel): created
 */
package org.knime.python2.config;

import java.util.Map;
import java.util.Optional;

import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtension;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtensions;

/**
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class PythonFlowVariableOptions {

    /**
     * The name of the flow variable that sets the serializer id.
     */
    public static final String PYTHON_SERIALIZATION_LIBRARY_FLOWVARIABLE_NAME = "python_serialization_library";

    private String m_serializerId;

    /**
     * @param flowVariables The available flow variables. {@link #getSerializerId()} returns the value of its
     *            {@link #PYTHON_SERIALIZATION_LIBRARY_FLOWVARIABLE_NAME corresponding flow variable} if it's present in
     *            the argument.
     */
    public PythonFlowVariableOptions(final Map<String, FlowVariable> flowVariables) {
        final FlowVariable serializerIdFlowVariable = flowVariables.get(PYTHON_SERIALIZATION_LIBRARY_FLOWVARIABLE_NAME);
        final String serializerId =
            (serializerIdFlowVariable == null) ? null : serializerIdFlowVariable.getStringValue();
        if (serializerId != null && serializerExists(serializerId)) {
            m_serializerId = serializerId;
        } else if (serializerId != null) {
            throw new IllegalArgumentException("Serialization library '" + serializerId + "' does not exist.");
        } else {
            m_serializerId = null;
        }
    }

    private static boolean serializerExists(final String serializerId) {
        return SerializationLibraryExtensions.getExtensions().stream() //
            .map(SerializationLibraryExtension::getId) //
            .anyMatch(id -> id.equals(serializerId));
    }

    /**
     * @return The serializer id configured by its corresponding flow variable if present.
     */
    public Optional<String> getSerializerId() {
        return Optional.ofNullable(m_serializerId);
    }
}
