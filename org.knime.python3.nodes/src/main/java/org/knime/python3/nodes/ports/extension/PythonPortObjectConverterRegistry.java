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
 *   Sep 4, 2024 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.ports.extension;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;
import org.knime.python3.types.port.framework.UntypedKnimeToPyPortObjectConverter;
import org.knime.python3.types.port.framework.UntypedPyToKnimePortObjectConverter;
import org.knime.python3.types.port.framework.UntypedPythonPortObjectConverter;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
// TODO move to org.knime.python3.nodes where it is used
public final class PythonPortObjectConverterRegistry {

    private final ClassHierarchyMap<PortObject, UntypedKnimeToPyPortObjectConverter> m_knimeToPyByPoClass =
        new ClassHierarchyMap<>(PortObject.class);

    private final ClassHierarchyMap<PortObjectSpec, UntypedKnimeToPyPortObjectConverter> m_knimeToPyBySpecClass =
        new ClassHierarchyMap<>(PortObjectSpec.class);

    private final Map<String, UntypedKnimeToPyPortObjectConverter> m_knimeToPyByPoClassName = new HashMap<>();

    private final Map<String, UntypedPyToKnimePortObjectConverter> m_pyToKnimeByPoClass = new HashMap<>();

    private final Map<String, UntypedPyToKnimePortObjectConverter> m_pyToKnimeBySpecClass = new HashMap<>();

    public PythonPortObjectConverterRegistry(final Stream<UntypedKnimeToPyPortObjectConverter> knimeToPy,
        final Stream<UntypedPyToKnimePortObjectConverter> pyToKnime) {
        knimeToPy.forEach(this::registerKnimeToPy);
        pyToKnime.forEach(this::registerPyToKnime);
    }

    private void registerKnimeToPy(final UntypedKnimeToPyPortObjectConverter knimeToPy) {
        m_knimeToPyByPoClass.put(knimeToPy.getPortObjectClass(), knimeToPy);
        m_knimeToPyBySpecClass.put(knimeToPy.getPortObjectSpecClass(), knimeToPy);
        m_knimeToPyByPoClassName.put(knimeToPy.getPortObjectClass().getName(), knimeToPy);
    }

    private void registerPyToKnime(final UntypedPyToKnimePortObjectConverter pyToKnime) {
        m_pyToKnimeByPoClass.put(pyToKnime.getPortObjectClass().getName(), pyToKnime);
        m_pyToKnimeBySpecClass.put(pyToKnime.getPortObjectSpecClass().getName(), pyToKnime);
    }

    public UntypedKnimeToPyPortObjectConverter
        getKnimeToPyForObject(final Class<? extends PortObject> portObjectClass) {
        return m_knimeToPyByPoClass.getHierarchyAware(portObjectClass);
    }

    public UntypedKnimeToPyPortObjectConverter
        getKnimeToPyForSpec(final Class<? extends PortObjectSpec> portObjectSpecClass) {
        return m_knimeToPyBySpecClass.getHierarchyAware(portObjectSpecClass);
    }

    public UntypedPyToKnimePortObjectConverter getPyToKnimeForObject(final String objJavaClassName) {
        return m_pyToKnimeByPoClass.get(objJavaClassName);
    }

    public UntypedPyToKnimePortObjectConverter getPyToKnimeForSpec(final String specJavaClassName) {
        return m_pyToKnimeBySpecClass.get(specJavaClassName);
    }

    // TODO return optional?
    public PortType getPortType(final String portObjectClassName) {
        var knimeToPy = m_knimeToPyByPoClassName.get(portObjectClassName);
        if (knimeToPy != null) {
            return getPortType(knimeToPy);
        }
        var pyToKnime = m_pyToKnimeByPoClass.get(portObjectClassName);
        if (pyToKnime != null) {
            return getPortType(pyToKnime);
        }
        return null;
    }

    private static PortType getPortType(final UntypedPythonPortObjectConverter converter) {
        return PortTypeRegistry.getInstance().getPortType(converter.getPortObjectClass());
    }

}
