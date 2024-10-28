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

import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonPortObjectSpec;
import org.knime.python3.nodes.ports.converters.PortObjectConversionContext;
import org.knime.python3.types.port.api.convert.PortObjectSpecConversionContext;
import org.knime.python3.types.port.api.ir.PortObjectIntermediateRepresentation;
import org.knime.python3.types.port.api.ir.PortObjectSpecIntermediateRepresentation;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ExtensionPortObjectConverter {

    private final PythonPortObjectConverterRegistry m_converterRegistry;

    public ExtensionPortObjectConverter(final PythonPortObjectConverterRegistry converterRegistry) {
        m_converterRegistry = converterRegistry;
    }

    public boolean canConvertSpecToPython(final PortObjectSpec spec) {
        return m_converterRegistry.getKnimeToPyForSpec(spec.getClass()) != null;
    }

    public boolean canConvertObjToPython(final PortObject object) {
        return m_converterRegistry.getKnimeToPyForObject(object.getClass()) != null;
    }

    public boolean canConvertSpecFromPython(final String specClassName) {
        return m_converterRegistry.getPyToKnimeForSpec(specClassName) != null;
    }

    public boolean canConvertObjFromPython(final String objClassName) {
        return m_converterRegistry.getPyToKnimeForObject(objClassName) != null;
    }

    public KnimeToPySpecContainer convertSpecToPython(final PortObjectSpec spec,
        final PortObjectSpecConversionContext context) {
        var converter = m_converterRegistry.getKnimeToPyForSpec(spec.getClass());
        var transfer = converter.convertSpecToPython(spec, context);
        return new KnimeToPySpecContainer(converter.getPortObjectSpecClass().getName(), transfer);
    }

    public KnimeToPyObjContainer convertObjectToPython(final PortObject portObject,
        final PortObjectConversionContext context) {
        var specContainer = convertSpecToPython(portObject.getSpec(), context);
        var converter = m_converterRegistry.getKnimeToPyForObject(portObject.getClass());
        var transfer = converter.convertPortObjectToPython(portObject, context); // TODO obtain from converter
        return new KnimeToPyObjContainer(converter.getPortObjectClass().getName(), transfer, specContainer);
    }

    public PortObjectSpec convertSpecFromPython(final PyToKnimeSpecContainer spec,
        final PortObjectSpecConversionContext context) {
        var converter = m_converterRegistry.getPyToKnimeForSpec(spec.getJavaClassName());
        return converter.convertSpecFromPython(spec.getTransfer(), context);
    }

    public PortObject convertObjFromPython(final PyToKnimeObjContainer obj, final PortObjectConversionContext context) {
        var converter = m_converterRegistry.getPyToKnimeForObject(obj.getJavaClassName());
        var spec = convertSpecFromPython(obj.getSpecContainer(), context);
        return converter.convertPortObjectFromPython(obj.getTransfer(), spec, context);
    }

    public interface PyToKnimeContainer {
        /**
         * Used as ID to pick the converter that turns the transfer into the actual PortObject(Spec)
         *
         * @return the java class name of the object
         */
        String getJavaClassName();

    }

    public interface PyToKnimeSpecContainer extends PyToKnimeContainer, PythonPortObjectSpec {

        PortObjectSpecIntermediateRepresentation getTransfer();

    }

    public interface PyToKnimeObjContainer extends PyToKnimeContainer, PythonPortObject {

        PortObjectIntermediateRepresentation getTransfer();

        PyToKnimeSpecContainer getSpecContainer();

    }

}
