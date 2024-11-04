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
import org.knime.python3.types.port.converter.PortObjectSpecConversionContext;
import org.knime.python3.types.port.ir.PortObjectIntermediateRepresentation;
import org.knime.python3.types.port.ir.PortObjectSpecIntermediateRepresentation;

/**
 * The {@link ExtensionPortObjectConverters} delegates all calls to the respective converter in the provided
 * {@link ExtensionPortObjectConverterRegistry}
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ExtensionPortObjectConverters {

    /**
     * Exception that is thrown whenever one of the methods in {@link ExtensionPortObjectConverters} is called with a
     * port object or spec type for which there is no converter registered at the extension point.
     */
    public class NoConverterFoundException extends Exception {

        private static final long serialVersionUID = 1L;

    }

    private final ExtensionPortObjectConverterRegistry m_converterRegistry;

    public ExtensionPortObjectConverters(final ExtensionPortObjectConverterRegistry converterRegistry) {
        m_converterRegistry = converterRegistry;
    }

    public ExtensionPortObjectSpec convertSpecToPython(final PortObjectSpec spec,
        final PortObjectSpecConversionContext context) throws NoConverterFoundException {
        var converter = m_converterRegistry.getKnimeToPyForSpec(spec.getClass());
        if (converter == null) {
            throw new NoConverterFoundException();
        }
        var transfer = converter.encodePortObjectSpec(spec, context);
        return new ExtensionPortObjectSpec(converter.getPortObjectSpecClass().getName(), transfer);
    }

    public ExtensionPortObject convertObjectToPython(final PortObject portObject,
        final PortObjectConversionContext context) throws NoConverterFoundException {
        var converter = m_converterRegistry.getKnimeToPyForObject(portObject.getClass());
        if (converter == null) {
            throw new NoConverterFoundException();
        }
        var transfer = converter.encodePortObject(portObject, context); // TODO obtain from converter
        var specContainer = convertSpecToPython(portObject.getSpec(), context);
        return new ExtensionPortObject(converter.getPortObjectClass().getName(), transfer, specContainer);
    }

    public PortObjectSpec convertSpecFromPython(final PythonExtensionPortObjectSpec spec,
        final PortObjectSpecConversionContext context) throws NoConverterFoundException {
        var converter = m_converterRegistry.getPyToKnimeForSpec(spec.getJavaClassName());
        if (converter == null) {
            throw new NoConverterFoundException();
        }
        return converter.decodePortObjectSpec(spec.getIntermediateRepresentation(), context);
    }

    public PortObject convertObjFromPython(final PythonExtensionPortObject obj,
        final PortObjectConversionContext context) throws NoConverterFoundException {
        var converter = m_converterRegistry.getPyToKnimeForObject(obj.getJavaClassName());
        if (converter == null) {
            throw new NoConverterFoundException();
        }
        var spec = convertSpecFromPython(obj.getSpec(), context);
        return converter.decodePortObject(obj.getIntermediateRepresentation(), spec, context);
    }

    /**
     * These interfaces are implemented on the Python side and provide a java class name to be able to draw the link the
     * Java side.
     */
    private interface PythonExtension {
        /**
         * Used as ID to pick the converter that turns the intermediate representation into the actual PortObject(Spec)
         *
         * @return the java class name of the object
         */
        String getJavaClassName();

    }

    /**
     * A {@link PortObjectSpec} provided by a Python implementation
     */
    public interface PythonExtensionPortObjectSpec extends PythonExtension, PythonPortObjectSpec {

        /**
         * @return The intermediate representation to convert the content of this PortObjectSpec from the Python to the
         *         Java representation
         */
        PortObjectSpecIntermediateRepresentation getIntermediateRepresentation();

    }

    /**
     * A {@link PortObject} provided by a Python implementation
     */
    public interface PythonExtensionPortObject extends PythonExtension, PythonPortObject {

        /**
         * @return The intermediate representation to convert the content of this PortObject from Python to Java
         */
        PortObjectIntermediateRepresentation getIntermediateRepresentation();

        /**
         * @return Return the corresponding {@link PythonExtensionPortObjectSpec}
         */
        PythonExtensionPortObjectSpec getSpec();

    }

}
