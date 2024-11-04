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
 *   Oct 14, 2024 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortTypeRegistry;
import org.knime.python3.types.port.PortObjectConverterExtensionPoint;
import org.knime.python3.types.port.PythonImplementation;
import org.knime.python3.types.port.PythonPortObjectConverterExtension;
import org.knime.python3.types.port.converter.UntypedDelegatingPortObjectDecoder;
import org.knime.python3.types.port.converter.UntypedDelegatingPortObjectEncoder;

import py4j.Py4JException;

/**
 * This singleton provides methods to register extension port types in a {@link KnimeNodeBackend} and to get all the
 * paths that need to be put on the PYTHONPATH for the Python process to import the modules that contribute these
 * extension port objects.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class PortObjectExtensionPointUtils {

    public static PortObjectExtensionPointUtils getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new PortObjectExtensionPointUtils( //
                PortObjectConverterExtensionPoint.getKnimeToPyConverters(), //
                PortObjectConverterExtensionPoint.getPyToKnimeConverters() //
            );
        }
        return INSTANCE;
    }

    public Stream<Path> getPythonPaths() {
        return Stream.concat(m_knimeToPy.stream(), m_pyToKnime.stream())//
            .map(PythonPortObjectConverterExtension::pythonImplementation)//
            .map(PythonImplementation::parentFolder);
    }

    public void registerPortObjectConverters(final KnimeNodeBackend backend) {
        m_knimeToPy.forEach(catchAndLog(e -> registerKnimeToPyPortObjectConverter(e, backend)));
        m_pyToKnime.forEach(catchAndLog(e -> registerPyToKnimePortObjectConverter(e, backend)));
    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PortObjectExtensionPointUtils.class);

    private final List<PythonPortObjectConverterExtension<UntypedDelegatingPortObjectEncoder>> m_knimeToPy;

    private final List<PythonPortObjectConverterExtension<UntypedDelegatingPortObjectDecoder>> m_pyToKnime;

    private PortObjectExtensionPointUtils(
        final List<PythonPortObjectConverterExtension<UntypedDelegatingPortObjectEncoder>> knimeToPy,
        final List<PythonPortObjectConverterExtension<UntypedDelegatingPortObjectDecoder>> pyToKnime) {
        m_knimeToPy = new ArrayList<>(knimeToPy);
        m_pyToKnime = new ArrayList<>(pyToKnime);
    }

    private static PortObjectExtensionPointUtils INSTANCE;

    private static Consumer<PythonPortObjectConverterExtension<?>>
        catchAndLog(final Consumer<PythonPortObjectConverterExtension<?>> register) {
        return e -> {
            try {
                register.accept(e);
            } catch (Py4JException ex) {
                LOGGER.info(
                    "Failed to register a PythonPortObjectConverter contributed by %s.".formatted(e.contributor()), ex);
            }
        };
    }

    private static void registerKnimeToPyPortObjectConverter(final PythonPortObjectConverterExtension<?> extension,
        final KnimeNodeBackend backend) {
        var converter = extension.converter();
        var pythonImplementation = extension.pythonImplementation();
        var moduleName = pythonImplementation.moduleName();
        var pythonConverter = pythonImplementation.pythonClassName();
        var objClass = converter.getPortObjectClass();
        var objClassName = objClass.getName();
        var specClassName = converter.getPortObjectSpecClass().getName();
        var portTypeName = PortTypeRegistry.getInstance().getPortType(objClass).getName();
        backend.registerKnimeToPyPortObjectConverter(moduleName, pythonConverter, objClassName, specClassName,
            portTypeName);
    }

    private static void registerPyToKnimePortObjectConverter(final PythonPortObjectConverterExtension<?> extension,
        final KnimeNodeBackend backend) {
        var converter = extension.converter();
        var pythonImplementation = extension.pythonImplementation();
        var moduleName = pythonImplementation.moduleName();
        var pythonConverter = pythonImplementation.pythonClassName();
        Class<? extends PortObject> objClass = converter.getPortObjectClass();
        var objClassName = objClass.getName();
        var specClassName = converter.getPortObjectSpecClass().getName();
        var portTypeName = PortTypeRegistry.getInstance().getPortType(objClass).getName();
        backend.registerPyToKnimePortObjectConverter(moduleName, pythonConverter, objClassName, specClassName,
            portTypeName);
    }

}
