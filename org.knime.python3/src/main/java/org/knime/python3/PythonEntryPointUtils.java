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
 *   Apr 26, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import org.knime.python3.types.PythonValueFactoryModule;
import org.knime.python3.types.PythonValueFactoryRegistry;

import py4j.Py4JException;

/**
 * Contains utility methods to deal with {@link PythonEntryPoint PythonEntryPoints}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PythonEntryPointUtils {

    private PythonEntryPointUtils() {

    }

    /**
     * Registers the PythonValueFactories in the provided {@link PythonEntryPoint}.
     *
     * @param entryPoint to register the PythonValueFactories in
     * @throws Py4JException if the registration fails
     */
    public static void registerPythonValueFactories(final PythonEntryPoint entryPoint) throws Py4JException {
        final List<PythonValueFactoryModule> modules = PythonValueFactoryRegistry.getModules();

        // We iterate twice because the default representations must be registered first
        for (final var module : modules) {
            final var pythonModule = module.getModuleName();
            for (final var factory : module) {
                if (!factory.isDefaultPythonRepresentation()) {
                    continue;
                }
                entryPoint.registerPythonValueFactory(pythonModule, factory.getPythonValueFactoryName(),
                    factory.getValueFactoryDataType(), factory.getDataSpecRepresentation(), factory.getDataTraitsJson(),
                    factory.getValueTypeName(), factory.isDefaultPythonRepresentation());
            }
        }

        // Register proxy types after all other types because they will reference the original value factories
        for (final var module : modules) { // NOSONAR: cannot merge with previous loop
            final var pythonModule = module.getModuleName();
            for (final var factory : module) {
                if (factory.isDefaultPythonRepresentation()) {
                    continue;
                }
                entryPoint.registerPythonValueFactory(pythonModule, factory.getPythonValueFactoryName(),
                    factory.getValueFactoryDataType(), factory.getDataSpecRepresentation(), factory.getDataTraitsJson(),
                    factory.getValueTypeName(), false);
            }
        }

        registerColumnConverters(entryPoint, modules);
    }

    private static void registerColumnConverters(final PythonEntryPoint entryPoint,
        final List<PythonValueFactoryModule> modules) throws Py4JException {
        for (final var module : modules) {
            final var pythonModule = module.getModuleName();
            for (final var toPandasColumnConverter : module.getToPandasColumnConverters()) {
                entryPoint.registerToPandasColumnConverter(pythonModule, toPandasColumnConverter.getPythonClassName(),
                    toPandasColumnConverter.getValueFactory());
            }

            for (final var fromPandasColumnConverter : module.getFromPandasColumnConverters()) {
                entryPoint.registerFromPandasColumnConverter(pythonModule,
                    fromPandasColumnConverter.getPythonClassName(), fromPandasColumnConverter.getValueTypeName());
            }
        }
    }
    /**

     * Serves as python API
     * @return List of Strings of all supported TimeZone Strings in Java
     */

    public static List<String> getSupportedTimeZones(){
        return new ArrayList<>(ZoneId.getAvailableZoneIds());

    }

}
