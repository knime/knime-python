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
 *   Aug 12, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.types;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExtension;
import org.eclipse.core.runtime.IExtensionPoint;
import org.eclipse.core.runtime.IExtensionRegistry;
import org.eclipse.core.runtime.Platform;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;

/**
 * Registry for the PythonValueFactory extension point. Only a single PythonValueFactory can be linked to any
 * {@link ValueFactory}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class PythonValueFactoryRegistry {

    private static final String IS_DEFAULT_PYTHON_REPRESENTATION = "isDefaultPythonRepresentation";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonValueFactoryRegistry.class);

    private static final PythonValueFactoryRegistry INSTANCE = new PythonValueFactoryRegistry();

    private static final String EXT_POINT = "org.knime.python3.types.PythonValueFactory";

    private static final String MODULE_PATH = "modulePath";

    private static final String MODULE_NAME = "moduleName";

    private final List<PythonValueFactoryModule> m_modules = new ArrayList<>();

    private PythonValueFactoryRegistry() {
        IExtensionRegistry registry = Platform.getExtensionRegistry();
        IExtensionPoint extPoint = registry.getExtensionPoint(EXT_POINT);
        for (IExtension extension : extPoint.getExtensions()) {
            m_modules.addAll(extractModules(extension));
        }

        verifyFactories(m_modules);
    }

    private static List<PythonValueFactoryModule> extractModules(final IExtension extension) {
        final List<PythonValueFactoryModule> modules = new ArrayList<>();
        for (IConfigurationElement module : extension.getConfigurationElements()) {
            var mod = extractModule(module);
            if (mod != null) {
                modules.add(mod);
            }
        }
        return modules;
    }

    private static PythonValueFactoryModule extractModule(final IConfigurationElement module) {
        final var modulePath = extractModulePath(module);
        final var moduleName = extractModuleName(module);
        if (modulePath == null || moduleName == null) {
            return null;
        }
        final PythonValueFactory[] factories = extractFactories(module);
        return new PythonValueFactoryModule(modulePath, moduleName, factories);
    }

    private static Path extractModulePath(final IConfigurationElement module) {
        final String modulePath = module.getAttribute(MODULE_PATH);
        final String contributor = module.getContributor().getName();
        final var bundle = Platform.getBundle(contributor);
        try {
            final URL moduleUrl = FileLocator.find(bundle, new org.eclipse.core.runtime.Path(modulePath), null);//NOSONAR

            if (moduleUrl == null) {
                LOGGER.coding("Could not find module path '" + modulePath + "' in bundle '" + contributor + "'."
                    + " Please make sure that the extension is configured correctly and that the Python files are included "
                    + "in your build.properties");
                return null;
            }
            final URL moduleFileUrl = FileLocator.toFileURL(moduleUrl);//NOSONAR
            return FileUtil.resolveToPath(moduleFileUrl);
        } catch (IOException | URISyntaxException ex) {
            LOGGER.error(String.format("Can't resolve KnimeArrowExtensionType provided by %s.", contributor), ex);
            return null;
        }
    }

    private static String extractModuleName(final IConfigurationElement module) {
        return module.getAttribute(MODULE_NAME);
    }

    private static PythonValueFactory[] extractFactories(final IConfigurationElement module) {
        final List<PythonValueFactory> factories = new ArrayList<>();
        for (IConfigurationElement factory : module.getChildren("PythonValueFactory")) {
            try {
                factories.add(extractFactory(factory));
            } catch (CoreException ex) {
                LOGGER.error(
                    String.format("An error occurred during registration of a PythonValueFactory provided by '%s'.",
                        module.getContributor().getName()),
                    ex);
            }
        }

        return factories.toArray(PythonValueFactory[]::new);
    }

    /**
     * Check that for each java value factory there is exactly one default type and not just proxy types, and adjust the
     * "isDefault" settings if needed.
     *
     * @param factories The registered {@link PythonValueFactory}s
     */
    private static void verifyFactories(final List<PythonValueFactoryModule> modules) {
        Set<String> javaValueFactoriesWithDefault = getDefaultValueFactories(modules);

        // Check that all non-default types are backed by a default type
        for (var module : modules) {
            for (var f : module) {
                if (f.isDefaultPythonRepresentation()) {
                    continue;
                }
                if (!javaValueFactoriesWithDefault.contains(f.getValueFactoryClassName())) {
                    LOGGER.coding("Found proxy PythonValueFactory (" + f.getPythonValueFactoryName() + ") for "
                        + f.getValueFactoryClassName() + " but no default yet. Using it as default.");
                    f.makeDefaultPythonRepresentation(true);
                    javaValueFactoriesWithDefault.add(f.getValueFactoryClassName());
                }
            }
        }
    }

    private static Set<String> getDefaultValueFactories(final List<PythonValueFactoryModule> modules) {
        Set<String> javaValueFactoriesWithDefault = new HashSet<>();

        // Check for duplicate defaults
        for (var module : modules) {
            for (var f : module) {
                if (!f.isDefaultPythonRepresentation()) {
                    continue;
                }
                if (javaValueFactoriesWithDefault.contains(f.getValueFactoryClassName())) {
                    LOGGER.coding("Found multiple default PythonValueFactories for " + f.getValueFactoryClassName()
                        + ". Using the first found Python value factory as default instead of "
                        + f.getPythonValueFactoryName());
                    f.makeDefaultPythonRepresentation(false);
                } else {
                    javaValueFactoriesWithDefault.add(f.getValueFactoryClassName());
                }
            }
        }
        return javaValueFactoriesWithDefault;
    }

    private static PythonValueFactory extractFactory(final IConfigurationElement factory) throws CoreException {
        final ValueFactory<?, ?> valueFactory = (ValueFactory<?, ?>)factory.createExecutableExtension("ValueFactory");
        final String pythonValueFactoryName = factory.getAttribute("PythonClassName");
        final boolean isDefault = factory.getAttribute(IS_DEFAULT_PYTHON_REPRESENTATION) == null
            || factory.getAttribute(IS_DEFAULT_PYTHON_REPRESENTATION).equalsIgnoreCase("true");
        final String valueTypeName = factory.getAttribute("ValueTypeName");
        return new PythonValueFactory(valueFactory, pythonValueFactoryName, valueTypeName, isDefault);
    }

    /**
     * @return the list of registered {@link PythonValueFactoryModule PythonValueFactoryModules}
     */
    public static List<PythonValueFactoryModule> getModules() {
        return new ArrayList<>(INSTANCE.m_modules);
    }

}
