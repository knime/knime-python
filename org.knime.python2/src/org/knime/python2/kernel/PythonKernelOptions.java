/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 */

package org.knime.python2.kernel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.knime.python2.Activator;
import org.knime.python2.PythonCommand;
import org.knime.python2.PythonModuleSpec;
import org.knime.python2.PythonVersion;
import org.knime.python2.extensions.serializationlibrary.SerializationLibraryExtensions;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibraryFactory;
import org.knime.python2.prefs.PythonPreferences;

/**
 * Options to configure {@link PythonKernel}. Includes {@link SerializationOptions serialization options} and the
 * {@link PythonVersion Python version} that shall be used by the kernel, among other options.
 * <P>
 * Implementation note: This class is intended to be immutable.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class PythonKernelOptions {

    private static final String KERNEL_SCRIPT_RELATIVE_PATH = "py/PythonKernelLauncher.py";

    private final String m_kernelScriptPath =
        Activator.getFile(Activator.PLUGIN_ID, KERNEL_SCRIPT_RELATIVE_PATH).getAbsolutePath();

    private final PythonVersion m_pythonVersion;

    private final PythonCommand m_python2Command;

    private final PythonCommand m_python3Command;

    private final SerializationOptions m_serializationOptions;

    private final Set<PythonModuleSpec> m_additionalRequiredModules;

    private final String m_externalCustomPath;

    /**
     * Default constructor. Consults the {@link PythonPreferences preferences} for the default
     * {@link PythonPreferences#getPythonVersionPreference() Python version},
     * {@link PythonPreferences#getPython2CommandPreference() Python 2 command}, and
     * {@link PythonPreferences#getPython3CommandPreference() Python 3 command} to use.
     */
    public PythonKernelOptions() {
        m_pythonVersion = PythonPreferences.getPythonVersionPreference();
        m_python2Command = PythonPreferences.getPython2CommandPreference();
        m_python3Command = PythonPreferences.getPython3CommandPreference();
        m_serializationOptions = new SerializationOptions();
        m_additionalRequiredModules = Collections.emptySet();
        m_externalCustomPath = "";
    }

    /**
     * @param pythonVersion The Python version to use.
     * @param python2Command The command to start Python 2. May be {@code null} in which case we resort to
     *            {@link PythonPreferences#getPython2CommandPreference()}.
     * @param python3Command The command to start Python 3. May be {@code null} in which case we resort to
     *            {@link PythonPreferences#getPython3CommandPreference()}.
     * @param serializationOptions Configures the data transfer between Java and a Python kernel configured by these
     *            options.
     */
    public PythonKernelOptions(final PythonVersion pythonVersion, final PythonCommand python2Command,
        final PythonCommand python3Command, final SerializationOptions serializationOptions) {
        m_pythonVersion = pythonVersion;
        m_python2Command = python2Command != null ? python2Command : PythonPreferences.getPython2CommandPreference();
        m_python3Command = python3Command != null ? python3Command : PythonPreferences.getPython3CommandPreference();
        m_serializationOptions = serializationOptions;
        m_additionalRequiredModules = Collections.emptySet();
        m_externalCustomPath = "";
    }

    /**
     * @param python2Command The command to start Python 2. May be {@code null} in which case we resort to
     *            {@link PythonPreferences#getPython2CommandPreference()}.
     * @param python3Command The command to start Python 3. May be {@code null} in which case we resort to
     *            {@link PythonPreferences#getPython3CommandPreference()}.
     */
    private PythonKernelOptions(final PythonVersion pythonVersion, final PythonCommand python2Command,
        final PythonCommand python3Command, final SerializationOptions serializationOptions,
        final Set<PythonModuleSpec> additionalRequiredModules, final String externalCustomPath) {
        m_pythonVersion = pythonVersion;
        m_python2Command = python2Command != null ? python2Command : PythonPreferences.getPython2CommandPreference();
        m_python3Command = python3Command != null ? python3Command : PythonPreferences.getPython3CommandPreference();
        m_serializationOptions = serializationOptions;
        m_additionalRequiredModules = additionalRequiredModules;
        m_externalCustomPath = externalCustomPath;
    }

    /**
     * @return The path to the Python kernel launch script.
     */
    public String getKernelScriptPath() {
        return m_kernelScriptPath;
    }

    /**
     * @return {@code true} if a kernel configured by these options shall use Python 3, {@code false} if it shall use
     *         Python 2.
     */
    public boolean getUsePython3() {
        return m_pythonVersion.equals(PythonVersion.PYTHON3);
    }

    /**
     * @return The Python version to be used by the Python kernel.
     */
    public PythonVersion getPythonVersion() {
        return m_pythonVersion;
    }

    /**
     * Returns a copy of this instance for the given Python version. This instance remains unaffected.
     *
     * @param pythonVersion The Python version to be used by the Python kernel.
     * @return A copy of this options instance with the given value set.
     */
    public PythonKernelOptions forPythonVersion(final PythonVersion pythonVersion) {
        return new PythonKernelOptions(pythonVersion, m_python2Command, m_python3Command, m_serializationOptions,
            m_additionalRequiredModules, m_externalCustomPath);
    }

    /**
     * @return The command that starts Python 2 to run the Python kernel.
     */
    public PythonCommand getPython2Command() {
        return m_python2Command;
    }

    /**
     * Returns a copy of this instance for the given Python 2 command. This instance remains unaffected.
     *
     * @param python2Command The Python 2 command to use. May be {@code null} in which case we resort to
     *            {@link PythonPreferences#getPython2CommandPreference()}.
     * @return A copy of this options instance with the given value set.
     */
    public PythonKernelOptions forPython2Command(final PythonCommand python2Command) {
        return new PythonKernelOptions(m_pythonVersion, python2Command, m_python3Command, m_serializationOptions,
            m_additionalRequiredModules, m_externalCustomPath);
    }

    /**
     * @return The command that starts Python 3 to run the Python kernel.
     */
    public PythonCommand getPython3Command() {
        return m_python3Command;
    }

    /**
     * Returns a copy of this instance for the given Python 3 command. This instance remains unaffected.
     *
     * @param python3Command The Python 3 command to use. May be {@code null} in which case we resort to
     *            {@link PythonPreferences#getPython3CommandPreference()}.
     * @return A copy of this options instance with the given value set.
     */
    public PythonKernelOptions forPython3Command(final PythonCommand python3Command) {
        return new PythonKernelOptions(m_pythonVersion, m_python2Command, python3Command, m_serializationOptions,
            m_additionalRequiredModules, m_externalCustomPath);
    }

    /**
     * @return The options that configure the data transfer between Java and the Python kernel.
     */
    public SerializationOptions getSerializationOptions() {
        return m_serializationOptions;
    }

    /**
     * Returns a copy of this instance for the given serialization options. This instance remains unaffected.
     *
     * @param serializationOptions Configures the data transfer between Java and a Python kernel configured by these
     *            options.
     * @return A copy of this options instance with the given value set.
     */
    public PythonKernelOptions forSerializationOptions(final SerializationOptions serializationOptions) {
        return new PythonKernelOptions(m_pythonVersion, m_python2Command, m_python3Command, serializationOptions,
            m_additionalRequiredModules, m_externalCustomPath);
    }

    /**
     * @return A set of all additional required modules which are checked on Python kernel startup. Includes the
     *         required packages of the {@link SerializationOptions#getSerializerId() serializer} configured by
     *         {@link PythonKernelOptions#getSerializationOptions() these options}. This should be considered if the
     *         list returned by this method is being passed to another options instance that configures a different
     *         serializer. In such cases, it is recommended to call
     *         {@link #forSerializationOptions(SerializationOptions)} on this instance instead, specifying the other
     *         serializer.
     *
     */
    public Set<PythonModuleSpec> getAdditionalRequiredModules() {
        final SerializationLibraryFactory serializerFactory =
            SerializationLibraryExtensions.getSerializationLibraryFactory(m_serializationOptions.getSerializerId());
        final Set<PythonModuleSpec> allRequiredModules = new HashSet<>(m_additionalRequiredModules);
        allRequiredModules.addAll(serializerFactory.getRequiredExternalModules());
        return allRequiredModules;
    }

    /**
     * Returns a copy of this instance with the given modules <em>added</em> to its set of additional required modules.
     * This instance remains unaffected.
     *
     * @param moduleNames The names of the modules to add to the modules to check for.
     * @return A copy of this options instance with added required modules.
     */
    public PythonKernelOptions forAddedAdditionalRequiredModuleNames(final Collection<String> moduleNames) {
        final Collection<PythonModuleSpec> modules = new ArrayList<>(moduleNames.size());
        for (final String moduleName : moduleNames) {
            modules.add(new PythonModuleSpec(moduleName));
        }
        return forAddedAdditionalRequiredModules(modules);
    }

    /**
     * Returns a copy of this instance with the given modules <em>added</em> to its set of additional required modules.
     * This instance remains unaffected.
     *
     * @param modules The modules to add to the additional modules to check for.
     * @return A copy of this options instance with added required modules.
     */
    public PythonKernelOptions forAddedAdditionalRequiredModules(final Collection<PythonModuleSpec> modules) {
        final Set<PythonModuleSpec> additionalRequiredModules = new HashSet<>(m_additionalRequiredModules);
        additionalRequiredModules.addAll(modules);
        return new PythonKernelOptions(m_pythonVersion, m_python2Command, m_python3Command, m_serializationOptions,
            additionalRequiredModules, m_externalCustomPath);
    }

    /**
     * @return The external custom path that is meant to be appended to the Python kernel's PYTHONPATH. If is not
     *         configured, an empty string is returned.
     */
    public String getExternalCustomPath() {
        return m_externalCustomPath;
    }

    /**
     * Returns a copy of this instance for the given external custom path. This instance remains unaffected.
     *
     * @param externalCustomPath The external custom path to set. It will be appended to the Python kernel's PYTHONPATH.
     * @return A copy of this options instance with the given value set.
     */
    public PythonKernelOptions forExternalCustomPath(final String externalCustomPath) {
        return new PythonKernelOptions(m_pythonVersion, m_python2Command, m_python3Command, m_serializationOptions,
            m_additionalRequiredModules, externalCustomPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_kernelScriptPath, m_pythonVersion, m_python2Command, m_python3Command,
            m_serializationOptions, m_additionalRequiredModules, m_externalCustomPath);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final PythonKernelOptions other = (PythonKernelOptions)obj;
        final EqualsBuilder b = new EqualsBuilder();
        b.append(m_kernelScriptPath, other.m_kernelScriptPath);
        b.append(m_pythonVersion, other.m_pythonVersion);
        b.append(m_python2Command, other.m_python2Command);
        b.append(m_python3Command, other.m_python3Command);
        b.append(m_serializationOptions, other.m_serializationOptions);
        b.append(m_additionalRequiredModules, other.m_additionalRequiredModules);
        b.append(m_externalCustomPath, other.m_externalCustomPath);
        return b.isEquals();
    }
}
