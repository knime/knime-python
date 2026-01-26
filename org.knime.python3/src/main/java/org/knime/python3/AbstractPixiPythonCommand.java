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
 *   Jan 13, 2026 (Marc Lehner): created
 */
package org.knime.python3;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.knime.conda.envinstall.pixi.PixiBinary;
import org.knime.conda.envinstall.pixi.PixiBinary.PixiBinaryLocationException;
import org.knime.python3.processprovider.PythonProcessProvider;

/**
 * Abstract base class for Python commands that use Pixi environments. Executes Python via {@code pixi run python}
 * to ensure proper environment activation and variable setup.
 * <P>
 * Implementation note: Implementors must provide value-based implementations of {@link #hashCode()},
 * {@link #equals(Object)}, and {@link #toString()}.
 *
 * @author Marc Lehner, KNIME GmbH, Zurich, Switzerland
 */
abstract class AbstractPixiPythonCommand implements PythonProcessProvider {

    private final Path m_pixiTomlPath;

    private final String m_pixiEnvironmentName;

    /**
     * @param pixiTomlPath The path to the pixi.toml manifest file that describes the environment
     * @param environmentName The name of the environment within the pixi project (typically "default")
     */
    protected AbstractPixiPythonCommand(final Path pixiTomlPath, final String environmentName) {
        m_pixiTomlPath = Objects.requireNonNull(pixiTomlPath, "pixiTomlPath must not be null");
        m_pixiEnvironmentName = Objects.requireNonNull(environmentName, "environmentName must not be null");
    }

    /**
     * @param pixiTomlPath The path to the pixi.toml manifest file that describes the environment
     */
    protected AbstractPixiPythonCommand(final Path pixiTomlPath) {
        this(pixiTomlPath, "default");
    }

    @Override
    public ProcessBuilder createProcessBuilder() {
        try {
            final String pixiBinaryPath = PixiBinary.getPixiBinaryPath();
            final List<String> command = new ArrayList<>();
            command.add(pixiBinaryPath);
            command.add("run");
            command.add("--manifest-path");
            command.add(m_pixiTomlPath.toString());
            command.add("--environment");
            command.add(m_pixiEnvironmentName);
            command.add("--no-progress");
            command.add("python");
            return new ProcessBuilder(command);
        } catch (PixiBinaryLocationException ex) {
            throw new IllegalStateException(
                "Could not locate pixi binary. Please ensure the pixi bundle is properly installed.", ex);
        }
    }

    @Override
    public Path getPythonExecutablePath() {
        // Resolve the actual Python executable path within the environment
        // This is used for informational purposes only, not for execution
        final Path projectDir = m_pixiTomlPath.getParent();
        final Path envDir = projectDir.resolve(".pixi").resolve("envs").resolve(m_pixiEnvironmentName);
        final boolean isWindows = System.getProperty("os.name").toLowerCase().contains("win");
        final Path pythonPath = isWindows
            ? envDir.resolve("python.exe")
            : envDir.resolve("bin").resolve("python");

        // Return the path even if it doesn't exist yet - the environment might not be installed
        // The caller is responsible for checking existence if needed
        return pythonPath;
    }

    /**
     * @return The path to the pixi.toml manifest file
     */
    protected Path getPixiTomlPath() {
        return m_pixiTomlPath;
    }

    /**
     * @return The environment name
     */
    protected String getEnvironmentName() {
        return m_pixiEnvironmentName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_pixiTomlPath, m_pixiEnvironmentName);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final AbstractPixiPythonCommand other = (AbstractPixiPythonCommand)obj;
        return Objects.equals(m_pixiTomlPath, other.m_pixiTomlPath)
            && Objects.equals(m_pixiEnvironmentName, other.m_pixiEnvironmentName);
    }

    @Override
    public String toString() {
        return "pixi run --manifest-path " + m_pixiTomlPath + " --environment " + m_pixiEnvironmentName
            + " --no-progress python";
    }
}
