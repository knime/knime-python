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

/**
 * Pixi-specific implementation of {@link PythonProcessProvider}. Executes Python processes via {@code pixi run python}
 * to ensure proper environment activation and variable setup.
 * <P>
 * This command resolves the pixi binary and constructs a command line that invokes Python through pixi's
 * environment runner, which handles all necessary environment setup automatically.
 *
 * @author Marc Lehner, KNIME GmbH, Zurich, Switzerland
 */
public final class PixiPythonCommand extends AbstractPixiPythonCommand {

    /**
     * Constructs a {@link PythonProcessProvider} that describes a Python process run via pixi in the environment
     * identified by the given pixi.toml manifest file.<br>
     * The validity of the given arguments is not tested.
     *
     * @param pixiTomlPath The path to the pixi.toml manifest file that describes the environment.
     * @param environmentName The name of the environment within the pixi project (e.g., "default").
     */
    public PixiPythonCommand(final Path pixiTomlPath, final String environmentName) {
        super(pixiTomlPath, environmentName);
    }

    /**
     * Constructs a {@link PythonProcessProvider} that describes a Python process run via pixi in the default environment
     * identified by the given pixi.toml manifest file.<br>
     * The validity of the given arguments is not tested.
     *
     * @param pixiTomlPath The path to the pixi.toml manifest file that describes the environment.
     */
    public PixiPythonCommand(final Path pixiTomlPath) {
        super(pixiTomlPath, "default");
    }
}
