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
 *   Jul 21, 2022 (benjamin): created
 */
package org.knime.python3.scripting.nodes2;

import java.io.IOException;
import java.util.List;

import org.knime.core.webui.node.dialog.scripting.lsp.LanguageServerProxy;
import org.knime.python3.scripting.nodes.prefs.Python3ScriptingPreferences;

import com.google.gson.Gson;

/**
 * Utility for the LSP Server implementation for Python.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
final class PythonLanguageServer {

    private static final Gson GSON = new Gson();

    private static final String LSP_CONFIG = """
            {
              "pylsp": {
                "configurationSources": [],
                "plugins": {
                  "preload": {
                    "enabled": true,
                    "modules": ["numpy", "pandas"]
                  },
                  "jedi": {
                    "environment": %s,
                    "extra_paths": %s
                  },
                  "pycodestyle": {
                    "enabled": false
                  },
                  "mccabe": {
                    "enabled": false
                  },
                  "autopep8": {
                    "enabled": false
                  },
                  "yapf": {
                    "enabled": false
                  }
                }
              }
            }""";

    /**
     * System property to replace the default command for the language server. The default command uses
     * "python-lsp-server" from the bundled scripting environment.
     */
    private static final String LSP_SERVER_COMMAND_PROPERTY = System.getProperty("knime.python.lsp.command");

    /**
     * System property to replace the default config for the language server. The config is formated with the path to
     * the Python executable and a comma separated list of folders on the PYTHON_PATH.
     */
    private static final String LSP_CONFIG_PROPERTY = System.getProperty("knime.python.lsp.config", LSP_CONFIG);

    private PythonLanguageServer() {
        // Utility class
    }

    /**
     * Create a new {@link LanguageServerProxy} that is connected to a "python-lsp-server" process started from the
     * bundled Python environment.
     *
     * @throws IOException if an I/O error occurs starting the process
     */
    static LanguageServerProxy startLanguageServer() throws IOException {
        if (LSP_SERVER_COMMAND_PROPERTY != null) {
            // NB: We do not respect parameters in quotes because if would be too complicated for a debug property
            return new LanguageServerProxy(new ProcessBuilder(LSP_SERVER_COMMAND_PROPERTY.split(" ")));
        }
        final ProcessBuilder pylspProcessBuilder;
        try {
            pylspProcessBuilder = Python3ScriptingPreferences.getBundledPythonCommand().createProcessBuilder();
        } catch (final IllegalStateException ex) {
            // Thrown if no bundled environment is available
            throw new IOException(ex);
        }
        pylspProcessBuilder.command().addAll(List.of("-m", "pylsp"));
        return new LanguageServerProxy(pylspProcessBuilder);
    }

    /**
     * @return the configuration for the LSP server as a JSON string
     */
    static String getConfig(final String executablePath, final List<String> extraPaths) {
        return LSP_CONFIG_PROPERTY.formatted(GSON.toJson(executablePath), GSON.toJson(extraPaths));
    }
}
