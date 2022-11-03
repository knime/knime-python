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
 *   Nov 18, 2021 (marcel): created
 */
package org.knime.python3.scripting.nodes.view;

import java.nio.file.Path;

import org.knime.python2.ports.DataTableInputPort;
import org.knime.python2.ports.InputPort;
import org.knime.python2.ports.OutputPort;
import org.knime.python3.scripting.nodes.AbstractPythonScriptingNodeModel;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class PythonViewNodeModel extends AbstractPythonScriptingNodeModel {

    public PythonViewNodeModel(final InputPort[] inPorts, final OutputPort[] outPorts) {
        super(inPorts, outPorts, true, createDefaultScript(inPorts));
    }

    Path getPathToHtml() {
        return getPathToHtmlView()
            .orElseThrow(() -> new IllegalStateException("View is not present. This is an implementation error."));
    }

    private static String createDefaultScript(final InputPort[] inPorts) {
        final String defaultScript;
        if (inPorts.length == 1 && (inPorts[0] instanceof DataTableInputPort)) {
            defaultScript = getPython1TableInputDefaultScript();
        } else {
            defaultScript = getGenericDefaultScript();
        }

        return "import knime.scripting.io as knio\n\n" //
            + defaultScript;
    }

    private static String getPython1TableInputDefaultScript() {
        return "# This example script plots a histogram using matplotlib and assigns it to the output view\n" //
            + "import numpy as np\n" //
            + "import matplotlib.pyplot as plt\n" //
            + "\n" //
            + "# Only use numeric columns\n" //
            + "data = knio.input_tables[0].to_pandas().select_dtypes('number')\n" //
            + "\n" //
            + "# Use the values from the first column\n" //
            + "values = np.array(data[data.columns[0]])\n" //
            + "\n" //
            + "# Plot the histogram\n" //
            + "fig = plt.figure()\n" //
            + "plt.hist(np.array(values), bins=10)\n" //
            + "\n" //
            + "# Assign the figure to the output_view variable\n" //
            + "knio.output_view = knio.view(fig)  # alternative: knio.view_matplotlib()\n";
    }

    private static String getGenericDefaultScript() {
        return "# This example uses an HTML string as the output view\n" //
            + "\n" //
            + "knio.output_view = knio.view(\"\"\"<!DOCTYPE html>\n" //
            + "<html>\n" //
            + "    <body>\n" //
            + "        My HTML view\n" //
            + "    </body>\n" //
            + "</html>\n" //
            + "\"\"\")\n" //
            + "\n" //
            + "# Views can also be created from other objects like bytes containing a PNG or JPEG image,\n" //
            + "# or strings containing an SVG. Also, many objects that can be displayed in jupyter notebooks\n" //
            + "# can be displayed as an output view automatically. See the documentation of knio.view for\n" //
            + "# more information.";
    }
}
