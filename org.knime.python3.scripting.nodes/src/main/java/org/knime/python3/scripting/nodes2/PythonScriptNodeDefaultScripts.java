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
 *   19 Sep 2023 (chaubold): created
 */
package org.knime.python3.scripting.nodes2;

/**
 * Provide default scripts for the Python Script node based on its {@link PythonScriptPortsConfiguration}.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class PythonScriptNodeDefaultScripts {
    private PythonScriptNodeDefaultScripts() {
    }

    /**
     * @param portsConfiguration
     * @return Default script for the Python node with the given configuration
     */
    public static String getDefaultScript(final PythonScriptPortsConfiguration portsConfiguration) {
        String defaultScript = getDefaultScriptForPortsConfig(portsConfiguration);
        if (defaultScript == null) {
            // No known node configuration. Fall back to some generic default code that simply populates all output
            // variables with empty/null values.
            var numOutPorts = portsConfiguration.getNumOutTables() + portsConfiguration.getNumOutObjects()
                + portsConfiguration.getNumOutImages();
            defaultScript = numOutPorts != 0 ? "# These are the node's outputs that need to be populated:\n\n" : "";

            for (int i = 0; i < portsConfiguration.getNumOutTables(); i++) { // NOSONAR
                defaultScript += CodeAlias.forOutputTable(i) + " = None\n"; // NOSONAR
            }
            for (int i = 0; i < portsConfiguration.getNumOutObjects(); i++) { // NOSONAR
                defaultScript += CodeAlias.forOutputObject(i) + " = None\n"; // NOSONAR
            }
            for (int i = 0; i < portsConfiguration.getNumOutImages(); i++) { // NOSONAR
                defaultScript += CodeAlias.forOutputImage(i) + " = None\n"; // NOSONAR
            }
            if (portsConfiguration.hasView()) {
                defaultScript += CodeAlias.forOutputView() + " = None\n"; // NOSONAR
            }
        }
        return "import knime.scripting.io as knio\n\n" + defaultScript;
    }

    private static class CodeAlias {
        public static String forInputTable(final int index) {
            return "knio.input_tables[" + index + "]";
        }

        public static String forInputObject(final int index) {
            return "knio.input_objects[" + index + "]";
        }

        public static String forOutputTable(final int index) {
            return "knio.output_tables[" + index + "]";
        }

        public static String forOutputObject(final int index) {
            return "knio.output_objects[" + index + "]";
        }

        public static String forOutputImage(final int index) {
            return "knio.output_images[" + index + "]";
        }

        public static String forOutputView() {
            return "knio.output_view"; // NOSONAR not using a constant for consistency
        }

        public static String forFlowVariableDict() {
            return "knio.flow_variables"; // NOSONAR not using a constant for consistency
        }
    }

    /**
     * If the ports configuration of this node matches the ports of a non-dynamic Python scripting node, use the default
     * source code of that node.
     */
    private static String getDefaultScriptForPortsConfig( // NOSONAR There is no point in further splitting up this method.
        final PythonScriptPortsConfiguration portsConfiguration) {
        final int numInObjects = portsConfiguration.getNumInObjects();
        final int numInTables = portsConfiguration.getNumInTables();
        final int numInputs = numInObjects + numInTables;

        final int numOutTables = portsConfiguration.getNumOutTables();
        final int numOutImages = portsConfiguration.getNumOutImages();
        final int numOutObjects = portsConfiguration.getNumOutObjects();
        final int numOutputs = numOutTables + numOutImages + numOutObjects;

        String sourceCode = null;

        if (portsConfiguration.hasView()) {
            if (numInputs == 1 && numInTables == 1) {
                sourceCode = getPythonView1TableInputDefaultScript();
            } else {
                sourceCode = getPythonViewGenericDefaultScript();
            }
        } else {
            // Python Source node
            if (numInputs == 0 && numOutputs == 1 && numOutTables == 1) {
                sourceCode = getPythonSourceDefaultScript();
            }
            // Python Script (1 -> 1) node
            if (numInputs == 1 && numInTables == 1 && numOutputs == 1 && numOutTables == 1) {
                sourceCode = getPythonScript1To1DefaultScript();
            }
            // Python Script (1 -> 2) node
            if (numInputs == 1 && numInTables == 1 && numOutputs == 2 && numOutTables == 2) {
                sourceCode = getPythonScript1To2DefaultScript();
            }
            // Python Script (2 -> 1) node
            if (numInputs == 2 && numInTables == 2 && numOutputs == 1 && numOutTables == 1) {
                sourceCode = getPythonScript2To1DefaultScript();
            }
            // Python Script (2 -> 2) node
            if (numInputs == 2 && numInTables == 2 && numOutputs == 2 && numOutTables == 2) {
                sourceCode = getPythonScript2To2DefaultScript();
            }
            // Python Image OutputPort node
            if (numInputs == 1 && numInTables == 1 && numOutputs == 1 && numOutImages == 1) {
                sourceCode = getPythonImageOutputDefaultScript();
            }
            // Python Object Reader node
            if (numInputs == 0 && numOutputs == 1 && numOutObjects == 1) {
                // TODO: drag'n'drop support for .pkl files will require injecting the path.
                sourceCode = getPythonObjectReaderDefaultScript("python_object.pkl");
            }
            // Python Object Writer node
            if (numInputs == 1 && numInObjects == 1 && numOutputs == 0) {
                sourceCode = getPythonObjectWriterDefaultScript();
            }
            // Python Learner node
            if (numInputs == 1 && numInTables == 1 && numOutputs == 1 && numOutObjects == 1) {
                sourceCode = getPythonLearnerDefaultScript();
            }
            // Python Predictor node
            if (numInputs == 2 && numInObjects == 1 && numInTables == 1 && numOutputs == 1 && numOutTables == 1) {
                sourceCode = getPythonPredictorDefaultScript();
            }
        }

        return sourceCode;
    }

    private static String getPythonSourceDefaultScript() {
        return "# This example script creates an output table containing randomly drawn "
            + "integers using numpy and pandas.\n\n" //
            + "import numpy as np\n" //
            + "import pandas as pd\n\n" //
            + CodeAlias.forOutputTable(0) + " = knio.Table.from_pandas(\n" //
            + "    pd.DataFrame(\n" //
            + "        np.random.randint(0, 100, size=(10, 2)), columns=['First column', 'Second column']\n" //
            + "    )\n" //
            + ")\n";
    }

    private static String getPythonScript1To1DefaultScript() {
        return "# This example script simply outputs the node's input table.\n\n" //
            + CodeAlias.forOutputTable(0) + " = " + CodeAlias.forInputTable(0) + "\n";
    }

    private static String getPythonScript1To2DefaultScript() {
        return "# This example script simply outputs the node's input table.\n\n" //
            + CodeAlias.forOutputTable(0) + " = " + CodeAlias.forInputTable(0) + "\n" + CodeAlias.forOutputTable(1)
            + " = " + CodeAlias.forInputTable(0) + "\n";
    }

    private static String getPythonScript2To1DefaultScript() {
        return "# This example script performs an inner join on the node's input tables using pandas.\n\n" //
            + CodeAlias.forOutputTable(0) + " = knio.Table.from_pandas(\n" //
            + "    " + CodeAlias.forInputTable(0) + "\n" //
            + "    .to_pandas()\n" //
            + "    .join(\n" //
            + "        " + CodeAlias.forInputTable(1) + ".to_pandas(),\n" //
            + "        how='inner',\n" //
            + "        lsuffix=' (left)',\n" //
            + "        rsuffix=' (right)'\n" //
            + "    )\n" //
            + ")\n";
    }

    private static String getPythonScript2To2DefaultScript() {
        return "# This example script simply outputs the node's input tables.\n\n" //
            + CodeAlias.forOutputTable(0) + " = " + CodeAlias.forInputTable(0) + "\n" + CodeAlias.forOutputTable(1)
            + " = " + CodeAlias.forInputTable(1) + "\n";
    }

    private static String getPythonImageOutputDefaultScript() {
        return "# This example script plots the input table's numeric data using pandas and outputs the plot as SVG."
            + "\n\n" //
            + "from io import BytesIO\n\n" //
            + "# Only use numeric columns\n" //
            + "data = " + CodeAlias.forInputTable(0) + ".to_pandas().select_dtypes('number')\n" //
            + "# Replace row ID by number\n" //
            + "data.index = range(0, len(data))\n" //
            + "# Create buffer to write into\n" //
            + "buffer = BytesIO()\n" //
            + "# Create plot and write it into the buffer\n" //
            + "data.plot().get_figure().savefig(buffer, format='svg')\n" //
            + "# The output is the content of the buffer\n" //
            + CodeAlias.forOutputImage(0) + " = buffer.getvalue()\n";
    }

    private static String getPythonObjectReaderDefaultScript(final String path) {
        final String pathOS = path.replace("/", "' + os.sep + '");
        return "# This example script unpickles and outputs an object from a file in your KNIME workspace.\n\n" //
            + "import os\n" //
            + "import pickle\n\n" //
            + "# The path is <knime-workspace>/" + path + ".\n" //
            + "file_path = os.path.join(" + CodeAlias.forFlowVariableDict() + "['knime.workspace'], '" + pathOS
            + "')\n\n" + "with open(file_path, 'rb') as f:\n" //
            + "    " + CodeAlias.forOutputObject(0) + " = pickle.load(f)\n";
    }

    private static String getPythonObjectWriterDefaultScript() {
        return "# This example script pickles the input object to a file in your KNIME workspace.\n\n" //
            + "import os\n" //
            + "import pickle\n\n" //
            + "# The path is <knime-workspace>/python_object.pkl.\n" //
            + "file_path = os.path.join(" + CodeAlias.forFlowVariableDict() //
            + "['knime.workspace'], 'python_object.pkl')\n\n" //
            + "with open(file_path, 'wb') as f:\n" //
            + "    pickle.dump(" + CodeAlias.forInputObject(0) + ", f)\n";
    }

    private static String getPythonLearnerDefaultScript() {
        return "# This example script performs a linear regression on the input table's numeric data using numpy and "
            + "outputs the\n" //
            + "# solution as pickled object.\n" //
            + "import numpy as np\n\n" //
            + "# Only use numeric columns\n" //
            + "data = " + CodeAlias.forInputTable(0) + ".to_pandas().select_dtypes('number')\n\n" //
            + "# Use first column as value column\n" //
            + "value_column = data[data.columns[0]]\n\n" //
            + "# Use second column as target column\n" //
            + "target_column = data[data.columns[1]]\n"
            + "A = np.array([np.array(value_column), np.ones(len(value_column))])\n\n" //
            + "# Calculate linear regression\n" //
            + CodeAlias.forOutputObject(0) + " = np.linalg.lstsq(A.T, target_column)[0]\n";
    }

    private static String getPythonPredictorDefaultScript() {
        return "# This example script applies a linear regression to the first numeric column of the input table. For "
            + "this purpose, the\n" //
            + "# input object is assumed to be compatible with the first return value of numpy.linalg.lstsq "
            + "(one-dimensional case).\n\n" //
            + "import pandas as pd\n\n" //
            + "linear_model = " + CodeAlias.forInputObject(0) + "\n" //
            + "input_table = " + CodeAlias.forInputTable(0) + ".to_pandas()\n" //
            + "data = input_table.select_dtypes('number')\n\n" //
            + "# Linear model: prediction = value * m + c.\n" //
            + "predictions = []\n" //
            + "value_column = data.iloc[:, 0]\n" //
            + "m = linear_model[0]\n" //
            + "c = linear_model[1]\n\n" //
            + "for value in value_column:\n" //
            + "    predictions.append(value * m + c)\n\n" //
            + "# Append the predictions to the original table.\n" //
            + "output_table = input_table\n" //
            + "output_table['prediction'] = pd.Series(predictions, index=output_table.index)\n\n" //
            + CodeAlias.forOutputTable(0) + " = knio.Table.from_pandas(output_table)\n";
    }

    private static String getPythonView1TableInputDefaultScript() {
        return "# This example script plots a histogram using matplotlib and assigns it to the output view\n" //
            + "import numpy as np\n" //
            + "import matplotlib.pyplot as plt\n" //
            + "\n" //
            + "# Only use numeric columns\n" //
            + "data = " + CodeAlias.forInputTable(0) + ".to_pandas().select_dtypes('number')\n" //
            + "\n" //
            + "# Use the values from the first column\n" //
            + "values = np.array(data[data.columns[0]])\n" //
            + "\n" //
            + "# Plot the histogram\n" //
            + "fig = plt.figure()\n" //
            + "plt.hist(np.array(values), bins=10)\n" //
            + "\n" //
            + "# Assign the figure to the output_view variable\n" //
            + CodeAlias.forOutputView() + " = knio.view(fig)  # alternative: knio.view_matplotlib()\n";
    }

    private static String getPythonViewGenericDefaultScript() {
        return "# This example uses an HTML string as the output view\n" //
            + "\n" //
            + CodeAlias.forOutputView() + " = knio.view(\"\"\"<!DOCTYPE html>\n" //
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
