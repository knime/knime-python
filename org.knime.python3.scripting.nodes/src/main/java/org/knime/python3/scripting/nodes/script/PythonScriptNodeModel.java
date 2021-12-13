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
package org.knime.python3.scripting.nodes.script;

import java.util.ArrayList;
import java.util.List;

import org.knime.python2.generic.VariableNames;
import org.knime.python2.ports.DataTableInputPort;
import org.knime.python2.ports.DataTableOutputPort;
import org.knime.python2.ports.ImageOutputPort;
import org.knime.python2.ports.InputPort;
import org.knime.python2.ports.OutputPort;
import org.knime.python2.ports.PickledObjectInputPort;
import org.knime.python2.ports.PickledObjectOutputPort;
import org.knime.python3.scripting.nodes.AbstractPythonScriptingNodeModel;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class PythonScriptNodeModel extends AbstractPythonScriptingNodeModel {

    public PythonScriptNodeModel(final InputPort[] inPorts, final OutputPort[] outPorts) {
        super(inPorts, outPorts, createDefaultScript(inPorts, outPorts));
    }

    private static String createDefaultScript(final InputPort[] inPorts, final OutputPort[] outPorts) {
        final var variables = getVariableNames(inPorts, outPorts);
        String defaultScript = getOldNodesDefaultScript(inPorts, outPorts, variables);
        if (defaultScript == null) {
            // No old/known node configuration. Fall back to some generic default code that simply populates all output
            // variables with empty/null values.
            defaultScript = outPorts.length != 0 ? "# These are the node's outputs that need to be populated:\n\n" : "";
            for (int i = 0; i < outPorts.length; i++) {
                final OutputPort outPort = outPorts[i];
                final String outVariableName = outPort.getVariableName();
                if (outPort instanceof DataTableOutputPort) {
                    defaultScript += outVariableName + " = None"; // TODO: create an empty table
                } else if (outPort instanceof ImageOutputPort || outPort instanceof PickledObjectOutputPort) {
                    defaultScript += outVariableName + " = None";
                }
                defaultScript += "\n";
            }
        }
        return "import knime_io as knio\n\n" + defaultScript;
    }

    static VariableNames getVariableNames(final InputPort[] inPorts, final OutputPort[] outPorts) {
        final List<String> inputTables = new ArrayList<>(2);
        final List<String> inputObjects = new ArrayList<>(2);
        for (final InputPort inPort : inPorts) {
            final String variableName = inPort.getVariableName();
            if (inPort instanceof DataTableInputPort) {
                inputTables.add(variableName);
            } else if (inPort instanceof PickledObjectInputPort) {
                inputObjects.add(variableName);
            }
        }
        final List<String> outputTables = new ArrayList<>(2);
        final List<String> outputImages = new ArrayList<>(2);
        final List<String> outputObjects = new ArrayList<>(2);
        for (final OutputPort outPort : outPorts) {
            final String variableName = outPort.getVariableName();
            if (outPort instanceof DataTableOutputPort) {
                outputTables.add(variableName);
            } else if (outPort instanceof ImageOutputPort) {
                outputImages.add(variableName);
            } else if (outPort instanceof PickledObjectOutputPort) {
                outputObjects.add(variableName);
            }
        }
        return new VariableNames("knio.flow_variables", //
            inputTables.toArray(String[]::new), //
            outputTables.toArray(String[]::new), //
            outputImages.toArray(String[]::new), //
            inputObjects.toArray(String[]::new), //
            outputObjects.toArray(String[]::new));
    }

    /**
     * If the ports configuration of this node matches the ports of a non-dynamic Python scripting node, use the default
     * source code of that node.
     */
    private static String getOldNodesDefaultScript( // NOSONAR There is no point in further splitting up this method.
        final InputPort[] inPorts, final OutputPort[] outPorts, final VariableNames variables) {
        final int numInputs = inPorts.length;
        final int numInObjects = variables.getInputObjects().length;
        final int numInTables = variables.getInputTables().length;
        final int numOutputs = outPorts.length;
        final int numOutTables = variables.getOutputTables().length;
        final int numOutImages = variables.getOutputImages().length;
        final int numOutObjects = variables.getOutputObjects().length;

        String sourceCode = null;

        // Python Source node
        if (numInputs == 0 && numOutputs == 1 && numOutTables == 1) {
            sourceCode = getPythonSourceDefaultScript(variables);
        }
        // Python Script (1 -> 1) node
        if (numInputs == 1 && numInTables == 1 && numOutputs == 1 && numOutTables == 1) {
            sourceCode = getPythonScript1To1DefaultScript(variables);
        }
        // Python Script (1 -> 2) node
        if (numInputs == 1 && numInTables == 1 && numOutputs == 2 && numOutTables == 2) {
            sourceCode = getPythonScript1To2DefaultScript(variables);
        }
        // Python Script (2 -> 1) node
        if (numInputs == 2 && numInTables == 2 && numOutputs == 1 && numOutTables == 1) {
            sourceCode = getPythonScript2To1DefaultScript(variables);
        }
        // Python Script (2 -> 2) node
        if (numInputs == 2 && numInTables == 2 && numOutputs == 2 && numOutTables == 2) {
            sourceCode = getPythonScript2To2DefaultScript(variables);
        }
        // Python View node
        if (numInputs == 1 && numInTables == 1 && numOutputs == 1 && numOutImages == 1) {
            sourceCode = getPythonViewDefaultScript(variables);
        }
        // Python Object Reader node
        if (numInputs == 0 && numOutputs == 1 && numOutObjects == 1) {
            // TODO: drag'n'drop support for .pkl files will require injecting the path.
            sourceCode = getPythonObjectReaderDefaultScript(variables, "python_object.pkl");
        }
        // Python Object Writer node
        if (numInputs == 1 && numInObjects == 1 && numOutputs == 0) {
            sourceCode = getPythonObjectWriterDefaultScript(variables);
        }
        // Python Learner node
        if (numInputs == 1 && numInTables == 1 && numOutputs == 1 && numOutObjects == 1) {
            sourceCode = getPythonLearnerDefaultScript(variables);
        }
        // Python Predictor node
        if (numInputs == 2 && numInObjects == 1 && numInTables == 1 && numOutputs == 1 && numOutTables == 1) {
            sourceCode = getPythonPredictorDefaultScript(variables);
        }

        return sourceCode;
    }

    private static String getPythonSourceDefaultScript(final VariableNames variables) {
        return "# This example script creates an output table containing randomly drawn integers using numpy and pandas."
            + "\n\n" //
            + "import numpy as np\n" //
            + "import pandas as pd\n\n" //
            + variables.getOutputTables()[0] + " = knio.write_table(\n" //
            + "    pd.DataFrame(\n" //
            + "        np.random.randint(0, 100, size=(10, 2)), columns=['First column', 'Second column']\n" //
            + "    )\n" //
            + ")\n";
    }

    private static String getPythonScript1To1DefaultScript(final VariableNames variables) {
        return "# This example script simply outputs the node's input table.\n\n" //
            + variables.getOutputTables()[0] + " = knio.write_table(" + variables.getInputTables()[0] + ")\n";
    }

    private static String getPythonScript1To2DefaultScript(final VariableNames variables) {
        return "# This example script simply outputs the node's input table.\n\n" //
            + variables.getOutputTables()[0] + " = knio.write_table(" + variables.getInputTables()[0] + ")\n"
            + variables.getOutputTables()[1] + " = knio.write_table(" + variables.getInputTables()[0] + ")\n";
    }

    private static String getPythonScript2To1DefaultScript(final VariableNames variables) {
        return "# This example script performs an inner join on the node's input tables using pandas.\n\n" //
            + variables.getOutputTables()[0] + " = knio.write_table(\n" //
            + "    " + variables.getInputTables()[0] + "\n" //
            + "    .to_pandas()\n" //
            + "    .join(\n" //
            + "        " + variables.getInputTables()[1] + ".to_pandas(),\n" //
            + "        how='inner',\n" //
            + "        lsuffix=' (left)',\n" //
            + "        rsuffix=' (right)'\n" //
            + "    )\n" //
            + ")\n";
    }

    private static String getPythonScript2To2DefaultScript(final VariableNames variables) {
        return "# This example script simply outputs the node's input tables.\n\n" //
            + variables.getOutputTables()[0] + " = knio.write_table(" + variables.getInputTables()[0] + ")\n"
            + variables.getOutputTables()[1] + " = knio.write_table(" + variables.getInputTables()[1] + ")\n";
    }

    private static String getPythonViewDefaultScript(final VariableNames variables) {
        return "# This example script plots the input table's numeric data using pandas and outputs the plot as SVG."
            + "\n\n" //
            + "from io import BytesIO\n\n" //
            + "# Only use numeric columns\n" //
            + "data = " + variables.getInputTables()[0] + ".to_pandas().select_dtypes('number')\n" //
            + "# Replace row ID by number\n" //
            + "data.index = range(0, len(data))\n" //
            + "# Create buffer to write into\n" //
            + "buffer = BytesIO()\n" //
            + "# Create plot and write it into the buffer\n" //
            + "data.plot().get_figure().savefig(buffer, format='svg')\n" //
            + "# The output is the content of the buffer\n" //
            + variables.getOutputImages()[0] + " = buffer.getvalue()\n";
    }

    private static String getPythonObjectReaderDefaultScript(final VariableNames variables, final String path) {
        final String pathOS = path.replace("/", "' + os.sep + '");
        return "# This example script unpickles and outputs an object from a file in your KNIME workspace.\n\n" //
            + "import os\n" //
            + "import pickle\n\n" //
            + "# The path is <knime-workspace>/" + path + ".\n" //
            + "file_path = os.path.join(" + variables.getFlowVariables() + "['knime.workspace'], '" + pathOS + "')\n\n"
            + "with open(file_path, 'rb') as f:\n" //
            + "    " + variables.getOutputObjects()[0] + " = pickle.load(f)\n";
    }

    private static String getPythonObjectWriterDefaultScript(final VariableNames variables) {
        return "# This example script pickles the input object to a file in your KNIME workspace.\n\n" //
            + "import os\n" //
            + "import pickle\n\n" //
            + "# The path is <knime-workspace>/python_object.pkl.\n" //
            + "file_path = os.path.join(" + variables.getFlowVariables() //
            + "['knime.workspace'], 'python_object.pkl')\n\n" //
            + "with open(file_path, 'wb') as f:\n" //
            + "    pickle.dump(" + variables.getInputObjects()[0] + ", f)\n";
    }

    private static String getPythonLearnerDefaultScript(final VariableNames variables) {
        return "# This example script performs a linear regression on the input table's numeric data using numpy and "
            + "outputs the\n" //
            + "# solution as pickled object.\n\n" //
            + "import numpy as np\n\n" //
            + "data = " + variables.getInputTables()[0] + ".to_pandas().select_dtypes('number')\n\n" //
            + "value_column = data[data.columns[0]]\n" //
            + "target_column = data[data.columns[1]]\n"
            + "A = np.array([np.array(value_column), np.ones(len(value_column))])\n" //
            + variables.getOutputObjects()[0] + " = np.linalg.lstsq(A.T, target_column)[0]\n";
    }

    private static String getPythonPredictorDefaultScript(final VariableNames variables) {
        return "# This example script applies a linear regression to the first numeric column of the input table. For "
            + "this purpose, the\n" //
            + "# input object is assumed to be compatible with the first return value of numpy.linalg.lstsq "
            + "(one-dimensional case).\n\n" //
            + "import pandas as pd\n\n" //
            + "linear_model = " + variables.getInputObjects()[0] + "\n" //
            + "input_table = " + variables.getInputTables()[0] + ".to_pandas()\n" //
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
            + variables.getOutputTables()[0] + " = knio.write_table(output_table)\n";
    }
}
