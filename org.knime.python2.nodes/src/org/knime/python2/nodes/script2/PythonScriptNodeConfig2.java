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
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python2.nodes.script2;

import java.util.ArrayList;
import java.util.List;

import org.knime.python2.config.PythonSourceCodeConfig;
import org.knime.python2.generic.VariableNames;
import org.knime.python2.ports.DataTableInputPort;
import org.knime.python2.ports.DataTableOutputPort;
import org.knime.python2.ports.ImageOutputPort;
import org.knime.python2.ports.InputPort;
import org.knime.python2.ports.OutputPort;
import org.knime.python2.ports.PickledObjectInputPort;
import org.knime.python2.ports.PickledObjectOutputPort;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class PythonScriptNodeConfig2 extends PythonSourceCodeConfig {

    static final String FLOW_VARIABLES_NAME = "flow_variables";

    public static String getDefaultSourceCode(final InputPort[] inPorts, final OutputPort[] outPorts) {
        final VariableNames variables = getVariableNames(inPorts, outPorts);
        String defaultCode = getOldNodesDefaultSourceCode(inPorts, outPorts, variables);
        if (defaultCode == null) {
            // No old/known node configuration. Fall back to some generic default code that simply populates all output
            // variables with empty/null values.
            defaultCode = "";

            // Imports:
            if (variables.getOutputTables().length > 0) {
                defaultCode += "import pandas as pd\n\n";
            }

            // Populate outputs:
            for (int i = 0; i < outPorts.length; i++) {
                final OutputPort outPort = outPorts[i];
                final String outVariableName = outPort.getVariableName();
                if (outPort instanceof DataTableOutputPort) {
                    defaultCode += outVariableName + " = pd.DataFrame()";
                } else if (outPort instanceof ImageOutputPort) {
                    defaultCode += outVariableName + " = None";
                } else if (outPort instanceof PickledObjectOutputPort) {
                    defaultCode += outVariableName + " = None";
                }
                defaultCode += "\n";
            }
        }
        return defaultCode;
    }

    public static VariableNames getVariableNames(final InputPort[] inPorts, final OutputPort[] outPorts) {
        final List<String> inputTables = new ArrayList<>(2);
        final List<String> inputObjects = new ArrayList<>(2);
        final List<String> inputMisc = new ArrayList<>(2);
        for (final InputPort inPort : inPorts) {
            final String variableName = inPort.getVariableName();
            if (inPort instanceof DataTableInputPort) {
                inputTables.add(variableName);
            } else if (inPort instanceof PickledObjectInputPort) {
                inputObjects.add(variableName);
            } else {
                inputMisc.add(variableName);
            }
        }
        final List<String> outputTables = new ArrayList<>(2);
        final List<String> outputImages = new ArrayList<>(2);
        final List<String> outputObjects = new ArrayList<>(2);
        final List<String> outputMisc = new ArrayList<>(2);
        for (final OutputPort outPort : outPorts) {
            final String variableName = outPort.getVariableName();
            if (outPort instanceof DataTableOutputPort) {
                outputTables.add(variableName);
            } else if (outPort instanceof ImageOutputPort) {
                outputImages.add(variableName);
            } else if (outPort instanceof PickledObjectOutputPort) {
                outputObjects.add(variableName);
            } else {
                outputMisc.add(variableName);
            }
        }
        return new VariableNames(FLOW_VARIABLES_NAME, //
            inputTables.toArray(new String[0]), //
            outputTables.toArray(new String[0]), //
            outputImages.toArray(new String[0]), //
            inputObjects.toArray(new String[0]), //
            outputObjects.toArray(new String[0]), //
            inputMisc.toArray(new String[0]), //
            outputMisc.toArray(new String[0]));
    }

    /**
     * If the ports configuration of this node matches the ports of a non-dynamic Python scripting node, use the default
     * source code of that node.
     */
    private static String getOldNodesDefaultSourceCode(final InputPort[] inPorts, final OutputPort[] outPorts,
        final VariableNames variables) {
        final int numInputs = inPorts.length;
        final int numInObjects = variables.getInputObjects().length;
        final int numInTables = variables.getInputTables().length;
        final int numOutputs = outPorts.length;
        final int numOutTables = variables.getOutputTables().length;
        final int numOutImages = variables.getOutputImages().length;
        final int numOutObjects = variables.getOutputObjects().length;

        // Python Source node
        if (numInputs == 0 && numOutputs == 1 && numOutTables == 1) {
            return getPythonSourceDefaultSourceCode(variables);
        }
        // Python Script (1 -> 1) node
        if (numInputs == 1 && numInTables == 1 && numOutputs == 1 && numOutTables == 1) {
            return getPythonScript1To1DefaultSourceCode(variables);
        }
        // Python Script (1 -> 2) node
        if (numInputs == 1 && numInTables == 1 && numOutputs == 2 && numOutTables == 2) {
            return getPythonScript1To2DefaultSourceCode(variables);
        }
        // Python Script (2 -> 1) node
        if (numInputs == 2 && numInTables == 2 && numOutputs == 1 && numOutTables == 1) {
            return getPythonScript2To1DefaultSourceCode(variables);
        }
        // Python Script (2 -> 2) node
        if (numInputs == 2 && numInTables == 2 && numOutputs == 2 && numOutTables == 2) {
            return getPythonScript2To2DefaultSourceCode(variables);
        }
        // Python View node
        if (numInputs == 1 && numInTables == 1 && numOutputs == 1 && numOutImages == 1) {
            return getPythonViewDefaultSourceCode(variables);
        }
        // Python Object Reader node
        if (numInputs == 0 && numOutputs == 1 && numOutObjects == 1) {
            // TODO: drag'n'drop support for .pkl files will require injecting the path.
            return getPythonObjectReaderDefaultSourceCode(variables, "python_object.pkl");
        }
        // Python Object Writer node
        if (numInputs == 1 && numInObjects == 1 && numOutputs == 0) {
            return getPythonObjectWriterDefaultSourceCode(variables);
        }
        // Python Learner node
        if (numInputs == 1 && numInTables == 1 && numOutputs == 1 && numOutObjects == 1) {
            return getPythonLearnerDefaultSourceCode(variables);
        }
        // Python Predictor node
        if (numInputs == 2 && numInObjects == 1 && numInTables == 1 && numOutputs == 1 && numOutTables == 1) {
            return getPythonPredictorDefaultSourceCode(variables);
        }
        return null;
    }

    private static String getPythonSourceDefaultSourceCode(final VariableNames variables) {
        return "import pandas as pd\n\n" //
            + "# Create empty table\n" //
            + variables.getOutputTables()[0] + " = pd.DataFrame()\n";
    }

    private static String getPythonScript1To1DefaultSourceCode(final VariableNames variables) {
        return "# Copy input to output\n" //
            + variables.getOutputTables()[0] + " = " + variables.getInputTables()[0] + ".copy()\n";
    }

    private static String getPythonScript1To2DefaultSourceCode(final VariableNames variables) {
        return "# Copy input to output\n" //
            + variables.getOutputTables()[0] + " = " + variables.getInputTables()[0] + ".copy()\n" //
            + variables.getOutputTables()[1] + " = " + variables.getInputTables()[0] + ".copy()\n";
    }

    private static String getPythonScript2To1DefaultSourceCode(final VariableNames variables) {
        return "# Do pandas inner join\n" //
            + variables.getOutputTables()[0] + " = " + variables.getInputTables()[0] + ".join("
            + variables.getInputTables()[1] + ", how='inner', lsuffix=' (left)', rsuffix=' (right)')\n";
    }

    private static String getPythonScript2To2DefaultSourceCode(final VariableNames variables) {
        return "# Copy input to output\n" //
            + variables.getOutputTables()[0] + " = " + variables.getInputTables()[0] + ".copy()\n" //
            + variables.getOutputTables()[1] + " = " + variables.getInputTables()[1] + ".copy()\n";
    }

    private static String getPythonViewDefaultSourceCode(final VariableNames variables) {
        return "from io import BytesIO\n\n" //
            + "# Only use numeric columns\n" //
            + "data = " + variables.getInputTables()[0] + "._get_numeric_data()\n" //
            + "# Replace row ID by number\n" //
            + "data.index = range(0, len(data))\n" //
            + "# Create buffer to write into\n" //
            + "buffer = BytesIO()\n" //
            + "# Create plot and write it into the buffer\n" //
            + "data.plot().get_figure().savefig(buffer, format='svg')\n" //
            + "# The output is the content of the buffer\n" //
            + variables.getOutputImages()[0] + " = buffer.getvalue()\n";
    }

    private static String getPythonObjectReaderDefaultSourceCode(final VariableNames variables, final String path) {
        final String pathOS = path.replace("/", "' + os.sep + '");
        return "import os\n" //
            + "import pickle\n\n" //
            + "# Path is <workspace>/" + path + "\n" //
            + "path = " + variables.getFlowVariables() + "['knime.workspace'] + os.sep + '" + pathOS + "'\n" //
            + "# Load object from pickle file\n" //
            + variables.getOutputObjects()[0] + " = pickle.load(open(path, 'rb'))\n";
    }

    private static String getPythonObjectWriterDefaultSourceCode(final VariableNames variables) {
        return "import os\n" //
            + "import pickle\n\n" //
            + "# Path is <workspace>/python_object.pkl\n" //
            + "path = " + variables.getFlowVariables() + "['knime.workspace'] + os.sep + 'python_object.pkl'\n" //
            + "# Save object as pickle file\n" //
            + "pickle.dump(" + variables.getInputObjects()[0] + ", open(path, 'wb'), pickle.HIGHEST_PROTOCOL)\n";
    }

    private static String getPythonLearnerDefaultSourceCode(final VariableNames variables) {
        return "import numpy as np\n\n" //
            + "# Only use numeric columns\n" //
            + "data = " + variables.getInputTables()[0] + "._get_numeric_data()\n" //
            + "# Use first column as value column\n" //
            + "value_column = data[data.columns[0]]\n" //
            + "# Use second column as target column\n" //
            + "target_column = data[data.columns[1]]\n" + "A = np.array([np.array(value_column), np.ones(len(value_column))])\n" //
            + "# Calculate linear regression\n" //
            + variables.getOutputObjects()[0] + " = np.linalg.lstsq(A.T, target_column)[0]\n";
    }

    private static String getPythonPredictorDefaultSourceCode(final VariableNames variables) {
        return "import pandas as pd\n\n" //
            + "# Only use numeric columns\n" //
            + "data = " + variables.getInputTables()[0] + "._get_numeric_data()\n" //
            + "# Use first column as value\n" //
            + "value_column = data[data.columns[0]]\n" //
            + "# List of predictions\n" //
            + "predictions = []\n" //
            + "# prediction = value * m + c\n" //
            + "# m is first value in model\n" //
            + "m = " + variables.getInputObjects()[0] + "[0]\n" //
            + "# c is second value in model\n" //
            + "c = " + variables.getInputObjects()[0] + "[1]\n" //
            + "# Iterate over values\n" //
            + "for i in range(len(value_column)):\n" //
            + "\t# Calculate predictions\n" //
            + "\tpredictions.append(value_column[i]*m+c)\n" //
            + "# Copy input table to output table\n" //
            + variables.getOutputTables()[0] + " = " + variables.getInputTables()[0] + ".copy()\n" //
            + "# Append predictions\n" //
            + variables.getOutputTables()[0] + "['prediction'] = pd.Series(predictions, index="
            + variables.getOutputTables()[0] + ".index)\n";
    }
}
