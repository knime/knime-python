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
 *   Oct 26, 2022 (benjamin): created
 */
package org.knime.python3.scripting.nodes;

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

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class VariableNamesUtils {

    private VariableNamesUtils() {
    }

    /**
     * Get the variable names for the given input and output ports.
     *
     * @param inPorts
     * @param outPorts
     * @return the variable names
     */
    public static VariableNames getVariableNames(final InputPort[] inPorts, final OutputPort[] outPorts) {
        return getVariableNames(inPorts, outPorts, null, null);
    }

    /**
     * Get the variable names for the given input and output ports.
     *
     * @param inPorts
     * @param outPorts
     * @param generalInputs
     * @param generalOutputs
     * @return the variable names
     */
    public static VariableNames getVariableNames(final InputPort[] inPorts, final OutputPort[] outPorts,
        final String[] generalInputs, final String[] generalOutputs) {
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
            outputObjects.toArray(String[]::new), //
            generalInputs, //
            generalOutputs //
        );
    }
}
