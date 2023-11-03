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
 *   Oct 27, 2023 (benjamin): created
 */
package org.knime.python3.scripting.nodes2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.port.PickledObjectFileStorePortObject;
import org.knime.scripting.editor.WorkflowControl.InputPortInfo;

/**
 * Unit tests for the {@link PythonScriptingInputOutputModelUtils}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class PythonScriptingInputOutputModelUtilsTest {

    private static final DataTableSpec TABLE_SPEC = new DataTableSpec( //
        new DataColumnSpecCreator("int_col", IntCell.TYPE).createSpec(), //
        new DataColumnSpecCreator("double_col", DoubleCell.TYPE).createSpec(), //
        new DataColumnSpecCreator("string_col", StringCell.TYPE).createSpec() //
    );

    @Test
    public void testGetFlowVariableInputs() {
        final var variables = List.of( //
            new FlowVariable("string_var", "foo"), //
            new FlowVariable("double_var", 0.1), //
            new FlowVariable("int_var", 1) //
        );
        final var model = PythonScriptingInputOutputModelUtils.getFlowVariableInputs(variables);

        assertEquals("name", "Flow Variables", model.name());
        assertEquals("codeAlias", "knio.flow_variables", model.codeAlias());
        assertEquals("subItemCodeAliasTemplate", "knio.flow_variables[\"{{subItems.[0]}}\"]",
            model.subItemCodeAliasTemplate());
        assertEquals("requiredImport", "import knime.scripting.io as knio", model.requiredImport());
        assertFalse("multiSelection should be false", model.multiSelection());

        var subItems = model.subItems();
        assertEquals("num flow variables", variables.size(), subItems.length);
        for (int i = 0; i < subItems.length; i++) {
            assertEquals("variable name " + i, variables.get(i).getName(), subItems[i].name());
            assertEquals("variable type" + i, variables.get(i).getVariableType().toString(), subItems[i].type());
        }
    }

    @Test
    public void testGetInputObjects() {
        final var inputPorts = new InputPortInfo[]{ //
            new InputPortInfo(BufferedDataTable.TYPE, TABLE_SPEC), // table 0 - spec available
            new InputPortInfo(PickledObjectFileStorePortObject.TYPE, null), // object 0 - spec ignored
            new InputPortInfo(BufferedDataTable.TYPE, null), // table 1 - spec not available
            new InputPortInfo(BufferedDataTable.TYPE, null), // table 2 - spec not available
            new InputPortInfo(PickledObjectFileStorePortObject.TYPE, null), // object 1 - spec ignored
        };
        final var models = PythonScriptingInputOutputModelUtils.getInputObjects(inputPorts);
        assertEquals("num inputs", inputPorts.length, models.size());

        // table 0 - with columns
        var table0 = models.get(0);
        assertEquals("name", "Input Table 1", table0.name());
        assertEquals("codeAlias", "knio.input_tables[0].to_pandas()", table0.codeAlias());
        assertNotNull("subItemCodeAliasTemplate", table0.subItemCodeAliasTemplate());
        assertEquals("requiredImport", "import knime.scripting.io as knio", table0.requiredImport());
        assertTrue("multiSelection should be true", table0.multiSelection());

        var subItems = table0.subItems();
        assertEquals("num columns", TABLE_SPEC.getNumColumns(), subItems.length);
        for (int i = 0; i < subItems.length; i++) {
            assertEquals("column name" + i, TABLE_SPEC.getColumnNames()[i], subItems[i].name());
            assertEquals("column type " + i, TABLE_SPEC.getColumnSpec(i).getType().toString(), subItems[i].type());
        }

        // object 0
        var object0 = models.get(1);
        assertEquals("name", "Input Object 1", object0.name());
        assertEquals("codeAlias", "knio.input_objects[0]", object0.codeAlias());
        assertEquals("requiredImport", "import knime.scripting.io as knio", object0.requiredImport());
        assertNull("no subitems", object0.subItems());

        // table 1 - no columns
        var table1 = models.get(2);
        assertEquals("name", "Input Table 2", table1.name());
        assertEquals("codeAlias", "knio.input_tables[1].to_pandas()", table1.codeAlias());
        assertEquals("requiredImport", "import knime.scripting.io as knio", table1.requiredImport());
        assertNull("no subitems", table1.subItems());

        // table 2 - no columns
        var table2 = models.get(3);
        assertEquals("name", "Input Table 3", table2.name());
        assertEquals("codeAlias", "knio.input_tables[2].to_pandas()", table2.codeAlias());
        assertEquals("requiredImport", "import knime.scripting.io as knio", table2.requiredImport());
        assertNull("no subitems", table2.subItems());

        // object 1
        var object1 = models.get(4);
        assertEquals("name", "Input Object 2", object1.name());
        assertEquals("codeAlias", "knio.input_objects[1]", object1.codeAlias());
        assertEquals("requiredImport", "import knime.scripting.io as knio", object1.requiredImport());
        assertNull("no subitems", object1.subItems());
    }

    @Test
    public void testGetOutputObjectsNoView() {
        final var outputPorts = new PortType[]{ //
            BufferedDataTable.TYPE, //
            ImagePortObject.TYPE, //
            PickledObjectFileStorePortObject.TYPE, //
            PickledObjectFileStorePortObject.TYPE, //
            ImagePortObject.TYPE, //
            BufferedDataTable.TYPE, //
        };

        var models = PythonScriptingInputOutputModelUtils.getOutputObjects(outputPorts, false);
        assertEquals("num outputs", outputPorts.length, models.size());

        // table 0
        var table0 = models.get(0);
        assertEquals("name", "Output Table 1", table0.name());
        assertEquals("codeAlias", "knio.output_tables[0]", table0.codeAlias());
        assertEquals("requiredImport", "import knime.scripting.io as knio", table0.requiredImport());
        assertNull("no subitems", table0.subItems());

        // image 0
        var image0 = models.get(1);
        assertEquals("name", "Output Image 1", image0.name());
        assertEquals("codeAlias", "knio.output_images[0]", image0.codeAlias());
        assertEquals("requiredImport", "import knime.scripting.io as knio", image0.requiredImport());
        assertNull("no subitems", image0.subItems());

        // object 0
        var object0 = models.get(2);
        assertEquals("name", "Output Object 1", object0.name());
        assertEquals("codeAlias", "knio.output_objects[0]", object0.codeAlias());
        assertEquals("requiredImport", "import knime.scripting.io as knio", object0.requiredImport());
        assertNull("no subitems", object0.subItems());

        // object 1
        var object1 = models.get(3);
        assertEquals("name", "Output Object 2", object1.name());
        assertEquals("codeAlias", "knio.output_objects[1]", object1.codeAlias());
        assertEquals("requiredImport", "import knime.scripting.io as knio", object1.requiredImport());
        assertNull("no subitems", object1.subItems());

        // image 1
        var image1 = models.get(4);
        assertEquals("name", "Output Image 2", image1.name());
        assertEquals("codeAlias", "knio.output_images[1]", image1.codeAlias());
        assertEquals("requiredImport", "import knime.scripting.io as knio", image1.requiredImport());
        assertNull("no subitems", image1.subItems());

        // table 1
        var table1 = models.get(5);
        assertEquals("name", "Output Table 2", table1.name());
        assertEquals("codeAlias", "knio.output_tables[1]", table1.codeAlias());
        assertEquals("requiredImport", "import knime.scripting.io as knio", table1.requiredImport());
        assertNull("no subitems", table1.subItems());
    }

    @Test
    public void testGetOutputObjectsWithView() {
        final var outputPorts = new PortType[]{BufferedDataTable.TYPE};
        var models = PythonScriptingInputOutputModelUtils.getOutputObjects(outputPorts, true);
        assertEquals("num outputs", 2, models.size());

        // table 0
        var table0 = models.get(0);
        assertEquals("name", "Output Table 1", table0.name());

        // view
        var view = models.get(1);
        assertEquals("name", "Output View", view.name());
        assertEquals("codeAlias", "knio.output_view", view.codeAlias());
        assertEquals("requiredImport", "import knime.scripting.io as knio", view.requiredImport());
        assertNull("no subitems", view.subItems());
    }
}
