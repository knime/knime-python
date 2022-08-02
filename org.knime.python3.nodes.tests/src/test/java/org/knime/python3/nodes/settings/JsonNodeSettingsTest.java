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
 *   May 30, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.settings;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class JsonNodeSettingsTest {

    private static final String SIMPLE_MODEL_SCHEMA = createSimpleModelSchema().toString();

    private static ObjectNode createSimpleModelSchema() {
        var schema = JsonTestUtils.OM.createObjectNode().put("type", "object");
        schema.putObject("properties")//
        .set("model", JsonTestUtils.createSimpleSchema());
        return schema;
    }

    private static final String SIMPLE_MODEL_JSON = JsonTestUtils.OM.createObjectNode()//
        .set("model", JsonTestUtils.createSimpleJson()).toString();

    private static final String NESTED_MODEL_SCHEMA = createNestedModelSchema().toString();

    private static ObjectNode createNestedModelSchema() {
        var schema = JsonTestUtils.OM.createObjectNode().put("type", "object");
        schema.putObject("properties")//
        .set("model", JsonTestUtils.createNestedSchema());
        return schema;
    }

    private static final String NESTED_MODEL_JSON = JsonTestUtils.OM.createObjectNode()//
            .set("model", JsonTestUtils.createNestedJson()).toString();

    @Test
    public void testSimpleSaveTo() {
        var jsonSettings = new JsonNodeSettings(SIMPLE_MODEL_JSON, SIMPLE_MODEL_SCHEMA, KNIMEConstants.VERSION);
        var nodeSettings = new NodeSettings("test");
        jsonSettings.saveTo(nodeSettings);
        var expected = JsonTestUtils.createSimpleSettings();
        addVersion(expected, KNIMEConstants.VERSION);
        addJsonType(expected);
        assertEquals(expected, nodeSettings);
    }

    @Test
    public void testSimpleLoadFrom() throws Exception {
        var jsonSettings = new JsonNodeSettings(SIMPLE_MODEL_JSON, SIMPLE_MODEL_SCHEMA, KNIMEConstants.VERSION);
        var oldVersion = "4.3.0.qualifier";
        var nodeSettings = JsonTestUtils.createSimpleSettings();
        addVersion(nodeSettings, oldVersion);
        addJsonType(nodeSettings);
        jsonSettings = jsonSettings.createFromSettings(nodeSettings);
        assertEquals(oldVersion, jsonSettings.getCreationVersion());
        assertEquals(SIMPLE_MODEL_JSON, jsonSettings.getParameters());
    }

    @Test
    public void testNestedSaveTo() throws Exception {
        var jsonSettings = new JsonNodeSettings(NESTED_MODEL_JSON, NESTED_MODEL_SCHEMA, KNIMEConstants.VERSION);
        var nodeSettings = new NodeSettings("test");
        jsonSettings.saveTo(nodeSettings);
        var expected = JsonTestUtils.createNestedSettings();
        addVersion(expected, KNIMEConstants.VERSION);
        addJsonType(expected);
        addJsonType(expected.getNodeSettings("outer"));
        addJsonType(expected.getNodeSettings("outer").getNodeSettings("inner"));
        assertEquals(expected, nodeSettings);
    }

    @Test
    public void testNestedLoadFrom() throws Exception {
        var jsonSettings = new JsonNodeSettings(NESTED_MODEL_JSON, NESTED_MODEL_SCHEMA, KNIMEConstants.VERSION);
        var oldVersion = "4.3.0.qualifier";
        var nodeSettings = JsonTestUtils.createNestedSettings();
        addVersion(nodeSettings, oldVersion);
        addJsonType(nodeSettings);
        addJsonType(nodeSettings.getNodeSettings("outer"));
        addJsonType(nodeSettings.getNodeSettings("outer").getNodeSettings("inner"));
        jsonSettings = jsonSettings.createFromSettings(nodeSettings);
        assertEquals(oldVersion, jsonSettings.getCreationVersion());
        assertEquals(NESTED_MODEL_JSON, jsonSettings.getParameters());
    }

    private static void addVersion(final NodeSettingsWO settings, final String version) {
        settings.addString("version" + SettingsModel.CFGKEY_INTERNAL, version);
    }

    private static void addJsonType(final NodeSettings expected) {
        expected.addString("json-type" + SettingsModel.CFGKEY_INTERNAL, "OBJECT");
    }
}
