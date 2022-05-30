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
import org.knime.core.node.NodeSettings;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public class JsonNodeSettingsMapperTest {

    @Test
    public void testPrimitive() throws Exception {
        var values = "{\"int_param\":3,\"double_param\":1.5,\"string_param\":\"foobar\",\"bool_param\":false}";
        var mapper = new JsonNodeSettingsMapper(JsonTestUtils.SIMPLE_SCHEMA);
        var settings = new NodeSettings("test");
        mapper.writeIntoNodeSettings(values, settings);
        var expected = new NodeSettings("test");
        expected.addInt("int_param", 3);
        expected.addDouble("double_param", 1.5);
        expected.addString("string_param", "foobar");
        expected.addBoolean("bool_param", false);
        assertEquals(expected, settings);

        var fromSettings = mapper.toJson(expected);
        assertEquals(values, fromSettings);
    }

    @Test
    public void testNested() throws Exception {
        var mapper = new JsonNodeSettingsMapper(JsonTestUtils.NESTED_SCHEMA);
        var settingsFromJson = new NodeSettings("test");
        mapper.writeIntoNodeSettings(JsonTestUtils.NESTED_JSON, settingsFromJson);
        assertEquals(JsonTestUtils.NESTED_SETTINGS, settingsFromJson);
        var jsonFromSettings = mapper.toJson(JsonTestUtils.NESTED_SETTINGS);
        assertEquals(JsonTestUtils.NESTED_JSON, jsonFromSettings);
    }

    @Test
    public void testColumns() throws Exception {
        var mapper = new JsonNodeSettingsMapper(JsonTestUtils.createColumnsSchema("foo", "bar", "bla").toString());
        var json = JsonTestUtils.createColumnsJson("foo", "bar", "bla").toString();
        var settings = JsonTestUtils.createColumnsSettings("foo", "bar", "bla");
        var settingsFromJson = new NodeSettings("test");
        mapper.writeIntoNodeSettings(json, settingsFromJson);
        assertEquals(settings, settingsFromJson);
        var jsonFromSettings = mapper.toJson(settings);
        assertEquals(json, jsonFromSettings);
    }
}
