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

import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class JsonTestUtils {

    static final ObjectMapper OM = new ObjectMapper();

    static final String SIMPLE_SCHEMA = createSimpleSchema().toString();

    static ObjectNode createSimpleSchema() {
        var schema = OM.createObjectNode().put("type", "object");
        var properties = schema.putObject("properties");
        properties.putObject("int_param").put("type", "integer");
        properties.putObject("double_param").put("type", "number");
        properties.putObject("string_param").put("type", "string");
        properties.putObject("bool_param").put("type", "boolean");
        return schema;
    }

    static final String SIMPLE_JSON = createSimpleJson().toString();

    static ObjectNode createSimpleJson() {
        return OM.createObjectNode()//
            .put("int_param", 3)//
            .put("double_param", 1.5)//
            .put("string_param", "foobar")//
            .put("bool_param", false);
    }

    static final NodeSettingsRO SIMPLE_SETTINGS = createSimpleSettings();

    static NodeSettings createSimpleSettings() {
        var settings = new NodeSettings("test");
        settings.addInt("int_param", 3);
        settings.addDouble("double_param", 1.5);
        settings.addString("string_param", "foobar");
        settings.addBoolean("bool_param", false);
        return settings;
    }

    static final String NESTED_SCHEMA = createNestedSchema().toString();

    static ObjectNode createNestedSchema() {
        var schema = OM.createObjectNode().put("type", "object");
        var properties = schema.putObject("properties");
        properties.putObject("int_param").put("type", "integer");
        var outer = properties.putObject("outer").put("type", "object")//
            .putObject("properties");
        outer.putObject("double_param").put("type", "number");
        outer.putObject("inner")//
            .put("type", "object")//
            .putObject("properties")//
            .putObject("string_param").put("type", "string");
        return schema;
    }

    static final String NESTED_JSON = createNestedJson().toString();

    static ObjectNode createNestedJson() {
        var root = OM.createObjectNode();
        root.put("int_param", 3)//
            .putObject("outer")//
            .put("double_param", 1.5)//
            .putObject("inner")//
            .put("string_param", "foobar");
        return root;
    }

    static final NodeSettings NESTED_SETTINGS = createNestedSettings();

    static NodeSettings createNestedSettings() {
        var settings = new NodeSettings("test");
        settings.addInt("int_param", 3);
        var outerSettings = settings.addNodeSettings("outer");
        outerSettings.addDouble("double_param", 1.5);
        var innerSettings = outerSettings.addNodeSettings("inner");
        innerSettings.addString("string_param", "foobar");
        return settings;
    }

    static ObjectNode createColumnsSchema(final String... selectableColumns) {
        var schema = OM.createObjectNode().put("type", "object");
        var properties = schema.putObject("properties");
        var singleCol = properties.putObject("single").putArray("oneOf");
        for (var col : selectableColumns) {//
            singleCol.addObject()//
                .put("const", col)//
                .put("title", col);
        }
        var multiCol = properties.putObject("multi").putArray("anyOf");
        for (var col : selectableColumns) {
            multiCol.addObject()//
            .put("const", col)//
            .put("title", col);
        }
        return schema;
    }

    static ObjectNode createColumnsJson(final String single, final String ... multi) {
        var root = OM.createObjectNode().put("single", single);
        var array = root.putArray("multi");
        for (var col : multi) {
            array.add(col);
        }
        return root;
    }

    static NodeSettings createColumnsSettings(final String single, final String ... multi) {
        var settings = new NodeSettings("test");
        settings.addString("single", single);
        settings.addStringArray("multi", multi);
        return settings;
    }

}
