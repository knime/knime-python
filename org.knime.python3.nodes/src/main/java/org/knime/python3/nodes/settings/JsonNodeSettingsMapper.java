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
 *   May 27, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.settings;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Maps between JSON and {@link NodeSettings} using a JSON schema.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class JsonNodeSettingsMapper {

    private final List<Converter> m_converters;

    /**
     * Constructor.
     *
     * @param jsonSchema the schema of the mapped JSON
     */
    public JsonNodeSettingsMapper(final String jsonSchema) {
        var mapper = createMapper();
        try {
            var root = mapper.readTree(jsonSchema);
            m_converters = createConverters((ObjectNode)root);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Invalid JSON schema.", ex);
        }
    }

    private static List<Converter> createConverters(final ObjectNode node) {
        var converters = new ArrayList<Converter>();
        var properties = (ObjectNode)node.get("properties");
        for (var fieldEntries = properties.fields(); fieldEntries.hasNext();) {
            var entry = fieldEntries.next();
            var child = entry.getValue();
            var name = entry.getKey();
            if (child.has("type")) {
                var type = child.get("type").asText();
                if ("object".equals(type)) {
                    var childConverters = createConverters((ObjectNode)child);
                    converters.add(new GroupConverter(name, childConverters));
                } else {
                    converters.add(new PrimitiveConverter(name, getPrimitiveConverterType(type)));
                }
            } else if (child.has("oneOf")) {
                converters.add(new PrimitiveConverter(name, PrimitiveConverterType.STRING));
            } else if (child.has("anyOf")) {
                converters.add(new StringArrayConverter(name));
            } else {
                throw new IllegalArgumentException("Encountered unsupported schema: " + node.toPrettyString());
            }
        }
        return converters;
    }

    private static PrimitiveConverterType getPrimitiveConverterType(final String type) {
        if ("integer".equals(type)) {
            return PrimitiveConverterType.INTEGER;
        } else if ("number".equals(type)) {
            return PrimitiveConverterType.DOUBLE;
        } else if ("boolean".equals(type)) {
            return PrimitiveConverterType.BOOLEAN;
        } else if ("string".equals(type)) {
            return PrimitiveConverterType.STRING;
        } else {
            throw new IllegalArgumentException("Unsupported primitive type: " + type);
        }
    }

    private static ObjectMapper createMapper() {
        return new ObjectMapper();
    }

    /**
     * Writes the provided JSON string into the provided NodeSettingsWO object.
     *
     * @param json to write into the settings
     * @param settings to write into
     */
    public void writeIntoNodeSettings(final String json, final NodeSettingsWO settings) {
        try {
            var root = (ObjectNode)createMapper().readTree(json);
            for (var converter : m_converters) {
                converter.addFieldToSettings(root, settings);
            }
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Invalid JSON.", ex);
        }
    }

    /**
     * Converts the provided NodeSettings to JSON.
     *
     * @param settings to convert to JSON
     * @return a JSON string containing the data stored in settings
     * @throws InvalidSettingsException if the settings are invalid e.g. if some settings are missing
     */
    public String toJson(final NodeSettingsRO settings) throws InvalidSettingsException {
        var root = createMapper().createObjectNode();
        for (var converter : m_converters) {
            converter.putFieldFromSettings(settings, root);
        }
        return root.toString();
    }

    private interface Converter {

        void putFieldFromSettings(final NodeSettingsRO settings, final ObjectNode node) throws InvalidSettingsException;

        void addFieldToSettings(final ObjectNode node, final NodeSettingsWO settings);

    }

    private static final class StringArrayConverter implements Converter {

        private final String m_name;

        StringArrayConverter(final String name) {
            m_name = name;
        }

        @Override
        public void putFieldFromSettings(final NodeSettingsRO settings, final ObjectNode node) throws InvalidSettingsException {
            var array = settings.getStringArray(m_name);
            var arrayNode = node.putArray(m_name);
            if (array != null) {
                for (var value : array) {
                    arrayNode.add(value);
                }
            }
        }

        @Override
        public void addFieldToSettings(final ObjectNode node, final NodeSettingsWO settings) {
            var arrayNode = (ArrayNode) node.get(m_name);
            var array = IntStream.range(0, arrayNode.size())//
                    .mapToObj(arrayNode::get)//
                    .map(JsonNode::asText)//
                    .toArray(String[]::new);
            settings.addStringArray(m_name, array);
        }


    }

    private static final class PrimitiveConverter implements Converter {

        private final String m_name;

        private final PrimitiveConverterType m_type;

        PrimitiveConverter(final String name, final PrimitiveConverterType type) {
            m_name = name;
            m_type = type;
        }

        @Override
        public void putFieldFromSettings(final NodeSettingsRO settings, final ObjectNode node)
            throws InvalidSettingsException {
            m_type.mapToJson(m_name, settings, node);
        }

        @Override
        public void addFieldToSettings(final ObjectNode node, final NodeSettingsWO settings) {
            m_type.mapToSettings(m_name, node, settings);

        }

    }

    private interface SettingsToJsonMapper {
        void mapToJson(String name, final NodeSettingsRO settings, final ObjectNode node)
            throws InvalidSettingsException;
    }

    private interface JsonToSettingsMapper {
        void mapToSettings(String name, final ObjectNode node, final NodeSettingsWO settings);
    }

    enum PrimitiveConverterType implements SettingsToJsonMapper, JsonToSettingsMapper {
            STRING(//
                (n, s, j) -> j.put(n, s.getString(n)), //
                (n, j, s) -> s.addString(n, j.get(n).asText())//
            ), INTEGER(//
                (n, s, j) -> j.put(n, s.getInt(n)), //
                (n, j, s) -> s.addInt(n, j.get(n).asInt())//
            ), DOUBLE(//
                (n, s, j) -> j.put(n, s.getDouble(n)), //
                (n, j, s) -> s.addDouble(n, j.get(n).asDouble())//
            ), BOOLEAN(//
                (n, s, j) -> j.put(n, s.getBoolean(n)), //
                (n, j, s) -> s.addBoolean(n, j.get(n).asBoolean())//
            );

        private final SettingsToJsonMapper m_settingsToJsonMapper;

        private final JsonToSettingsMapper m_jsonToSettingsMapper;

        PrimitiveConverterType(final SettingsToJsonMapper settingsToJsonMapper,
            final JsonToSettingsMapper jsonToSettingsMapper) {
            m_settingsToJsonMapper = settingsToJsonMapper;
            m_jsonToSettingsMapper = jsonToSettingsMapper;
        }

        @Override
        public void mapToJson(final String name, final NodeSettingsRO settings, final ObjectNode node)
            throws InvalidSettingsException {
            m_settingsToJsonMapper.mapToJson(name, settings, node);
        }

        @Override
        public void mapToSettings(final String name, final ObjectNode node, final NodeSettingsWO settings) {
            m_jsonToSettingsMapper.mapToSettings(name, node, settings);
        }

    }

    private static final class GroupConverter implements Converter {

        private final String m_name;

        private final List<Converter> m_converters;

        GroupConverter(final String name, final List<Converter> converters) {
            m_name = name;
            m_converters = converters;
        }

        @Override
        public void putFieldFromSettings(final NodeSettingsRO settings, final ObjectNode node)
            throws InvalidSettingsException {
            var subnode = node.putObject(m_name);
            var subsettings = settings.getNodeSettings(m_name);
            for (var converter : m_converters) {
                converter.putFieldFromSettings(subsettings, subnode);
            }
        }

        @Override
        public void addFieldToSettings(final ObjectNode node, final NodeSettingsWO settings) {
            var subsettings = settings.addNodeSettings(m_name);
            var subnode = (ObjectNode)node.get(m_name);
            for (var converter : m_converters) {
                converter.addFieldToSettings(subnode, subsettings);
            }
        }
    }
}
