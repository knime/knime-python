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
 *   Mar 7, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.pycentric;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.knime.python3.nodes.KnimeNodeBackend;
import org.knime.python3.nodes.PurePythonNodeSetFactory.PythonExtensionParser;
import org.knime.python3.nodes.PyNodeExtension;
import org.knime.python3.nodes.PythonNode;
import org.knime.python3.nodes.PythonNodeGatewayFactory;
import org.knime.python3.nodes.extension.NodeDescriptionBuilder;
import org.knime.python3.nodes.extension.NodeDescriptionBuilder.Tab;
import org.yaml.snakeyaml.Yaml;

import com.google.gson.Gson;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PythonCentricExtensionParser implements PythonExtensionParser {

    @Override
    public PyNodeExtension parseExtension(final Path path) throws IOException {
        var staticInfo = readStaticInformation(path);
        return retrieveDynamicInformationFromPython(path, staticInfo);
    }

    private static StaticExtensionInfo readStaticInformation(final Path path) throws IOException {
        var yaml = new Yaml();
        try (var inputStream = Files.newInputStream(path.resolve("knime.yml"))) {
            Map<String, Object> map = yaml.load(inputStream);
            final var name = (String)map.get("name");
            final var group_id = (String)map.get("group_id");
            final var env_name = group_id.replace('.', '_') + "_" + name;
            return new StaticExtensionInfo(//
                name, //
                group_id, //
                (String)map.get("author"), //
                env_name, //
                (String)map.get("extension_module")//
            );
        }
    }

    private static PyNodeExtension retrieveDynamicInformationFromPython(final Path pathToExtension,
        final StaticExtensionInfo staticInfo) throws IOException {
        try (var gateway =
            PythonNodeGatewayFactory.create(staticInfo.m_id, pathToExtension, staticInfo.m_environmentName)) {
            return createNodeExtension(gateway.getEntryPoint(), staticInfo);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Python gateway creation was interrupted.", ex);
        }
    }

    private static PyNodeExtension createNodeExtension(final KnimeNodeBackend backend,
        final StaticExtensionInfo staticInfo) {
        // TODO should we decide on using only this way of defining PythonNodeExtensions, we should add the extension
        // module to the preloaded modules to take full advantage of a potential process/gateway queue
        var nodesJson = backend.retrieveNodesAsJson(staticInfo.m_extensionModule);
        return new FluentPythonNodeExtension(staticInfo.m_id, "TODO", staticInfo.m_environmentName,
            staticInfo.m_extensionModule, parseNodes(nodesJson));
    }

    private static PythonNode[] parseNodes(final String nodesJson) {
        JsonNodeDescription[] nodes = new Gson().fromJson(nodesJson, JsonNodeDescription[].class);
        return Stream.of(nodes)//
            .map(JsonNodeDescription::toPythonNode)//
            .toArray(PythonNode[]::new);
    }

    @SuppressWarnings("java:S116") // the fields are named this way for JSON deserialization
    private static final class JsonNodeDescription {
        private String id;

        private String name;

        private String node_type;

        private String icon_path;

        private String category;

        private String after;

        private String short_description;

        private String full_description;

        private String[] input_port_types;

        private String[] output_port_types;

        private JsonTab[] tabs;

        private JsonDescribed[] options;

        private JsonDescribed[] input_ports;

        private JsonDescribed[] output_ports;

        private JsonDescribed[] views;

        PythonNode toPythonNode() {
            return new PythonNode(id, category, after, icon_path, createDescriptionBuilder(), name, node_type,
                input_port_types, output_port_types, views.length);
        }

        private NodeDescriptionBuilder createDescriptionBuilder() {
            var builder = new NodeDescriptionBuilder(name, node_type)//
                .withShortDescription(short_description)//
                .withIntro(full_description);
            consumeIfPresent(tabs, t -> builder.withTab(t.toTab()));
            consumeIfPresent(options, o -> o.enter(builder::withOption));
            consumeIfPresent(input_ports, p -> p.enter(builder::withInputPort));
            consumeIfPresent(output_ports, p -> p.enter(builder::withOutputPort));
            consumeIfPresent(views, v -> v.enter(builder::withView));
            return builder;
        }

        private static <T> void consumeIfPresent(final T[] array, final Consumer<T> elementConsumer) {
            if (array != null) {
                for (var element : array) {
                    elementConsumer.accept(element);
                }
            }
        }
    }

    @SuppressWarnings("java:S116") // the fields are named this way for JSON deserialization
    private static class JsonDescribed {
        protected String name;

        protected String description;

        void enter(final BiConsumer<String, String> descriptionConsumer) {
            descriptionConsumer.accept(name, description);
        }
    }

    @SuppressWarnings("java:S116") // the fields are named this way for JSON deserialization
    private static class JsonTab extends JsonDescribed {
        private JsonDescribed[] options;

        Tab toTab() {
            var builder = Tab.builder(name, description);
            for (var option : options) {
                option.enter(builder::withOption);
            }
            return builder.build();
        }
    }

    /**
     * Struct-like POJO that is filled by SnakeYaml. Contains static information on the extension. The critical fields
     * are id, environment_name and extension_module.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    private static final class StaticExtensionInfo {

        private String m_id;

        private String m_author;

        private String m_environmentName;

        private String m_extensionModule;

        StaticExtensionInfo(final String name, final String group_id, final String author, final String environmentName,
            final String extensionModule) {
            m_id = group_id + "." + name;
            m_author = author;
            m_environmentName = environmentName;
            m_extensionModule = extensionModule;
        }
    }
}
