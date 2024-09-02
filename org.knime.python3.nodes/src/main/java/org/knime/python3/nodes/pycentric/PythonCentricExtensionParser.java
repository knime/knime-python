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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.extension.CategoryExtension;
import org.knime.python3.PythonGatewayUtils;
import org.knime.python3.PythonProcessTerminatedException;
import org.knime.python3.nodes.KnimeNodeBackend;
import org.knime.python3.nodes.PurePythonNodeSetFactory.PythonExtensionParser;
import org.knime.python3.nodes.PyNodeExtension;
import org.knime.python3.nodes.PythonNode;
import org.knime.python3.nodes.PythonNodeGatewayFactory;
import org.knime.python3.nodes.extension.NodeDescriptionBuilder;
import org.knime.python3.nodes.extension.NodeDescriptionBuilder.Tab;
import org.knime.python3.views.FolderViewResources;
import org.knime.python3.views.ViewResources;
import org.yaml.snakeyaml.Yaml;

import com.google.gson.Gson;

import py4j.Py4JException;

/**
 * Parses a Python Extension of the following format:
 *
 * <pre>
 * knime.yml
 * extension.py
 * environment.yml
 * ...
 * </pre>
 *
 * The knime.yml is the entry point pointing to the extension module ({@code extension.py} in this example) as well as
 * an environment definition ({@code environment.yml} in this example). It also declares the id, version and description
 * of the extension. The extension module registers categories and nodes and can also be located in a potentially nested
 * subfolder. Note that the paths for icons are relative to the knime.yml file, not the module they are referenced from.
 * That is, if {@code extension.py} is within a subfolder {@code sub} and an icon {@code icon.png} is located next to
 * it, then the correct path to the icon is {@code sub/icon.png}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PythonCentricExtensionParser implements PythonExtensionParser {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonCentricExtensionParser.class);

    @Override
    public PyNodeExtension parseExtension(final Path path) throws IOException {
        var staticInfo = readStaticInformation(path);
        return retrieveDynamicInformationFromPython(staticInfo);
    }

    private static StaticExtensionInfo readStaticInformation(final Path path) throws IOException {
        var yaml = new Yaml();
        try (var inputStream = Files.newInputStream(path.resolve("knime.yml"))) {
            Map<String, Object> map = yaml.load(inputStream);
            final var name = (String)map.get("name");
            final var group_id = (String)map.get("group_id");
            final var env_name = (String)map.getOrDefault("bundled_env_name", group_id.replace('.', '_') + "_" + name);
            final var version = (String)map.get("version");
            return new StaticExtensionInfo(//
                name, //
                group_id, //
                env_name, //
                (String)map.get("extension_module"), //
                path, //
                version);
        }
    }

    private static PyNodeExtension retrieveDynamicInformationFromPython(final StaticExtensionInfo staticInfo)
        throws IOException {
        var gatewayFactory = new PythonNodeGatewayFactory(staticInfo.m_id, staticInfo.m_environmentName,
            staticInfo.m_version, staticInfo.m_modulePath);
        try (var gateway = gatewayFactory.create();
                var outputConsumer = PythonGatewayUtils.redirectGatewayOutput(gateway, LOGGER::debug, LOGGER::debug)) {
            try {
                return createNodeExtension(gateway.getEntryPoint(), staticInfo, gatewayFactory);
            } catch (Py4JException ex) {
                // TODO(AP-23257) can we give a hint to the user? There could be something wrong with the Python
                // extension but this is mostly relevant to the developer. If the process was killed by the watchdog
                // this is most likely due to the system configuration so we could give a hint to the user about that.
                var terminatedEx = PythonProcessTerminatedException.ifTerminated(gateway, ex);
                if (terminatedEx.isPresent()) {
                    throw new IOException(terminatedEx.get().getMessage(), terminatedEx.get());
                }
                throw new IOException(ex.getMessage(), ex);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Python gateway creation was interrupted.", ex);
        } catch (IOException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IOException(ex.getMessage(), ex);
        }
    }

    private static PyNodeExtension createNodeExtension(final KnimeNodeBackend backend,
        final StaticExtensionInfo staticInfo, final PythonNodeGatewayFactory gatewayFactory) {
        var categoriesJson = backend.retrieveCategoriesAsJson();
        var nodesJson = backend.retrieveNodesAsJson();
        return new FluentPythonNodeExtension(staticInfo.m_id, parseNodes(nodesJson, staticInfo.m_extensionPath),
            parseCategories(categoriesJson, staticInfo.m_extensionPath), gatewayFactory, staticInfo.m_version);
    }

    private static List<CategoryExtension.Builder> parseCategories(final String categoriesJson,
        final Path pathToExtension) {
        JsonCategory[] categories = new Gson().fromJson(categoriesJson, JsonCategory[].class);
        return Stream.of(categories) //
            .map(c -> c.toExtension(pathToExtension)) //
            .collect(Collectors.toUnmodifiableList());
    }

    private static PythonNode[] parseNodes(final String nodesJson, final Path extensionPath) {
        JsonNodeDescription[] nodes = new Gson().fromJson(nodesJson, JsonNodeDescription[].class);
        return Stream.of(nodes)//
            .map(n -> n.toPythonNode(extensionPath))//
            .toArray(PythonNode[]::new);
    }

    @SuppressWarnings("java:S116") // the fields are named this way for JSON deserialization
    private static final class JsonNodeDescription {
        private String id;

        private String name;

        private boolean is_deprecated;

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

        private JsonView[] views;

        PythonNode toPythonNode(final Path modulePath) {
            var descriptionBuilder = createDescriptionBuilder();
            descriptionBuilder.withIcon(modulePath.resolve(icon_path));
            return new PythonNode(id, category, after, descriptionBuilder.build(), input_port_types, output_port_types,
                views.length, is_deprecated, getViewResources(modulePath));
        }

        private NodeDescriptionBuilder createDescriptionBuilder() {
            var builder = new NodeDescriptionBuilder(name, node_type, is_deprecated)//
                .withShortDescription(short_description)//
                .withIntro(full_description);
            consumeIfPresent(tabs, t -> builder.withTab(t.toTab()));
            consumeIfPresent(options, o -> o.enter(builder::withOption));
            consumeIfPresent(input_ports, p -> p.enter(builder::withInputPort));
            consumeIfPresent(output_ports, p -> p.enter(builder::withOutputPort));
            consumeIfPresent(views, v -> v.enter(builder::withView));
            return builder;
        }

        private ViewResources[] getViewResources(final Path modulePath) {
            return Arrays.stream(views) //
                .map(v -> v.static_resources != null
                    ? new FolderViewResources(modulePath.resolve(v.static_resources), v.static_resources, true)
                    : ViewResources.EMPTY_RESOURCES) //
                .toArray(ViewResources[]::new);
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

    @SuppressWarnings("java:S116") // the fields are named this way for JSON deserialization
    private static class JsonView extends JsonDescribed {
        private String static_resources;
    }

    @SuppressWarnings("java:S116") // the fields are named this way for JSON deserialization
    private static class JsonCategory {
        private String path;

        private String level_id;

        private String name;

        private String description;

        private String icon;

        private String after;

        private boolean locked;

        CategoryExtension.Builder toExtension(final Path modulePath) {
            return CategoryExtension.builder(name, level_id) //
                .withPath(path) //
                .withDescription(description) //
                .withIcon(pathToIcon(modulePath)) //
                .withAfter(after) //
                .withLocked(locked);
        }

        private String pathToIcon(final Path pathToExtension) {
            return pathToExtension.resolve(icon).toAbsolutePath().toString();
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

        private String m_environmentName;

        private String m_moduleName;

        private String m_version;

        private Path m_modulePath;

        private Path m_extensionPath;

        StaticExtensionInfo(final String name, final String group_id, final String environmentName,
            final String extensionModule, final Path extensionPath, final String version) {
            m_id = group_id + "." + name;
            m_environmentName = environmentName;
            m_extensionPath = extensionPath;
            m_version = version;

            if (m_version == null || m_version.isBlank()) {
                m_version = "0.0.0";
                LOGGER.warnWithFormat(
                    "Missing extension version in knime.yml for extension '%s'; setting version to '0.0.0'", name);
            }

            var relativeModulePath = Path.of(extensionModule);
            if (relativeModulePath.getParent() == null) {
                // the extension module is top level in the extension folder next to the knime.yml
                m_moduleName = extensionModule;
                m_modulePath = extensionPath.resolve(extensionModule);
            } else {
                // the extension module is in a potentially nested subfolder
                m_modulePath = extensionPath.resolve(relativeModulePath);
                m_moduleName = relativeModulePath.getFileName().toString();
            }
        }

    }
}
