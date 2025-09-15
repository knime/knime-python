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
package org.knime.python3.nodes;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.extension.CategoryExtension;
import org.knime.python3.PythonGatewayUtils;
import org.knime.python3.PythonProcessTerminatedException;
import org.knime.python3.nodes.extension.ExtensionNode.ExtensionNodeView;
import org.knime.python3.nodes.extension.ExtensionNodeSetFactory.PortSpecifier;
import org.knime.python3.nodes.extension.NodeDescriptionBuilder;
import org.knime.python3.nodes.extension.NodeDescriptionBuilder.Tab;
import org.osgi.framework.Version;

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
 * As parsing an extension needs to start a Python process and performing all the imports there, it can take a while. To
 * speed up collecting the extensions, we cache the information of the nodes in the configuration area specific to this
 * installation. The cached info is keyed by extension name and date, so it will be re-built whenever the extension is
 * updated. To make sure we also capture new nodes that might become available because optional dependencies are
 * installed, a {@link PythonExtensionInfoCacheCleaner} is registered during bundle activation and kicks in during
 * install processes.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public final class PythonExtensionParser {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonExtensionParser.class);

    /** Private constructor as this classes API consists of a single public static method */
    private PythonExtensionParser() {
    }

    /**
     * Parses the extension found at the provided path.
     *
     * @param path to the extension
     * @param bundleVersion the version of the bundle providing the extension
     * @return the parsed extension
     * @throws IOException if parsing failed
     */
    public static PythonNodeExtension parseExtension(final Path path, final Version bundleVersion) throws IOException {
        var knimeYaml = KnimeYaml.fromDirectory(path);
        return retrieveDynamicInformationFromPython(knimeYaml, bundleVersion);
    }

    /**
     * Clear the cached file for a specific extension
     *
     * @param path to the extension
     * @param bundleVersion the version of the bundle providing the extension
     */
    public static void clearCache(final Path path, final Version bundleVersion) {
        try {
            var knimeYaml = KnimeYaml.fromDirectory(path);
            var cachePath = getExtensionCachePath(knimeYaml, bundleVersion);
            cachePath.toFile().delete();
        } catch (IOException ex) {
            LOGGER.warn("Could not remove extension cache for extension at " + path, ex);
        }
    }

    private static PythonNodeExtension retrieveDynamicInformationFromPython(final KnimeYaml knimeYaml,
        final Version bundleVersion) throws IOException {
        var gatewayFactory = new PythonNodeGatewayFactory(knimeYaml.getId(), knimeYaml.bundledEnvName(),
            knimeYaml.version(), knimeYaml.getModulePath());

        Path cachePath = getExtensionCachePath(knimeYaml, bundleVersion);

        var extension = loadCachedExtension(knimeYaml, gatewayFactory, cachePath, bundleVersion);
        if (extension != null) {
            return extension;
        }

        try (var gateway = gatewayFactory.create();
                var outputConsumer = PythonGatewayUtils.redirectGatewayOutput(gateway, LOGGER::debug, LOGGER::debug)) {
            try {
                var backend = gateway.getEntryPoint();
                var categoriesJson = backend.retrieveCategoriesAsJson();
                var nodesJson = backend.retrieveNodesAsJson();
                cacheExtension(cachePath, knimeYaml, categoriesJson, nodesJson, bundleVersion);

                return createNodeExtension(categoriesJson, nodesJson, knimeYaml, gatewayFactory);
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

    /**
     * Extension info is cached in the Eclipse configuration area, keyed by {id}_{version}.{qualifier} so that plugin
     * updates don't use old cached info.
     *
     * If the extension has a custom source code path set, we don't want to load cached info because new nodes could
     * have been added in the source.
     *
     * @param knimeYaml The info from the knime.yml of the extension
     * @param bundleVersion the version of the bundle providing the extension (null if not from bundle)
     * @return the path where extension info is or should be cached, or null if the extension is not (and should not be)
     *         cached
     */
    private static Path getExtensionCachePath(final KnimeYaml knimeYaml, final Version bundleVersion) {
        var id = knimeYaml.getId();
        if (PythonExtensionPreferences.hasCustomSrcPath(id)) {
            return null;
        }

        Path cachePath = null;

        if (bundleVersion != null) {

            try {
                // The config area path can contain spaces and umlauts. These need to be URL encoded to be able to create
                // a URI from it. And we need a URI so we can get a File and from that a Path. This seems to be the safest
                // way to do it so it works across operating systems.
                var configAreaURL = Platform.getConfigurationLocation().getURL();
                String encodedURL = URLEncoder.encode(configAreaURL.toString(), StandardCharsets.UTF_8)
                    .replace("+", "%20").replace("%2F", "/") // Preserve slashes in the path
                    .replace("%3A", ":"); // Preserve colons
                var configAreaURI = new URI(encodedURL);
                cachePath = new File(configAreaURI).toPath().resolve(id);
            } catch (NullPointerException | URISyntaxException ex) { // NOSONAR
                LOGGER.debug("Could not find configuration area path to cache " + id + "_" + bundleVersion, ex);
            }
        } else {
            LOGGER.debug("Not caching extension " + id + " info because it doesn't have a bundle version.");
        }

        return cachePath;
    }

    private static void cacheExtension(final Path cachePath, final KnimeYaml extensionInfo, final String categoriesJson,
        final String nodesJson, final Version bundleVersion) throws IOException {
        if (cachePath == null || bundleVersion == null) {
            return;
        }

        // we have plain strings of JSON encoded categories and nodes, so we construct the string for a JSON by hand.
        StringBuilder sb = new StringBuilder();
        sb.append("{ \"bundleVersion\": \"");
        sb.append(bundleVersion);
        sb.append("\",\n\"categories\": ");
        sb.append(categoriesJson);
        sb.append(",\n\"nodes\": ");
        sb.append(nodesJson);
        sb.append("\n}");

        try {
            Files.writeString(cachePath, sb.toString());
            LOGGER.info("Saved extension '" + extensionInfo.getId() + "_" + bundleVersion + "' cache to " + cachePath);
        } catch (IOException e) {
            LOGGER.debug(
                "Could not write extension '" + extensionInfo.getId() + "_" + bundleVersion + "' cache to " + cachePath,
                e);
        }

    }

    private static PythonNodeExtension loadCachedExtension(final KnimeYaml knimeYaml,
        final PythonNodeGatewayFactory gatewayFactory, final Path cachePath, final Version expectedVersion) {

        if (cachePath != null) {
            try (var cachedExtensionReader = Files.newBufferedReader(cachePath)) {
                LOGGER.info("Trying to load cached extension '" + knimeYaml.getId() + "_" + expectedVersion + "' from "
                    + cachePath);

                JsonExtension cachedExt = new Gson().fromJson(cachedExtensionReader, JsonExtension.class);

                if (!cachedExt.bundleVersion.equals(expectedVersion.toString())) {
                    throw new IOException("Extension cache has wrong version, expected " + expectedVersion
                        + " but found " + cachedExt.bundleVersion);
                }

                return new PythonNodeExtension(knimeYaml.getId(), //
                    parseNodes(cachedExt.nodes, knimeYaml.extensionPath()), //
                    parseCategories(cachedExt.categories, knimeYaml.extensionPath()), //
                    gatewayFactory, //
                    knimeYaml.version());

            } catch (IOException e) { // NOSONAR we're not re-throwing this exception because we handle it directly
                LOGGER.debug("Didn't find cached info for extension '" + knimeYaml.getId() + "_" + expectedVersion
                    + "'. Parsing Python extension instead.");
            }
        }

        return null;
    }

    private static PythonNodeExtension createNodeExtension(final String categoriesJson, final String nodesJson,
        final KnimeYaml knimeYaml, final PythonNodeGatewayFactory gatewayFactory) {
        return new PythonNodeExtension( //
            knimeYaml.getId(), //
            parseNodes(nodesJson, knimeYaml.extensionPath()), //
            parseCategories(categoriesJson, knimeYaml.extensionPath()), //
            gatewayFactory, //
            knimeYaml.version() //
        );
    }

    private static List<CategoryExtension.Builder> parseCategories(final String categoriesJson,
        final Path pathToExtension) {
        JsonCategory[] categories = new Gson().fromJson(categoriesJson, JsonCategory[].class);
        return parseCategories(categories, pathToExtension);
    }

    private static List<CategoryExtension.Builder> parseCategories(final JsonCategory[] categories,
        final Path pathToExtension) {
        return Stream.of(categories) //
            .map(c -> c.toExtension(pathToExtension)) //
            .collect(Collectors.toUnmodifiableList());
    }

    private static PythonNode[] parseNodes(final String nodesJson, final Path extensionPath) {
        JsonNodeDescription[] nodes = new Gson().fromJson(nodesJson, JsonNodeDescription[].class);
        return parseNodes(nodes, extensionPath);
    }

    private static PythonNode[] parseNodes(final JsonNodeDescription[] nodes, final Path extensionPath) {
        return Stream.of(nodes)//
            .map(n -> n.toPythonNode(extensionPath))//
            .toArray(PythonNode[]::new);
    }

    @SuppressWarnings("java:S116") // the fields are named this way for JSON deserialization
    private static final class JsonNodeDescription {
        private String id;

        private String name;

        private boolean is_deprecated;

        private boolean is_hidden;

        private String node_type;

        private String icon_path;

        private String category;

        private String after;

        private String[] keywords;

        private String short_description;

        private String full_description;

        private JsonPort[] input_port_specifier;

        private JsonPort[] output_port_specifier;

        private JsonTab[] tabs;

        private JsonDescribed[] options;

        private JsonView[] views;

        PythonNode toPythonNode(final Path modulePath) {

            var descriptionBuilder = createDescriptionBuilder();
            descriptionBuilder.withIcon(modulePath.resolve(icon_path));

            List<PortSpecifier> inputPortSpecifiers = Arrays.stream(input_port_specifier) //
                .map(JsonPort::toPortSpecifier) //
                .collect(Collectors.toList());

            List<PortSpecifier> outputPortSpecifiers = Arrays.stream(output_port_specifier) //
                .map(JsonPort::toPortSpecifier) //
                .collect(Collectors.toList());

            return new PythonNode(id, category, after, keywords, descriptionBuilder.build(), views.length,
                is_deprecated, is_hidden, getExtensionNodeViews(modulePath), inputPortSpecifiers, outputPortSpecifiers);
        }

        private NodeDescriptionBuilder createDescriptionBuilder() {
            var builder = new NodeDescriptionBuilder(name, node_type, is_deprecated)//
                .withShortDescription(short_description)//
                .withIntro(full_description)//
                .withKeywords(keywords);

            consumeIfPresent(tabs, t -> builder.withTab(t.toTab()));
            consumeIfPresent(options, o -> builder.withOption(o.name, o.description));

            consumeIfPresent(input_port_specifier, port -> {
                if (port.group || port.optional) {
                    builder.withDynamicInputPorts(port.name, port.description, port.description_index,
                        port.type_string);
                } else {
                    builder.withInputPort(port.name, port.description, port.description_index);
                }
            });

            consumeIfPresent(output_port_specifier, port -> {
                if (port.group) {
                    builder.withDynamicOutputPorts(port.name, port.description, port.description_index,
                        port.type_string);
                } else {
                    builder.withOutputPort(port.name, port.description, port.description_index);
                }
            });

            consumeIfPresent(views, v -> builder.withView(v.name, v.description));
            return builder;
        }

        private ExtensionNodeView[] getExtensionNodeViews(final Path modulePath) {
            return Arrays.stream(views) //
                .map(v -> new ExtensionNodeView(modulePath, v.static_resources, v.index_html_path)) //
                .toArray(ExtensionNodeView[]::new);
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

    }

    @SuppressWarnings("java:S116") // the fields are named this way for JSON deserialization
    private static class JsonPort extends JsonDescribed {
        protected String type_string;

        protected boolean group;

        protected int defaults;

        protected int description_index;

        protected boolean optional;

        PortSpecifier toPortSpecifier() {
            return new PortSpecifier(name, type_string, description, group, defaults, description_index, optional);
        }
    }

    @SuppressWarnings("java:S116") // the fields are named this way for JSON deserialization
    private static class JsonTab extends JsonDescribed {
        private JsonDescribed[] options;

        Tab toTab() {
            var builder = Tab.builder(name, description);
            for (var option : options) {
                builder.withOption(option.name, option.description);
            }
            return builder.build();
        }
    }

    @SuppressWarnings("java:S116") // the fields are named this way for JSON deserialization
    private static class JsonView extends JsonDescribed {
        private String static_resources;

        private String index_html_path;
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

    @SuppressWarnings("java:S116") // the fields are named this way for JSON deserialization
    private static class JsonExtension {
        protected JsonCategory[] categories;

        protected JsonNodeDescription[] nodes;

        protected String bundleVersion;
    }
}
