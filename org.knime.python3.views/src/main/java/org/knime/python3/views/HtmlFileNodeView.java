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
 *   May 4, 2022 (benjamin): created
 */
package org.knime.python3.views;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.interactive.ReExecutable;
import org.knime.core.node.workflow.NodeMessage;
import org.knime.core.webui.data.ApplyDataService;
import org.knime.core.webui.data.DisposeDataServicesOnNodeStateChange;
import org.knime.core.webui.data.InitialDataService;
import org.knime.core.webui.data.RpcDataService;
import org.knime.core.webui.node.view.NodeTableView;
import org.knime.core.webui.node.view.NodeView;
import org.knime.core.webui.page.Page;

/**
 * A {@link NodeView} that renders a static HTML document located on disk. The HTML file can be enriched with additional
 * resources ({@link ViewResources}) and can optionally communicate with the backend via JSON‑RPC.
 * <p>
 * Instances have to be created through the {@link #builder()} which guides callers through the mandatory and optional
 * configuration steps.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // webui node views are still restricted API
public final class HtmlFileNodeView implements NodeTableView, DisposeDataServicesOnNodeStateChange {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(HtmlFileNodeView.class);

    private final Supplier<Path> m_htmlSupplier;

    private final String m_relativeHTMLPath;

    private final ViewResources m_resources;

    private final Supplier<JsonRpcRequestHandler> m_dataServiceSupplier;

    private final BooleanSupplier m_canBeUsedInReport;

    private final ReExecutable<String> m_reExecutable;

    private final Supplier<String> m_initialDataSupplier;

    /**
     * Starts construction of an {@link HtmlFileNodeView}. The returned builder enforces that an
     * {@link HtmlFileNodeViewBuilderRequiresHtmlSupplier#htmlSupplier(Supplier) html supplier} is provided before the
     * view can be {@link HtmlFileNodeViewBuilder#build() built}.
     *
     * @return the first stage of the fluent builder API
     */
    public static HtmlFileNodeViewBuilderRequiresHtmlSupplier builder() {
        return new Builder();
    }

    private HtmlFileNodeView(final Supplier<Path> htmlSupplier, final String relativeHTMLPath,
        final ViewResources resources, final Supplier<JsonRpcRequestHandler> dataServiceSupplier,
        final BooleanSupplier canBeUsedInReport, final ReExecutable<String> reExecutable,
        final Supplier<String> initialDataSupplier) {
        m_htmlSupplier = htmlSupplier;
        m_relativeHTMLPath = relativeHTMLPath;
        m_resources = resources;
        m_dataServiceSupplier = dataServiceSupplier;
        m_canBeUsedInReport = canBeUsedInReport;
        m_reExecutable = reExecutable;
        m_initialDataSupplier = initialDataSupplier;
    }

    /**
     * Creates a view that shows the HTML document that is saved at the given location.
     *
     * @param htmlSupplier A supplier that provides the path to the HTML file that should be shown currently. The file
     *            must exist and must be readable.
     * @deprecated Use {@link #builder()} instead.
     */
    @Deprecated
    public HtmlFileNodeView(final Supplier<Path> htmlSupplier) {
        this(htmlSupplier, ViewResources.EMPTY_RESOURCES);
    }

    /**
     * Creates a view that shows the HTML document that is saved at the given location.
     *
     * @param htmlSupplier A supplier that provides the path to the HTML file that should be shown currently. The file
     *            must exist and must be readable.
     * @param resources resources that are available to the page.
     * @deprecated Use {@link #builder()} instead.
     */
    @Deprecated
    public HtmlFileNodeView(final Supplier<Path> htmlSupplier, final ViewResources resources) {
        this(htmlSupplier, "index.html", resources, null);
    }

    /**
     * Creates a view that shows the HTML document that is saved at the given location and uses a data service to
     * communicate with the backend.
     *
     * @param htmlSupplier supplier that provides the path to the HTML file that should be shown currently. The file
     *            must exist and must be readable.
     * @param relativeHTMLPath the relative path to the HTML file, used to resolve relative links in the HTML file.
     * @param resources resources that are available to the page.
     * @param dataServiceSupplier supplier that provides a JSON‑RPC request handler that can be used to handle requests
     *            from the HTML
     * @deprecated Use {@link #builder()} instead.
     */
    @Deprecated
    public HtmlFileNodeView(final Supplier<Path> htmlSupplier, final String relativeHTMLPath,
        final ViewResources resources, final Supplier<JsonRpcRequestHandler> dataServiceSupplier) {
        this(htmlSupplier, relativeHTMLPath, resources, dataServiceSupplier, () -> false, null, null);
    }

    /**
     * Interface for handling JSON‑RPC requests coming from the HTML view.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public interface JsonRpcRequestHandler extends AutoCloseable {

        /**
         * Handles a JSON‑RPC request and produces the corresponding response.
         *
         * @param jsonRpcRequest the raw JSON‑RPC request payload
         * @return the JSON‑RPC response as a String
         */
        String handleRequest(String jsonRpcRequest);

    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<InitialDataService<?>> createInitialDataService() {
        if (m_initialDataSupplier == null) {
            return Optional.empty();
        }
        return Optional.of(InitialDataService.builder(m_initialDataSupplier).build());
    }

    @Override
    public Optional<RpcDataService> createRpcDataService() {
        if (m_dataServiceSupplier == null) {
            return Optional.empty();
        }
        var dataService = m_dataServiceSupplier.get();
        return Optional.of(RpcDataService.builder(new JsonRpcWildcardHandler(dataService)).onDeactivate(() -> {
            try {
                dataService.close();
            } catch (Exception ex) {
                LOGGER.error("Failed to close JSON RPC request handler.", ex);
            }
        }).build());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<ApplyDataService<?>> createApplyDataService() {
        if (m_reExecutable == null) {
            return Optional.empty();
        }
        return Optional.of(ApplyDataService.builder(m_reExecutable).build());
    }

    @Override
    public Page getPage() {
        var pb = Page.create().fromString(this::openPage, StandardCharsets.UTF_8).relativePath(m_relativeHTMLPath);
        m_resources.addToPage(pb);
        return pb;
    }

    /** Open the HTML page */
    private InputStream openPage() {
        try {
            return Files.newInputStream(m_htmlSupplier.get());
        } catch (final IOException e) {
            // FIXME: UIEXT-635
            // Do not catch the IOException but propagate it to the framework. Currently, this is not possible, and
            // the IllegalStateException is not logged. Therefore, we log the error ourselves.
            LOGGER.error("Failed to open view file.", e);

            // We require the file to exist and be readable
            // If this is not the case we ended up in an illegal state
            throw new IllegalStateException("Failed to open view file.", e);
        }
    }

    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        // Nothing to do
    }

    @Override
    public void loadValidatedSettingsFrom(final NodeSettingsRO settings) {
        // Nothing to do
    }

    @Override
    public int getPortIndex() {
        // TODO(AP-22049) We should return the index of the input port that supplied the data shown in the view. Just
        // returning 0 is not always correct but we do not have information about what input port was used for the view.
        // Port 0 is our best guess
        return 0;
    }

    @Override
    public boolean canBeUsedInReport() {
        return m_canBeUsedInReport.getAsBoolean();
    }

    @Override
    public NodeMessage getViewNodeMessage(final NodeMessage nodeMessage) {
        if (nodeMessage.getMessageType() == NodeMessage.Type.WARNING) {
            // suppressing warnings for now - eventually to be controlled by the python node implementation
            return NodeMessage.NONE;
        } else {
            return nodeMessage;
        }
    }

    // ================= BUILDER

    /**
     * First stage of the builder—requires the mandatory {@code htmlSupplier}. After calling
     * {@link #htmlSupplier(Supplier)} the builder transitions to {@link HtmlFileNodeViewBuilder} exposing all optional
     * settings.
     */
    public interface HtmlFileNodeViewBuilderRequiresHtmlSupplier {
        /**
         * Defines the {@link Supplier} that provides the absolute {@link Path} to the HTML document. The supplier is
         * evaluated on every invocation of the view ensuring that the latest file is displayed. This is necessary if
         * the path changes between node executions.
         *
         * @param htmlSupplier supplier of an existing, readable HTML file
         * @return the next builder stage enabling the configuration of optional parameters
         */
        HtmlFileNodeViewBuilder htmlSupplier(Supplier<Path> htmlSupplier);
    }

    /**
     * Second and final stage of the fluent builder. All methods return {@code this} so that calls can be conveniently
     * chained. Only {@link #build()} creates the immutable {@link HtmlFileNodeView} instance.
     *
     * <pre>
     * HtmlFileNodeView view = HtmlFileNodeView.builder().htmlSupplier(() -> myHtmlPath).relativeHTMLPath("index.html")
     *     .resources(resources).dataServiceSupplier(() -> new MyRpcHandler()).canBeUsedInReport(true).build();
     * </pre>
     */
    public interface HtmlFileNodeViewBuilder {
        /**
         * Sets the relative path of the HTML file inside the page. This value is used as the base URI for resolving
         * relative links contained in the HTML. Defaults to {@code "index.html"}.
         *
         * @param relativeHTMLPath relative path to the HTML file
         * @return {@code this} builder instance
         */
        HtmlFileNodeViewBuilder relativeHTMLPath(String relativeHTMLPath);

        /**
         * Adds additional static resources (images, CSS, JavaScript, …) that are served alongside the HTML file.
         *
         * @param resources container holding the additional resources (never {@code null})
         * @return {@code this} builder instance
         */
        HtmlFileNodeViewBuilder resources(ViewResources resources);

        /**
         * Supplies a factory for a {@link JsonRpcRequestHandler}. If set, the view will expose a JSON‑RPC endpoint that
         * the frontend can use to invoke backend logic.
         *
         * @param dataServiceSupplier factory that returns a fresh handler for each view instance
         * @return {@code this} builder instance
         */
        HtmlFileNodeViewBuilder dataServiceSupplier(Supplier<JsonRpcRequestHandler> dataServiceSupplier);

        /**
         * Sets a the {@link ReExecutable} used to build the apply data service for view, see
         * {@link ApplyDataService#builder(ReExecutable)}.
         *
         * @param reExecutable the instance, may be {@code null} if no apply data service is required
         *
         * @return {@code this} builder instance
         */
        HtmlFileNodeViewBuilder reExecutable(ReExecutable<String> reExecutable);

        /**
         * Sets the initial data supplier used to build the initial data service for the view, see
         * {@link InitialDataService#builder(Supplier)}.
         *
         * @param dataSupplier supplier of the initial data, may be {@code null} if no initial data service is required
         *
         * @return {@code this} builder instance
         */
        HtmlFileNodeViewBuilder initialDataSupplier(Supplier<String> dataSupplier);

        /**
         * Marks the view as usable in KNIME report templates. Defaults to {@code false}.
         *
         * @param canBeUsedInReport whether the view currently supplied by the {@code htmlSupplier} can be used in a
         *            report
         * @return {@code this} builder instance
         */
        HtmlFileNodeViewBuilder canBeUsedInReport(BooleanSupplier canBeUsedInReport);

        /**
         * Builds the {@link HtmlFileNodeView} using the configuration collected so far.
         *
         * @return a fully‑configured, immutable {@link HtmlFileNodeView}
         * @throws IllegalStateException if the mandatory {@code htmlSupplier} has not been provided
         */
        HtmlFileNodeView build();
    }

    private static class Builder implements HtmlFileNodeViewBuilderRequiresHtmlSupplier, HtmlFileNodeViewBuilder {

        private Supplier<Path> m_htmlSupplier;

        private String m_relativeHTMLPath = "index.html";

        private ViewResources m_resources = ViewResources.EMPTY_RESOURCES;

        private Supplier<JsonRpcRequestHandler> m_dataServiceSupplier;

        private BooleanSupplier m_canBeUsedInReport = () -> false;

        private ReExecutable<String> m_reExecutable;

        private Supplier<String> m_initialDataSupplier;

        @Override
        public HtmlFileNodeViewBuilder htmlSupplier(final Supplier<Path> htmlSupplier) {
            m_htmlSupplier = htmlSupplier;
            return this;
        }

        @Override
        public HtmlFileNodeViewBuilder relativeHTMLPath(final String relativeHTMLPath) {
            m_relativeHTMLPath = relativeHTMLPath;
            return this;
        }

        @Override
        public HtmlFileNodeViewBuilder resources(final ViewResources resources) {
            m_resources = resources;
            return this;
        }

        @Override
        public HtmlFileNodeViewBuilder dataServiceSupplier(final Supplier<JsonRpcRequestHandler> dataServiceSupplier) {
            m_dataServiceSupplier = dataServiceSupplier;
            return this;
        }

        @Override
        public HtmlFileNodeViewBuilder reExecutable(final ReExecutable<String> reExecutable) {
            m_reExecutable = reExecutable;
            return this;
        }

        @Override
        public HtmlFileNodeViewBuilder initialDataSupplier(final Supplier<String> dataSupplier) {
            m_initialDataSupplier = dataSupplier;
            return this;
        }

        @Override
        public HtmlFileNodeViewBuilder canBeUsedInReport(final BooleanSupplier canBeUsedInReport) {
            m_canBeUsedInReport = canBeUsedInReport;
            return this;
        }

        @Override
        public HtmlFileNodeView build() {
            if (m_htmlSupplier == null) {
                throw new IllegalStateException("htmlSupplier is required");
            }
            return new HtmlFileNodeView(m_htmlSupplier, m_relativeHTMLPath, m_resources, m_dataServiceSupplier,
                m_canBeUsedInReport, m_reExecutable, m_initialDataSupplier);
        }
    }
}
