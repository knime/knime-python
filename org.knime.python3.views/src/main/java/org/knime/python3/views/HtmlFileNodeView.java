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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Supplier;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.webui.data.ApplyDataService;
import org.knime.core.webui.data.DataService;
import org.knime.core.webui.data.InitialDataService;
import org.knime.core.webui.node.view.NodeView;
import org.knime.core.webui.page.Page;

/**
 * A {@link NodeView} that just shows an HTML document that is saved on disk.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class HtmlFileNodeView implements NodeView {

    private final Supplier<Path> m_htmlSupplier;

    private final ViewResources m_resources;

    /**
     * Create a view that shows the HTML document that is saved at the given location.
     *
     * @param htmlSupplier A supplier that provides the path to the HTML file that should be shown currently. The file
     *            must exist and must be readable.
     */
    public HtmlFileNodeView(final Supplier<Path> htmlSupplier) {
        this(htmlSupplier, ViewResources.EMPTY_RESOURCES);
    }

    /**
     * Create a view that shows the HTML document that is saved at the given location.
     *
     * @param htmlSupplier A supplier that provides the path to the HTML file that should be shown currently. The file
     *            must exist and must be readable.
     * @param resources resources that are available to the page.
     */
    public HtmlFileNodeView(final Supplier<Path> htmlSupplier, final ViewResources resources) {
        m_htmlSupplier = htmlSupplier;
        m_resources = resources;
    }

    @Override
    public Optional<InitialDataService> createInitialDataService() {
        return Optional.empty();
    }

    @Override
    public Optional<DataService> createDataService() {
        return Optional.empty();
    }

    @Override
    public Optional<ApplyDataService> createApplyDataService() {
        return Optional.empty();
    }

    @Override
    public Page getPage() {
        var pb = Page.builder(this::openPage, "index.html");
        m_resources.addToPageBuilder(pb);
        return pb.build();
    }

    /** Open the HTML page */
    private InputStream openPage() {
        try {
            return Files.newInputStream(m_htmlSupplier.get());
        } catch (final IOException e) {
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
}
