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
 *   Jul 16, 2025 (benjaminwilhelm): created
 */
package org.knime.python3.views;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.knime.core.util.PathUtils;

/**
 * Represents the path to a HTML file for a Python node view and metadata about its usability in reports. This class
 * provides methods to manage the file path and metadata, and to interact with the file system.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public class PythonNodeViewStoragePath {

    private static final String VIEW_HTML_FILE_NAME = "view.html";

    private static final String REPORT_MARKER_FILE_NAME = "can_be_used_in_report";

    private final Path m_path;

    private boolean m_canBeUsedInReport;

    /**
     * Creates a temporary HTML file for the Python node view.
     *
     * @throws IOException if the temporary file cannot be created.
     */
    public PythonNodeViewStoragePath() throws IOException {
        this(PathUtils.createTempFile("output_view", ".html"));
    }

    /**
     * Initializes the storage path with the given file path.
     *
     * @param path the path to the HTML file.
     */
    public PythonNodeViewStoragePath(final Path path) {
        m_path = path;
    }

    /**
     * Initializes the storage path with the given file path and report usability metadata.
     *
     * @param path the path to the HTML file.
     * @param canBeUsedInReport whether the view can be used in reports.
     */
    private PythonNodeViewStoragePath(final Path path, final boolean canBeUsedInReport) {
        this(path);
        m_canBeUsedInReport = canBeUsedInReport;
    }

    /**
     * Gets the path to the HTML file.
     *
     * @return the path to the HTML file.
     */
    public Path getPath() {
        return m_path;
    }

    /**
     * Checks if the view can be used in reports.
     *
     * @return {@code true} if the view can be used in reports, {@code false} otherwise.
     */
    public boolean canBeUsedInReport() {
        return m_canBeUsedInReport;
    }

    /**
     * Deletes the HTML file if it exists.
     *
     * @throws IOException if the file cannot be deleted.
     */
    public void deleteIfExists() throws IOException {
        PathUtils.deleteFileIfExists(m_path);
    }

    /**
     * Creates a sink for the Python process to populate the view.
     *
     * @return a {@link PythonNodeViewSink} instance.
     */
    public PythonNodeViewSink getSink() {
        return new Sink();
    }

    /**
     * Loads the storage path from the internal directory.
     *
     * @param nodeInternDir the internal directory of the node.
     * @return an {@link Optional} containing the storage path if the file is readable, or an empty {@link Optional}
     *         otherwise.
     */
    public static Optional<PythonNodeViewStoragePath> loadFromInternals(final Path nodeInternDir) {
        var viewHtmlPath = nodeInternDir.resolve(VIEW_HTML_FILE_NAME);
        if (Files.isReadable(viewHtmlPath)) {
            var canBeUsedInReport = Files.exists(nodeInternDir.resolve(REPORT_MARKER_FILE_NAME));
            return Optional.of(new PythonNodeViewStoragePath(viewHtmlPath, canBeUsedInReport));
        }
        return Optional.empty();
    }

    /**
     * Saves the storage path to the internals directory of the node.
     *
     * @param nodeInternDir the internal directory of the node.
     * @throws IOException if the file cannot be saved.
     */
    public void saveToInternals(final Path nodeInternDir) throws IOException {
        Files.copy(m_path, nodeInternDir.resolve(VIEW_HTML_FILE_NAME));
        if (m_canBeUsedInReport) {
            Files.createFile(nodeInternDir.resolve(REPORT_MARKER_FILE_NAME));
        } else {
            Files.deleteIfExists(nodeInternDir.resolve(REPORT_MARKER_FILE_NAME));
        }
    }

    /** Represents a sink for the Python process to populate the view. */
    private class Sink implements PythonNodeViewSink {

        @Override
        public String getOutputFilePath() {
            return m_path.toAbsolutePath().toString();
        }

        @Override
        public void setCanBeUsedInReport(final boolean canBeUsedInReport) {
            m_canBeUsedInReport = canBeUsedInReport;
        }
    }
}
