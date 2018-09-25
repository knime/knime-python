/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python2.generic;

import javax.swing.text.BadLocationException;

import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.rsyntaxtextarea.guarded.GuardedDocument;

/**
 * Configuration for the generic source code panel.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 */
public class SourceCodeConfig {



    static final int DEFAULT_ROW_LIMIT = 1000;

    private static final String CFG_SOURCE_CODE = "sourceCode";

    private GuardedDocument m_document = new GuardedDocument(SyntaxConstants.SYNTAX_STYLE_PYTHON);

    private static final String CFG_ROW_LIMIT = "rowLimit";

    private int m_rowLimit = DEFAULT_ROW_LIMIT;

    /**
     *Constructor that initializes the document
     */
    public SourceCodeConfig() {
        try {
            m_document.replace(0, m_document.getLength(), getDefaultSourceCode(), null);
        } catch (BadLocationException ex) {
            //this should not happen
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Save configuration to the given node settings.
     *
     * @param settings The settings to save to
     */
    public void saveTo(final NodeSettingsWO settings) {
        try {
            settings.addString(CFG_SOURCE_CODE, m_document.getText(0, m_document.getLength()));
        } catch (BadLocationException ex) {
          //this should not happen
            throw new IllegalStateException(ex);
        }
        settings.addInt(CFG_ROW_LIMIT, m_rowLimit);
    }

    /**
     * Load configuration from the given node settings.
     *
     * @param settings The settings to load from
     * @throws InvalidSettingsException If the settings are invalid
     */
    public void loadFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        String sourceCode = settings.getString(CFG_SOURCE_CODE);
        try {
            m_document.replace(0, m_document.getLength(), sourceCode, null);
        } catch (BadLocationException ex) {
            //this should not happen
            throw new IllegalStateException(ex);
        }
        m_rowLimit = settings.getInt(CFG_ROW_LIMIT);
    }

    /**
     * Load configuration from the given node settings (using defaults if necessary).
     *
     * @param settings The settings to load from
     */
    public void loadFromInDialog(final NodeSettingsRO settings) {
        String sourceCode = settings.getString(CFG_SOURCE_CODE, getDefaultSourceCode());
        try {
            m_document.replace(0, m_document.getLength(), sourceCode, null);
        } catch (BadLocationException ex) {
            //this should not happen
            throw new IllegalStateException(ex);
        }
        m_rowLimit = settings.getInt(CFG_ROW_LIMIT, DEFAULT_ROW_LIMIT);
    }

    /**
     * Return the source code.
     *
     * @return The source code
     */
    public String getSourceCode() {
        try {
            return m_document.getText(0, m_document.getLength());
        } catch (BadLocationException ex) {
          //this should not happen
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Sets the source code.
     *
     * @param sourceCode The source code
     */
    public void setSourceCode(final String sourceCode) {
        try {
            m_document.replace(0, m_document.getLength(), sourceCode, null);
        } catch (BadLocationException ex) {
            //this should not happen
            throw new IllegalStateException(ex);
        }
    }

    /**
     * @return the guardedDoc
     */
    public GuardedDocument getDoc() {
        return m_document;
    }

    /**
     * @param document the guardedDoc to set
     */
    public void setDoc(final GuardedDocument document) {
        m_document = document;
    }
    /**
     * Return the row limit.
     *
     * @return The row limit
     */
    public int getRowLimit() {
        return m_rowLimit;
    }

    /**
     * Sets the row limit.
     *
     * @param rowLimit The row limit
     */
    public void setRowLimit(final int rowLimit) {
        m_rowLimit = rowLimit;
    }

    /**
     * Return the default source code.
     *
     * @return The default source code
     */
    protected String getDefaultSourceCode() {
        return "";
    }

}
