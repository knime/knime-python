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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
package org.knime.python2.generic.templates;

/**
 * Container class for a source code template. The actual contents are stored in a file.
 *
 * @author Patrick Winter, Universität Konstanz, Konstanz, Germany
 */

public class SourceCodeTemplate implements Comparable<SourceCodeTemplate> {

    private final String m_fileName;

    private final String m_category;

    private final String m_title;

    private final String m_description;

    private final String m_sourceCode;

    private final boolean m_predefined;

    /**
     * Constructor.
     *
     * @param fileName the name of the file for storing the template
     * @param category the template's category
     * @param title the template's title
     * @param description a description of what the template does
     * @param sourceCode the actual source code
     * @param predefined flag indicating if the template is predefined or userdefined
     */
    public SourceCodeTemplate(final String fileName, final String category, final String title,
        final String description, final String sourceCode, final boolean predefined) {
        m_fileName = fileName;
        m_category = category;
        m_title = title;
        m_description = description;
        m_sourceCode = sourceCode;
        m_predefined = predefined;
    }

    /**
     * Gets the file name.
     *
     * @return the file name
     */
    public String getFileName() {
        return m_fileName;
    }

    /**
     * Gets the category.
     *
     * @return the category
     */
    public String getCategory() {
        return m_category;
    }

    /**
     * Gets the title.
     *
     * @return the title
     */
    public String getTitle() {
        return m_title;
    }

    /**
     * Gets the description.
     *
     * @return the description
     */
    public String getDescription() {
        return m_description;
    }

    /**
     * Gets the source code.
     *
     * @return the source code
     */
    public String getSourceCode() {
        return m_sourceCode;
    }

    /**
     * Checks if is predefined.
     *
     * @return true, if is predefined
     */
    public boolean isPredefined() {
        return m_predefined;
    }

    @Override
    public String toString() {
        return m_title;
    }

    @Override
    public int compareTo(final SourceCodeTemplate o) {
        if (isPredefined() != o.isPredefined()) {
            return isPredefined() ? 1 : -1;
        }
        int result = getTitle().compareTo(o.getTitle());
        if (result == 0) {
            result = getFileName().compareTo(o.getFileName());
        }
        return result;
    }

}
