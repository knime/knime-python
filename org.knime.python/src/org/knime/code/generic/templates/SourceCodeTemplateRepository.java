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
package org.knime.code.generic.templates;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;

public class SourceCodeTemplateRepository {

    private static final String CATEGORY_CFG = "category";
    private static final String TITLE_CFG = "title";
    private static final String DESCRIPTION_CFG = "description";
    private static final String SOURCECODE_CFG = "sourcecode";

    private final String m_id;
    private final File m_templatesFolder;
    private final Map<String, Set<SourceCodeTemplate>> m_categorizedTemplates;

    public SourceCodeTemplateRepository(final String repositoryId) {
        m_id = repositoryId;
        m_templatesFolder = new File(new File(new File(
            KNIMEConstants.getKNIMEHomeDir()), "sourcecode-templates"),
            m_id);
        if (!m_templatesFolder.exists()) {
            m_templatesFolder.mkdirs();
        }
        m_categorizedTemplates = new TreeMap<String, Set<SourceCodeTemplate>>();
        for (final File templateFolder : SourceCodeTemplatesExtensions.getTemplateFolders()) {
            loadTemplates(new File(templateFolder, m_id), true);
        }
        loadTemplates(m_templatesFolder, false);
    }

    public Set<String> getCategories() {
        return m_categorizedTemplates.keySet();
    }

    public Set<SourceCodeTemplate> getTemplatesForCategory(final String category) {
        if (category == null) {
            return new TreeSet<SourceCodeTemplate>();
        }
        return m_categorizedTemplates.get(category);
    }

    public void createTemplate(final String category, final String title,
        final String description, final String sourceCode)
                throws IOException, IllegalArgumentException {
        if ((category == null) || category.isEmpty()) {
            throw new IllegalArgumentException("Category must not be empty");
        } else if ((title == null) || title.isEmpty()) {
            throw new IllegalArgumentException("Title must not be empty");
        }
        final String fileName = generateFileName(title);
        final SourceCodeTemplate template = new SourceCodeTemplate(fileName,
            category, title, description, sourceCode, false);
        saveTemplateFile(template);
        putIntoMap(template);
    }

    public void removeTemplate(final SourceCodeTemplate template) {
        deleteTemplateFile(template);
        removeFromMap(template);
    }

    private void saveTemplateFile(final SourceCodeTemplate template)
            throws IOException {
        final File file = new File(m_templatesFolder, template.getFileName());
        final NodeSettings settings = new NodeSettings(file.getName());
        settings.addString(CATEGORY_CFG, template.getCategory());
        settings.addString(TITLE_CFG, template.getTitle());
        settings.addString(DESCRIPTION_CFG, template.getDescription());
        settings.addString(SOURCECODE_CFG, template.getSourceCode());
        settings.saveToXML(new FileOutputStream(file));
    }

    private void deleteTemplateFile(final SourceCodeTemplate template) {
        new File(m_templatesFolder, template.getFileName()).delete();
    }

    private String generateFileName(final String title) throws IOException {
        return File.createTempFile(title + "__", ".xml", m_templatesFolder)
                .getName();
    }

    private void removeFromMap(final SourceCodeTemplate template) {
        final Set<SourceCodeTemplate> categoryTemplates = m_categorizedTemplates
                .get(template.getCategory());
        categoryTemplates.remove(template);
        if (categoryTemplates.isEmpty()) {
            m_categorizedTemplates.remove(template.getCategory());
        }
    }

    private void putIntoMap(final SourceCodeTemplate template) {
        if (m_categorizedTemplates.containsKey(template.getCategory())) {
            m_categorizedTemplates.get(template.getCategory()).add(template);
        } else {
            final Set<SourceCodeTemplate> newSet = new TreeSet<SourceCodeTemplate>();
            newSet.add(template);
            m_categorizedTemplates.put(template.getCategory(), newSet);
        }
    }

    private void loadTemplates(final File folder, final boolean predefined) {
        if (folder.isDirectory()) {
            for (final File file : folder.listFiles()) {
                if (!file.isDirectory()) {
                    try {
                        final NodeSettingsRO settings = NodeSettings
                                .loadFromXML(new FileInputStream(file));
                        final String category = settings.getString(CATEGORY_CFG);
                        final String title = settings.getString(TITLE_CFG);
                        final String description = settings.getString(DESCRIPTION_CFG);
                        final String sourceCode = settings.getString(SOURCECODE_CFG);
                        final SourceCodeTemplate template = new SourceCodeTemplate(
                            file.getName(), category, title, description,
                            sourceCode, predefined);
                        putIntoMap(template);
                    } catch (IOException | InvalidSettingsException e) {
                    }
                }
            }
        }
    }

}
