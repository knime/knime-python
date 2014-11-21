package org.knime.code.generic;

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

	private String m_id;
	private File m_templatesFolder;
	private Map<String, Set<SourceCodeTemplate>> m_categorizedTemplates;

	public SourceCodeTemplateRepository(final String repositoryId) {
		m_id = repositoryId;
		m_templatesFolder = new File(new File(new File(
				KNIMEConstants.getKNIMEHomeDir()), "python-templates"),
				repositoryId);
		if (!m_templatesFolder.exists()) {
			m_templatesFolder.mkdirs();
		}
		m_categorizedTemplates = new TreeMap<String, Set<SourceCodeTemplate>>();
		loadTemplates();
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
			throws IOException {
		String fileName = generateFileName(title);
		SourceCodeTemplate template = new SourceCodeTemplate(fileName,
				category, title, description, sourceCode);
		saveTemplateFile(template);
		putIntoMap(template);
	}

	public void removeTemplate(final SourceCodeTemplate template) {
		deleteTemplateFile(template);
		removeFromMap(template);
	}

	private void saveTemplateFile(final SourceCodeTemplate template)
			throws IOException {
		File file = new File(m_templatesFolder, template.getFileName());
		NodeSettings settings = new NodeSettings(file.getName());
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
		return File.createTempFile(title + "_", ".xml", m_templatesFolder)
				.getName();
	}

	private void removeFromMap(final SourceCodeTemplate template) {
		Set<SourceCodeTemplate> categoryTemplates = m_categorizedTemplates
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
			Set<SourceCodeTemplate> newSet = new TreeSet<SourceCodeTemplate>();
			newSet.add(template);
			m_categorizedTemplates.put(template.getCategory(), newSet);
		}
	}

	private void loadTemplates() {
		for (File file : m_templatesFolder.listFiles()) {
			if (!file.isDirectory()) {
				try {
					NodeSettingsRO settings = NodeSettings
							.loadFromXML(new FileInputStream(file));
					String category = settings.getString(CATEGORY_CFG);
					String title = settings.getString(TITLE_CFG);
					String description = settings.getString(DESCRIPTION_CFG);
					String sourceCode = settings.getString(SOURCECODE_CFG);
					SourceCodeTemplate template = new SourceCodeTemplate(
							file.getName(), category, title, description,
							sourceCode);
					putIntoMap(template);
				} catch (IOException | InvalidSettingsException e) {
				}
			}
		}
	}

}
