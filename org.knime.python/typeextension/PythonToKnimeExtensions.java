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
package org.knime.python.typeextension;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;

public class PythonToKnimeExtensions {

	private static Map<String, PythonToKnimeExtension> extensions = new HashMap<String, PythonToKnimeExtension>();
	private Map<String, Deserializer> m_deserializers = new HashMap<String, Deserializer>();

	private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonToKnimeExtensions.class);

	public static void init() {
		IConfigurationElement[] configs = Platform.getExtensionRegistry().getConfigurationElementsFor(
				"org.knime.python.typeextension.pythontoknime");
		for (IConfigurationElement config : configs) {
			try {
				Object o = config.createExecutableExtension("java-deserializer-factory");
				if (o instanceof DeserializerFactory) {
					String contributer = config.getContributor().getName();
					String filePath = config.getAttribute("python-serializer");
					File file = Activator.getFile(contributer, filePath);
					if (file != null) {
						DeserializerFactory deserializer = (DeserializerFactory) o;
						String id = config.getAttribute("id");
						extensions.put(id, new PythonToKnimeExtension(id, config.getAttribute("python-type-identifier"), file.getAbsolutePath(), deserializer));
					}
				}
			} catch (CoreException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
		addExtensionsToPython2();
	}
	
	public static void addExtensionsToPython2() {
		for (PythonToKnimeExtension extension : extensions.values()) {
			org.knime.python2.typeextension.PythonToKnimeExtensions.addExtension(extension.getId(), extension.getType(), extension.getPythonSerializerPath(), new DeserializerFactoryWrapper(extension.getJavaDeserializerFactory()), false);
		}
	}
	
	public Deserializer getDeserializer(final String id) {
		if (!m_deserializers.containsKey(id)) {
			m_deserializers.put(id, extensions.get(id).getJavaDeserializerFactory().createDeserializer());
		}
		return m_deserializers.get(id);
	}
	
	public static PythonToKnimeExtension getExtension(final String id) {
		return extensions.get(id);
	}
	
	public static Collection<PythonToKnimeExtension> getExtensions() {
		return extensions.values();
	}
	
}
