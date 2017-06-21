/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
package org.knime.python2.extensions.serializationlibrary;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.knime.python2.Activator;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibraryFactory;

/**
 * Class administrating all {@link SerializationLibraryExtension}s.
 * 
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 */

public class SerializationLibraryExtensions {
	
	private static Map<String, SerializationLibraryExtension> extensions = new HashMap<String, SerializationLibraryExtension>();
	private Map<String, SerializationLibrary> m_serializationLibrary = new HashMap<String, SerializationLibrary>();

	private static final NodeLogger LOGGER = NodeLogger.getLogger(SerializationLibraryExtensions.class);
		
	/**
	 * Initialize the internal map of all registered {@link SerializationLibraryExtension}s.
	 */
	public static void init() {
		IConfigurationElement[] configs = Platform.getExtensionRegistry().getConfigurationElementsFor(
				"org.knime.python2.serializationlibrary");
		for (IConfigurationElement config : configs) {
			try {
				Object o = config.createExecutableExtension("java-serializationlibrary-factory");
				if (o instanceof SerializationLibraryFactory) {
					String contributer = config.getContributor().getName();
					String filePath = config.getAttribute("python-serializationlibrary");
					File file = Activator.getFile(contributer, filePath);
					if (file != null) {
						SerializationLibraryFactory serializationLibrary = (SerializationLibraryFactory) o;
						String id = config.getAttribute("id");
						extensions.put(id, new SerializationLibraryExtension(id, file.getAbsolutePath(), serializationLibrary));
					}
				}
			} catch (CoreException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
	}
	
	public SerializationLibrary getSerializationLibrary(final String id) {
		if (!m_serializationLibrary.containsKey(id)) {
			m_serializationLibrary.put(id, extensions.get(id).getJavaSerializationLibraryFactory().createInstance());
		}
		return m_serializationLibrary.get(id);
	}
	
	public static Collection<SerializationLibraryExtension> getExtensions() {
		return extensions.values();
	}
	
	public static String getSerializationLibraryPath(final String id) {
		return extensions.get(id).getPythonSerializationLibraryPath();
	}

}
