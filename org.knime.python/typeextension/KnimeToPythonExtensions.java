/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
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
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.node.NodeLogger;

public class KnimeToPythonExtensions {
	
	private static Map<String, KnimeToPythonExtension> extensions = new HashMap<String, KnimeToPythonExtension>();
	private Map<String, Serializer<? extends DataValue>> m_serializers = new HashMap<String, Serializer<? extends DataValue>>();

	private static final NodeLogger LOGGER = NodeLogger.getLogger(KnimeToPythonExtensions.class);
		
	@SuppressWarnings({ "unchecked" })
	public static void init() {
		IConfigurationElement[] configs = Platform.getExtensionRegistry().getConfigurationElementsFor(
				"org.knime.python.typeextension.knimetopython");
		for (IConfigurationElement config : configs) {
			try {
				Object o = config.createExecutableExtension("java-serializer-factory");
				if (o instanceof SerializerFactory) {
					String contributer = config.getContributor().getName();
					String filePath = config.getAttribute("python-deserializer");
					File file = Activator.getFile(contributer, filePath);
					if (file != null) {
						SerializerFactory<? extends DataValue> serializer = (SerializerFactory<? extends DataValue>) o;
						String id = config.getAttribute("id");
						extensions.put(id, new KnimeToPythonExtension(id, file.getAbsolutePath(), serializer));
					}
				}
			} catch (CoreException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
		addExtensionsToPython2();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void addExtensionsToPython2() {
		for (KnimeToPythonExtension extension : extensions.values()) {
			org.knime.python2.typeextension.KnimeToPythonExtensions.addExtension(extension.getId(), extension.getPythonDeserializerPath(), new SerializerFactoryWrapper(extension.getJavaSerializerFactory()), false);
		}
	}
	
	public Serializer<? extends DataValue> getSerializer(final String id) {
		if (!m_serializers.containsKey(id)) {
			m_serializers.put(id, extensions.get(id).getJavaSerializerFactory().createSerializer());
		}
		return m_serializers.get(id);
	}
	
	public static KnimeToPythonExtension getExtension(final DataType type) {
		for (KnimeToPythonExtension extension : extensions.values()) {
			Class<? extends DataValue> preferredValueClass = type.getPreferredValueClass();
			if (preferredValueClass.equals(extension.getJavaSerializerFactory().getDataValue())) {
				return extension;
			}
		}
		for (KnimeToPythonExtension extension : extensions.values()) {
			if (extension.getJavaSerializerFactory().isCompatible(type)) {
				return extension;
			}
		}
		return null;
	}
	
	public static Collection<KnimeToPythonExtension> getExtensions() {
		return extensions.values();
	}

}
