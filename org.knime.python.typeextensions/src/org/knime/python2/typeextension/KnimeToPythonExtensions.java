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
package org.knime.python2.typeextension;

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
import org.knime.python2.typeextension.KnimeToPythonExtension;
import org.knime.python2.typeextension.Serializer;
import org.knime.python.typeextensions.Activator;

/**
 * Class for administering all {@link KnimeToPythonExtension}s defined as extension points. 
 * 
 * @author Patrick Winter, Universität Konstanz, Konstanz, Germany
 */
public class KnimeToPythonExtensions {
	
	private static Map<String, KnimeToPythonExtension> extensions = new HashMap<String, KnimeToPythonExtension>();
	private Map<String, Serializer<? extends DataValue>> m_serializers = new HashMap<String, Serializer<? extends DataValue>>();

	private static final NodeLogger LOGGER = NodeLogger.getLogger(KnimeToPythonExtensions.class);
	
	/**
	 * Initialize the internal map of all registered {@link KnimeToPythonExtension}s.
	 */
	@SuppressWarnings({ "unchecked" })
	public static void init() {
		IConfigurationElement[] configs = Platform.getExtensionRegistry().getConfigurationElementsFor(
				"org.knime.python2.typeextension.knimetopython");
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
	}
	
	/**
	 * Add an extension from an external source.
	 * @param id	the extensions id
	 * @param pythonDeserializerPath	path to the file defining the deserializer function for python
	 * @param javaSerializer	{@link SerializerFactory} defining functionallity to serialize a KNIME type
	 * @param force	flag indicating if extension should even be added if the id already exists
	 * @return success
	 */
	public static boolean addExtension(final String id, final String pythonDeserializerPath, final SerializerFactory<? extends DataValue> javaSerializer, final boolean force) {
		if (extensions.containsKey(id) && !force) {
			return false;
		} else {
			extensions.put(id, new KnimeToPythonExtension(id, pythonDeserializerPath, javaSerializer));
			return true;
		}
	}
	
	/**
	 * Return the {@link Serializer} for the given id. The {@link Serializer} instance is saved and returned on every
	 * successive call.
	 * @param id 	the {@link Serializer}'s id
	 * @throws NullPointerException		if the id is not found
	 */
	public Serializer<? extends DataValue> getSerializer(final String id) {
		if (!m_serializers.containsKey(id)) {
			m_serializers.put(id, extensions.get(id).getJavaSerializerFactory().createSerializer());
		}
		return m_serializers.get(id);
	}
	
	/**
	 * Return the extension handeling the given KNIME-{@link DataType}. 
	 * @param type 	a KNIME-{@link DataType}
	 * @return an extension or null if no suitable one was found
	 */
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
	
	/**
	 * @return a list of all registered {@link KnimeToPythonExtension}s
	 */
	public static Collection<KnimeToPythonExtension> getExtensions() {
		return extensions.values();
	}

}
