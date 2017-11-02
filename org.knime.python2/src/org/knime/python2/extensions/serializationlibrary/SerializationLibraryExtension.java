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
package org.knime.python2.extensions.serializationlibrary;

import org.knime.python2.PythonPreferencePage;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibraryFactory;

/**
 * Extension for plugins implementing a serialization library for transmitting data between KNIME and python.
 *
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 */

public class SerializationLibraryExtension {
    private final String m_id;

    private final String m_pythonSerializationLibraryPath;

    private final SerializationLibraryFactory m_javaSerializationLibraryFactory;

    private final boolean m_hidden;

    /**
     * Constructor.
     *
     * @param id a unique id for the serialization library
     * @param pythonSerializationLibraryPath the path to the serialization libraries main python source file
     * @param javaSerializationLibraryFactory the serialization library factory for creating a
     *            {@link SerializationLibrary} managing serialization on the java side
     */
    public SerializationLibraryExtension(final String id, final String pythonSerializationLibraryPath,
        final SerializationLibraryFactory javaSerializationLibraryFactory) {
        this(id, pythonSerializationLibraryPath, javaSerializationLibraryFactory, false);
    }

    /**
     * Constructor.
     *
     * @param id a unique id for the serialization library
     * @param pythonSerializationLibraryPath the path to the serialization libraries main python source file
     * @param javaSerializationLibraryFactory the serialization library factory for creating a
     *            {@link SerializationLibrary} managing serialization on the java side
     * @param hidden indicates if the serialization library should be hidden from the user in the
     *            {@link PythonPreferencePage}
     */
    public SerializationLibraryExtension(final String id, final String pythonSerializationLibraryPath,
        final SerializationLibraryFactory javaSerializationLibraryFactory, final boolean hidden) {
        m_id = id;
        m_pythonSerializationLibraryPath = pythonSerializationLibraryPath;
        m_javaSerializationLibraryFactory = javaSerializationLibraryFactory;
        m_hidden = hidden;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public String getId() {
        return m_id;
    }

    /**
     * Checks if library is hidden from the user.
     *
     * @return true, if is hidden
     */
    public boolean isHidden() {
        return m_hidden;
    }

    /**
     * Gets the python serialization library path.
     *
     * @return the python serialization library path
     */
    public String getPythonSerializationLibraryPath() {
        return m_pythonSerializationLibraryPath;
    }

    /**
     * Gets the java serialization library factory.
     *
     * @return the java serialization library factory
     */
    public SerializationLibraryFactory getJavaSerializationLibraryFactory() {
        return m_javaSerializationLibraryFactory;
    }
}
