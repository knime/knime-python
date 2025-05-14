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
 *   May 14, 2025 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.callback;

import java.io.IOException;

import org.knime.core.util.auth.CouldNotAuthorizeException;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy.ExpiryDate;

/**
 * Interface for callbacks that handle authentication port object specs.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public interface AuthCallback {

    /**
     * Retrieves the authentication schema from a serialized XML representation of a credential.
     *
     * This method parses the input XML string and extracts the authentication schema from it, assuming that the XML
     * represents a valid CredentialPortObjectSpec.
     *
     * @param serializedXMLString The serialized XML string containing credential information.
     * @return The authentication schema extracted from the credential, or null if the schema is not found.
     * @throws CouldNotAuthorizeException If there is an issue with the authorization process.
     * @throws ClassNotFoundException If the required class is not found during deserialization.
     * @throws InstantiationException If an error occurs while instantiating an object during deserialization.
     * @throws IllegalAccessException If there is an illegal access operation during deserialization.
     * @throws IOException If an I/O error occurs during deserialization.
     */
    public String get_auth_schema(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
        ClassNotFoundException, InstantiationException, IllegalAccessException, IOException;

    /**
     * Retrieves the authentication parameters from a serialized XML representation of a credential.
     *
     * This method parses the input XML string and extracts the authentication parameters from it, assuming that the XML
     * represents a valid CredentialPortObjectSpec.
     *
     * @param serializedXMLString The serialized XML string containing credential information.
     * @return The authentication parameters extracted from the credential, or null if not found.
     * @throws CouldNotAuthorizeException If there is an issue with the authorization process.
     * @throws ClassNotFoundException If the required class is not found during deserialization.
     * @throws InstantiationException If an error occurs while instantiating an object during deserialization.
     * @throws IllegalAccessException If there is an illegal access operation during deserialization.
     * @throws IOException If an I/O error occurs during deserialization or when retrieving parameters.
     */
    public String get_auth_parameters(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
        ClassNotFoundException, InstantiationException, IllegalAccessException, IOException;

    /**
     * Retrieves the optional expiry time of the access token from a serialized XML representation of a credential.
     *
     * This method parses the input XML string and extracts the expiry time of the access token from it, assuming that
     * the XML represents a valid CredentialPortObjectSpec that has an expiry time.
     *
     * @param serializedXMLString The serialized XML string containing credential information.
     * @return The optional expiry time of the access token extracted from the credential, or null if not found.
     * @throws CouldNotAuthorizeException If there is an issue with the authorization process.
     * @throws ClassNotFoundException If the required class is not found during deserialization.
     * @throws InstantiationException If an error occurs while instantiating an object during deserialization.
     * @throws IllegalAccessException If there is an illegal access operation during deserialization.
     * @throws IOException If an I/O error occurs during deserialization or when retrieving parameters.
     */
    public ExpiryDate get_expires_after(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
        ClassNotFoundException, InstantiationException, IllegalAccessException, IOException;

}
