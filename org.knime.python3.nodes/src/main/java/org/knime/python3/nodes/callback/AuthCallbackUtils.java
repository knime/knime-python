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
import java.time.Instant;
import java.util.NoSuchElementException;

import org.knime.core.util.auth.CouldNotAuthorizeException;
import org.knime.credentials.base.Credential;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.credentials.base.oauth.api.AccessTokenAccessor;
import org.knime.credentials.base.oauth.api.HttpAuthorizationHeaderCredentialValue;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonCredentialPortObjectSpec;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy.ExpiryDate;

/**
 * Default implementation for callbacks that handle authentication port object specs.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class AuthCallbackUtils {

    private AuthCallbackUtils() {
        // Utility class
    }

    /**
     * Retrieves the authentication schema from a serialized XML representation of a credential.
     *
     * @param serializedXMLString The serialized XML string containing credential information.
     * @return The authentication schema extracted from the credential, or null if the credential is null.
     * @throws CouldNotAuthorizeException if the credential cannot be authorized.
     * @throws ClassNotFoundException if the class of the credential cannot be found.
     * @throws InstantiationException if the credential cannot be instantiated.
     * @throws IllegalAccessException if accessing the class of the credential is illegal.
     * @throws IOException if an I/O error occurs during deserialization.
     * @throws NoSuchElementException if the credential from the XML string is not found.
     */
    public static String getAuthSchema(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
        ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR

        final HttpAuthorizationHeaderCredentialValue cred = getCredentialFromXMLString(serializedXMLString);
        if (cred != null) {
            return cred.getAuthScheme();
        }

        return null;

    }

    /**
     * Retrieves the authentication parameters from a serialized XML representation of a credential.
     *
     * @param serializedXMLString The serialized XML string containing credential information.
     * @return The authentication parameters extracted from the credential, or null if the credential is null.
     * @throws CouldNotAuthorizeException if the credential cannot be authorized.
     * @throws ClassNotFoundException if the class of the credential cannot be found.
     * @throws InstantiationException if the credential cannot be instantiated.
     * @throws IllegalAccessException if accessing the class of the credential is illegal.
     * @throws IOException if an I/O error occurs during deserialization.
     * @throws NoSuchElementException if the credential from the XML string is not found.
     */
    public static String getAuthParameters(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
        ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR

        final HttpAuthorizationHeaderCredentialValue cred = getCredentialFromXMLString(serializedXMLString);
        if (cred != null) {
            return cred.getAuthParameters();
        }

        return null;
    }

    /**
     * @param serializedXMLString
     * @return The expiry date of the access token extracted from the credential, or null if the credential is not a
     * @throws CouldNotAuthorizeException if the credential cannot be authorized.
     * @throws ClassNotFoundException if the class of the credential cannot be found.
     * @throws InstantiationException if the credential cannot be instantiated.
     * @throws IllegalAccessException if accessing the class of the credential is illegal.
     * @throws IOException if an I/O error occurs during deserialization.
     * @throws NoSuchElementException if the credential from the XML string is not found.
     */
    public static ExpiryDate getExpiresAfter(final String serializedXMLString) throws CouldNotAuthorizeException, // NOSONAR
        ClassNotFoundException, InstantiationException, IllegalAccessException, IOException { // NOSONAR
        final HttpAuthorizationHeaderCredentialValue cred = getCredentialFromXMLString(serializedXMLString);
        if (cred instanceof AccessTokenAccessor accessor) {
            final Instant instant = accessor.getExpiresAfter().orElse(null);
            if (instant != null) {
                return new ExpiryDate(instant.getEpochSecond(), instant.getNano());
            }
        }
        return null;
    }

    /**
     * Retrieves the credential from a serialized XML representation of a credential.
     *
     * @param serializedXMLString The serialized XML string containing credential information.
     * @return The credential extracted from the XML string, or null if the credential is not a
     *         {@link HttpAuthorizationHeaderCredentialValue}.
     * @throws ClassNotFoundException if the class of the credential cannot be found.
     * @throws InstantiationException if the credential cannot be instantiated.
     * @throws IllegalAccessException if accessing the class of the credential is illegal.
     * @throws IOException if an I/O error occurs during deserialization.
     * @throws NoSuchElementException if the credential from the XML string is not found.
     */
    public static HttpAuthorizationHeaderCredentialValue getCredentialFromXMLString(final String serializedXMLString)
        throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {

        CredentialPortObjectSpec credentialPortObjectSpec =
            PythonCredentialPortObjectSpec.loadFromXMLCredentialPortObjectSpecString(serializedXMLString);

        var credential = credentialPortObjectSpec.getCredential(Credential.class);

        var cred = credential.orElseThrow();
        if (cred instanceof HttpAuthorizationHeaderCredentialValue val) {
            return val;
        }
        return null;
    }

}
