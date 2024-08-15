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
 *   Aug 1, 2024 (adrian.nembach): created
 */
package org.knime.python3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.knime.core.node.NodeLogger;
import org.knime.core.util.PathUtils;

/**
 * Controls how the the CAs that Python trusts when using SSL are determined. The mode can be set via the system
 * property {@code knime.python.cacerts} which can be set to either {@link PythonCaCertsMode#ENV} to trust the CAs of
 * the Python environment or {@link PythonCaCertsMode#AP} to trust the CAs that the AP trusts. The property match is
 * done case-insensitive and defaults to ENV, also if the property isn't set at all.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
enum PythonCaCertsMode {
        /**
         * Trust the CAs of the Python environment. This is the default behavior.
         */
        ENV(e -> {
            // leave as is
        }),
        /**
         * Trust the CAs that the AP trusts. Creates a .crt file with the CAs that Java trusts and points the
         * following environment variables to it:
         * <ul>
         * <li>{@code REQUESTS_CA_BUNDLE}: Respected by requests, httpx and packages that use them.
         * <li>{@code SSL_CERT_FILE}: Respected by httpx, ssl and packages that use them.
         * </ul>
         */
        AP(PythonCaCertsMode::overwriteWithAPCerts);

    private final Consumer<Map<String, String>> m_envModifier;

    PythonCaCertsMode(final Consumer<Map<String, String>> envModifier) {
        m_envModifier = envModifier;
    }

    private static final String PYTHON_CACERTS_PROPERTY = "knime.python.cacerts";

    // environment variables that common Python libraries use to resolve custom CAs (requests, httpx, ssl)
    private static final Set<String> CA_CERT_ENV_VARS = Set.of("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE");

    static PythonCaCertsMode fromProperty() {
        var property = System.getProperty(PYTHON_CACERTS_PROPERTY, PythonCaCertsMode.ENV.name());
        if (AP.name().equalsIgnoreCase(property)) {
            return AP;
        }
        return ENV;
    }

    void updateEnvironment(final Map<String, String> environment) {
        m_envModifier.accept(environment);
    }

    private static void overwriteWithAPCerts(final Map<String, String> environment) {

        var tempCertFile = getCertificatePath();
        if (tempCertFile.isEmpty()) {
            // writing the certificates failed for some reason, so we leave the environment as is
            return;
        }
        var pathString = tempCertFile.get().toAbsolutePath().toString();
        for (var envVar : CA_CERT_ENV_VARS) {
            environment.put(envVar, pathString);
        }
    }

    static Optional<Path> getCertificatePath() {
        return Optional.ofNullable(CertificateHolder.CERTIFICATE_PATH);
    }

    // By having the holder class, the Java class loading mechnanism ensures thread-safety.
    private static final class CertificateHolder {
        // initialized first so that it is available in the catch-clause of getCertificatePath
        private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonCaCertsMode.CertificateHolder.class);

        private static final Path CERTIFICATE_PATH = getCertificatePath();

        /**
         * @return the path to a temporary cert file or null if the certificate file couldn't be created.
         */
        private static Path getCertificatePath() {
            try {
                var trustManager = getTrustManager();
                if (trustManager == null) {
                    return null;
                }
                // the file is deleted on JVM shutdown
                var tempCertFile = PathUtils.createTempFile("all_certs", ".crt");
                writeCertificatesToFile(trustManager.getAcceptedIssuers(), tempCertFile);
                return tempCertFile;
            } catch (Throwable e) {// NOSONAR here we want to catch all Throwables including Errors
                LOGGER.error("Failed to create the CA file for Python.", e);
                return null;
            }
        }

        private static TrustManagerFactory getDefaultTrustManagerFactory()
            throws NoSuchAlgorithmException, KeyStoreException {
            TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init((KeyStore)null); // Passing null initializes with the default trust store
            return trustManagerFactory;
        }

        private static X509TrustManager getTrustManager() throws NoSuchAlgorithmException, KeyStoreException {
            // Get the default TrustManagerFactory
            var trustManagerFactory = getDefaultTrustManagerFactory();
            // Get the trust managers from the factory
            for (var trustManager : trustManagerFactory.getTrustManagers()) {
                if (trustManager instanceof X509TrustManager typedManager) {
                    return typedManager;
                }
            }
            LOGGER.error("Could not find a suitable TrustManager.");
            return null;
        }

        private static void writeCertificatesToFile(final X509Certificate[] certs, final Path outputPath)
            throws IOException, CertificateEncodingException {
            try (var writer = Files.newBufferedWriter(outputPath, StandardOpenOption.APPEND)) {
                for (var cert : certs) {
                    String pemCert = "-----BEGIN CERTIFICATE-----\n"
                        + Base64.getEncoder().encodeToString(cert.getEncoded()) + "\n-----END CERTIFICATE-----\n";
                    writer.write(pemCert);
                }
            }
        }
    }

}
