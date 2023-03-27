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
 *   May 16, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.knime.core.node.NodeLogger;
import org.knime.python3.PythonGateway;
import org.knime.python3.PythonGatewayCreationGate;
import org.knime.python3.PythonGatewayCreationGate.PythonGatewayCreationGateListener;
import org.knime.python3.nodes.PurePythonNodeSetFactory.ResolvedPythonExtension;
import org.knime.python3.nodes.proxy.CloseableNodeFactoryProxy;
import org.knime.python3.nodes.proxy.NodeDialogProxy;
import org.knime.python3.nodes.proxy.NodeProxyProvider;
import org.knime.python3.nodes.proxy.model.NodeConfigurationProxy;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A {@link NodeProxyProvider} that caches the gateway used for non-execution proxies. The number of gateways to cache
 * can be controlled via the system property {@code knime.python.extension.gateway.cache.size} and defaults to 3.
 * Gateways that aren't used for a certain time-interval are closed periodically. The expiration time in seconds can be
 * controlled via the system property {@code knime.python.extension.gateway.cache.expiration} and defaults to 300.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class CachedNodeProxyProvider extends PurePythonExtensionNodeProxyProvider {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(CachedNodeProxyProvider.class);

    private static final String CACHE_SIZE_PROPERTY = "knime.python.extension.gateway.cache.size";

    private static final int CACHE_SIZE_DEFAULT = 3;

    private static final String CACHE_EXPIRATION_PROPERTY = "knime.python.extension.gateway.cache.expiration";

    private static final int CACHE_EXPIRATION_DEFAULT = 300;

    private static final ScheduledExecutorService EXEC_SERVICE = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("python-node-gateway-cache-cleaner-%d").build());

    private static final LoadingCache<ResolvedPythonExtension, //
            CachedObject<PythonGateway<KnimeNodeBackend>>> GATEWAY_CACHE = createCache();

    private static boolean gatewayCacheClosed;

    CachedNodeProxyProvider(final ResolvedPythonExtension extension, final String nodeId) {
        super(extension, nodeId);
    }

    private static int getCacheSize() {
        final var property = System.getProperty(CACHE_SIZE_PROPERTY, Integer.toString(CACHE_SIZE_DEFAULT));
        try {
            var size = Integer.parseInt(property);
            if (size < 1) {
                LOGGER.errorWithFormat("Values below 1 (%s) for '%s' are not allowed.", size, CACHE_SIZE_PROPERTY);
            }
        } catch (NumberFormatException ex) {
            LOGGER.error("Illegal cache size specified. The cache size must be an integer.", ex);
        }
        return 3;
    }

    private static int getExpirationInSeconds() {
        final var property = System.getProperty(CACHE_EXPIRATION_PROPERTY, Integer.toString(CACHE_EXPIRATION_DEFAULT));
        try {
            var expiration = Integer.parseInt(property);
            if (expiration < 1) {
                LOGGER.errorWithFormat("Values below 1 (%s) for '%s' are not allowed.", expiration,
                    CACHE_EXPIRATION_PROPERTY);
            }
        } catch (NumberFormatException ex) {
            LOGGER.error("Illegal expiration time specified. "
                + "The expiration time must be an integer denoting the seconds an unused gateway stays alive.", ex);
        }
        return 300;
    }

    private static LoadingCache<ResolvedPythonExtension, CachedObject<PythonGateway<KnimeNodeBackend>>> createCache() {
        CacheLoader<ResolvedPythonExtension, CachedObject<PythonGateway<KnimeNodeBackend>>> loader =
            new CacheLoader<ResolvedPythonExtension, CachedObject<PythonGateway<KnimeNodeBackend>>>() {

                // the gateway is closed either in #onRemoveFromCache or by the proxy
                @SuppressWarnings("resource")
                @Override
                public CachedObject<PythonGateway<KnimeNodeBackend>> load(final ResolvedPythonExtension key)
                    throws Exception {
                    return new CachedObject<>(key.createGateway());
                }
            };

        var cache = CacheBuilder.newBuilder()//
            .expireAfterAccess(getExpirationInSeconds(), TimeUnit.SECONDS)//
            .maximumSize(getCacheSize())//
            .removalListener(CachedNodeProxyProvider::onRemoveFromCache)//
            .build(loader);

        EXEC_SERVICE.schedule(cache::cleanUp, 1, TimeUnit.MINUTES);

        PythonGatewayCreationGate.INSTANCE.registerListener(new PythonGatewayCreationGateListener() {
            @Override
            public void onPythonGatewayCreationGateOpen() {
                // Nothing to do here. Kernel creation is blocked anyways in createPythonNodeFromCache()
                // while gate is closed.
            }

            @Override
            public void onPythonGatewayCreationGateClose() {
                synchronized (GATEWAY_CACHE) {
                    GATEWAY_CACHE.invalidateAll();
                }
            }
        });
        return cache;
    }

    @SuppressWarnings("resource") // the value is closed if it isn't used anymore, otherwise the user has to close it
    private static void onRemoveFromCache(final RemovalNotification<ResolvedPythonExtension, //
            CachedObject<PythonGateway<KnimeNodeBackend>>> notification) {
        try {
            notification.getValue().removeFromCache();
        } catch (IOException ex) {
            LOGGER.error("Failed to close PythonGateway.", ex);
        }
    }

    public static void close() {
        synchronized (GATEWAY_CACHE) {
            gatewayCacheClosed = true;
            GATEWAY_CACHE.invalidateAll();
        }
    }

    @Override
    public NodeConfigurationProxy getConfigurationProxy() {
        return createPythonNodeFromCache();
    }

    @Override
    public CloseableNodeFactoryProxy getNodeFactoryProxy() {
        return createPythonNodeFromCache();
    }

    @Override
    public NodeDialogProxy getNodeDialogProxy() {
        return createPythonNodeFromCache();
    }

    @SuppressWarnings("resource") // the gateway is managed by the returned object
    private CloseablePythonNodeProxy createPythonNodeFromCache() {
        try {
            awaitGatewayCreationAllowed();
            CachedObject<PythonGateway<KnimeNodeBackend>> cachedGateway;
            synchronized (GATEWAY_CACHE) {
                if (gatewayCacheClosed) {
                    throw new IllegalStateException("Gateway cache has been closed.");
                }
                cachedGateway = GATEWAY_CACHE.get(m_extension);
                cachedGateway.markAsUsed();
            }
            var gateway = new CachedPythonNodeGateway(cachedGateway);
            return m_proxyFactory.createProxy(gateway);
        } catch (ExecutionException ex) {// NOSONAR ExecutionException is just a wrapper
            var cause = ex.getCause();
            if (cause instanceof InterruptedException) {
                throw new IllegalStateException("Interrupted while creating Python gateway.", cause);
            } else {
                throw new IllegalStateException("Failed to initialize Python gateway.", cause);
            }
        }
    }

    private static void awaitGatewayCreationAllowed() {
        try {
            PythonGatewayCreationGate.INSTANCE.awaitPythonGatewayCreationAllowedInterruptibly();
        } catch (InterruptedException ex) { // NOSONAR
            throw new IllegalStateException("Interrupted while waiting until Python processes may be started", ex);
        }
    }

    private static final class CachedPythonNodeGateway implements PythonGateway<KnimeNodeBackend> {

        private final CachedObject<PythonGateway<KnimeNodeBackend>> m_cachedGateway;

        private final PythonGateway<KnimeNodeBackend> m_delegate;

        CachedPythonNodeGateway(final CachedObject<PythonGateway<KnimeNodeBackend>> cachedGateway) {
            m_cachedGateway = cachedGateway;
            m_delegate = cachedGateway.get();
        }

        @Override
        public KnimeNodeBackend getEntryPoint() {
            return m_delegate.getEntryPoint();
        }

        @Override
        public InputStream getStandardOutputStream() {
            return m_delegate.getStandardOutputStream();
        }

        @Override
        public InputStream getStandardErrorStream() {
            return m_delegate.getStandardErrorStream();
        }

        @Override
        public void close() throws IOException {
            m_cachedGateway.close();
        }
    }

}
