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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A wrapper around an object that is cached.
 * Handles the scenario where an {@link AutoCloseable} is removed from a cache but still used by some client.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class CachedObject<T extends AutoCloseable> implements AutoCloseable {

    private final AtomicBoolean m_isCached = new AtomicBoolean(true);

    private final AtomicBoolean m_isUsed = new AtomicBoolean(false);

    private final T m_closeable;

    CachedObject(final T closeable) {
        m_closeable = closeable;
    }

    void markAsUsed() {
        if (!m_isCached.get()) {
            throw new IllegalStateException("A CachedObject may only be used if it is in the cache.");
        }
        m_isUsed.set(true);
    }

    T get() {
        return m_closeable;
    }

    /**
     * Needs to be called when the object is evicted from the cache.
     * Marks this object as no longer cached and closes the delegate if it is no longer in use
     * @throws Exception if the close of the delegate throws an exception
     */
    void removeFromCache() throws Exception {
        if (!m_isCached.getAndSet(false)) {
            throw new IllegalStateException("The current implementation allows for only one removal from the cache.");
        }
        if (!m_isUsed.get()) {
            closeInternal();
        }
    }

    /**
     * The delegate is only closed if it isn't cached anymore.
     */
    @Override
    public void close() throws Exception {
        m_isUsed.set(false);
        if (!m_isCached.get()) {
            closeInternal();
        }
    }

    private void closeInternal() throws Exception {//NOSONAR signature of AutoCloseable
        m_closeable.close();
    }


}
