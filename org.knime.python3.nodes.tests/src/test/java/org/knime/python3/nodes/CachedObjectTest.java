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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Contains unit tests for CachedObject.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@RunWith(MockitoJUnitRunner.class)
public class CachedObjectTest {

    @Mock
    private AutoCloseable m_closeable;

    /**
     * When a CachedObject is no longer (or was never) used and is evicted from the cache, then it should close its
     * underlying closeable.
     *
     * @throws Exception not thrown here
     */
    @Test
    public void testRemovingUnusedObjectFromCacheTriggersClose() throws Exception {
        @SuppressWarnings("resource")
        var cachedObject = new CachedObject<>(m_closeable);
        cachedObject.removeFromCache();
        verify(m_closeable).close();
    }

    /**
     * When a CachedObject is closed (i.e. no longer used), then it should not close the underlying closeable because it
     * may be reused at a later point in time.
     *
     * @throws Exception never thrown here
     */
    @Test
    public void testClosingCachedObjectDoesNotTriggerClose() throws Exception {
        @SuppressWarnings("resource")
        var cached = new CachedObject<>(m_closeable);
        cached.markAsUsed();
        cached.close();
        verify(m_closeable, never()).close();
        cached.removeFromCache();
        verify(m_closeable).close();
    }

    /**
     * If a CachedObject is no longer cached, then closing it should close the underlying closeable.
     *
     * @throws Exception never thrown here
     */
    @Test
    public void testCloseTriggersCloseIfNoLongerCached() throws Exception {
        try (var cached = new CachedObject<>(m_closeable)) {
            cached.markAsUsed();
            cached.removeFromCache();
            verify(m_closeable, never()).close();
        }
        verify(m_closeable).close();
    }

    /**
     * CachedObject should be reusable as long as it is in the cache.
     *
     * @throws Exception not thrown here
     */
    @Test
    public void testRecyclingOfCachedObject() throws Exception {
        @SuppressWarnings("resource")
        var cached = new CachedObject<>(m_closeable);
        cached.markAsUsed();
        // marks cached as unused
        cached.close();
        verify(m_closeable, never()).close();
        cached.markAsUsed();
        cached.close();
        verify(m_closeable, never()).close();
        cached.removeFromCache();
        verify(m_closeable).close();
    }

    /**
     * A CachedObject may only be reused if it is still in the cache. Otherwise an IllegalStateException is thrown.
     *
     * @throws Exception not thrown here
     */
    @Test(expected = IllegalStateException.class)
    public void testReusingAfterCacheEvictionThrows() throws Exception {
        @SuppressWarnings("resource")
        var cached = new CachedObject<>(m_closeable);
        cached.removeFromCache();
        cached.markAsUsed();
    }

}
