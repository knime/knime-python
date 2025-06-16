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
 *   May 21, 2025 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import java.io.IOException;

import org.knime.core.data.filestore.FileStoreKey;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.util.auth.CouldNotAuthorizeException;
import org.knime.python3.PythonFileStoreUtils;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.knime.python3.nodes.callback.AuthCallbackUtils;
import org.knime.python3.nodes.proxy.PythonNodeModelProxy.ExpiryDate;
import org.knime.python3.nodes.proxy.PythonNodeViewProxy;

/**
 * Default implementation of the {@link PythonNodeViewProxy.ViewCallback} interface used by the
 * CloseablePythonNodeProxy.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class DefaultViewCallback implements PythonNodeViewProxy.ViewCallback {
    private final LogCallback m_logCallback;

    private final PythonArrowTableConverter m_tableManager;

    private final IFileStoreHandler m_filestoreHandler;

    DefaultViewCallback(final PythonArrowTableConverter tableManager, final LogCallback logCallback,
        final IFileStoreHandler filestoreHandler) {
        m_tableManager = tableManager;
        m_logCallback = logCallback;
        m_filestoreHandler = filestoreHandler;
    }

    @Override
    public void log(final String message, final String severity) {
        m_logCallback.log(message, severity);
    }

    @Override
    public ExpiryDate get_expires_after(final String serializedXMLString) throws CouldNotAuthorizeException,
        ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        return AuthCallbackUtils.getExpiresAfter(serializedXMLString);
    }

    @Override
    public String get_auth_schema(final String serializedXMLString) throws CouldNotAuthorizeException,
        ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        return AuthCallbackUtils.getAuthSchema(serializedXMLString);
    }

    @Override
    public String get_auth_parameters(final String serializedXMLString) throws CouldNotAuthorizeException,
        ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        return AuthCallbackUtils.getAuthParameters(serializedXMLString);
    }

    @Override
    public String file_store_key_to_absolute_path(final String fileStoreKey) { // NOSONAR
        return PythonFileStoreUtils.getAbsolutePathForKey(m_filestoreHandler, FileStoreKey.load(fileStoreKey));
    }

    @Override
    public PythonArrowDataSink create_sink() throws IOException {
        return m_tableManager.createSink();
    }
}