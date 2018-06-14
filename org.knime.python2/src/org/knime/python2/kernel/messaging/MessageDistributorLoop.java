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
 *   May 18, 2018 (marcel): created
 */
package org.knime.python2.kernel.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.knime.python2.kernel.PythonExecutionMonitor;
import org.knime.python2.util.PythonUtils;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
final class MessageDistributorLoop extends AbstractMessageLoop {

    private final MessageReceiver m_receiver;

    private final Map<String, MessageHandler> m_messageHandlers = Collections.synchronizedMap(new HashMap<>());

    private boolean m_messageHandlersClosed = false;

    public MessageDistributorLoop(final MessageReceiver receiver, final PythonExecutionMonitor monitor) {
        super(monitor, "python-message-distribute-loop");
        m_receiver = receiver;
    }

    @Override
    protected void loop() throws Exception {
        while (isRunning()) {
            Message message = null;
            try {
                message = m_receiver.receive();
                final MessageHandler messageHandler = m_messageHandlers.get(message.getCategory());
                if (messageHandler != null) {
                    LOGGER.debug("Java - Distribute message: " + message);
                    messageHandler.handle(message);
                } else {
                    throw new IllegalStateException(
                        "Message '" + message + "' cannot be distributed. No matching handler available for category '"
                            + message.getCategory() + "'.");
                }
            } catch (final Exception ex) {
                throwExceptionInLoop(
                    "Failed to distribute message " + (message != null ? "'" + message + "' " : "") + "from Python.",
                    ex);
            }
        }
    }

    @Override
    protected void closeInternal() throws Exception {
        synchronized (m_messageHandlers) {
            m_messageHandlersClosed = true;
            // Handlers may want to unregister upon closing. Copy to avoid concurrent modification.
            final MessageHandler[] handlers = m_messageHandlers.values().toArray(new MessageHandler[0]);
            closeMessageHandlers(handlers);
        }
    }

    boolean registerMessageHandler(final String messageCategory, final MessageHandler handler) {
        synchronized (m_messageHandlers) {
            Exception exception = null;
            boolean registered = false;
            try {
                registered =
                    m_messageHandlers.putIfAbsent(checkNotNull(messageCategory), checkNotNull(handler)) == null;
            } catch (final Exception ex) {
                exception = ex;
            }
            if (m_messageHandlersClosed) {
                closeMessageHandlers(handler);
            }
            if (exception != null) {
                throw new RuntimeException(exception.getMessage(), exception);
            } else {
                return registered;
            }
        }
    }

    boolean unregisterMessageHandler(final String messageCategory) {
        return m_messageHandlers.remove(checkNotNull(messageCategory)) != null;
    }

    boolean canHandle(final String messageCategory) {
        return m_messageHandlers.containsKey(messageCategory);
    }

    private void closeMessageHandlers(final MessageHandler... handlers) {
        final Error error = PythonUtils.Misc.invokeSafely(LOGGER::debug, h -> {
            try {
                h.handle(m_monitor.getPoisonPill());
            } catch (final Exception ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }, handlers);
        if (error != null) {
            throw error;
        }
    }
}
