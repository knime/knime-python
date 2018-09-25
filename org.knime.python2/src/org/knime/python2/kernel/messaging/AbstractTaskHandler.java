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
 *   May 22, 2018 (marcel): created
 */
package org.knime.python2.kernel.messaging;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import org.knime.python2.kernel.PythonExecutionException;
import org.knime.python2.kernel.messaging.DefaultMessage.PayloadDecoder;
import org.knime.python2.util.PythonNodeLogger;

/**
 * Abstract base class for implementations of {@link TaskHandler}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractTaskHandler<T> implements TaskHandler<T> {

    public static final String FIELD_KEY_MESSAGE_TYPE = "type";

    public static final String MESSAGE_TYPE_SUCCESS = "success";

    public static final String MESSAGE_TYPE_FAILURE = "failure";

    public static final String FIELD_KEY_REPLY_TO = "reply-to";

    private static final PythonNodeLogger LOGGER = PythonNodeLogger.getLogger(AbstractTaskHandler.class);

    @Override
    public Message handle(final Message message, final MessageHandlerCollection messageHandlers,
        final IntSupplier messageIdSupplier, final Consumer<T> resultConsumer) throws Exception {
        final String messageType = message.getHeaderField(FIELD_KEY_MESSAGE_TYPE); // Nullable.

        LOGGER.debug("Java - Handle task, message: " + message);

        if (MESSAGE_TYPE_SUCCESS.equals(messageType)) {
            final T result = handleSuccessMessage(message);

            LOGGER.debug("Java - Handled task, message: " + message + ", result: " + result);

            resultConsumer.accept(result);
        } else if (MESSAGE_TYPE_FAILURE.equals(messageType)) {
            handleFailureMessage(message);
        } else {
            final AtomicReference<Message> messageToSend = new AtomicReference<>();
            if (handleCustomMessage(message, messageIdSupplier, messageToSend::set, resultConsumer)) {
                Message messageToSendObj = messageToSend.get();

                LOGGER.debug("Java - Handled task, message: " + message + ", follow-up: " + messageToSendObj);

                return messageToSendObj;
            } else {
                if (!messageHandlers.handle(message)) {
                    throw new IllegalStateException("Message '" + message + "' could not be handled.");
                }
            }
        }
        return null;
    }

    /**
     * Called to handle a {@link #MESSAGE_TYPE_SUCCESS success} message.
     *
     * @param message the success message to handle
     * @return the result that is obtain by handling the given message, may be <code>null</code>
     * @throws Exception if any exception occurs while handling the message
     */
    protected T handleSuccessMessage(final Message message) throws Exception {
        return null;
    }

    /**
     * Called to handle a {@link #MESSAGE_TYPE_FAILURE failure} message. A usual implementation would be to throw an
     * exception that features an informative error message.
     *
     * @param message the failure message to handle
     * @throws Exception if any exception occurs while handling the message, e.g. an exception that explains the failure
     */
    protected void handleFailureMessage(final Message message) throws Exception {
        final String errorMessage;
        if (message.getPayload() != null) {
            errorMessage = new PayloadDecoder(message.getPayload()).getNextString();
        } else {
            errorMessage = "Python task failed for unknown reasons.";
        }
        throw new PythonExecutionException(errorMessage);
    }

    /**
     * Called to handle a custom message (i.e. a message that is neither a {@link #MESSAGE_TYPE_SUCCESS success} nor a
     * {@link #MESSAGE_TYPE_FAILURE failure} message). Implementations can respond to the message by creating a message
     * (possibly using {@code responseMessageIdSupplier} to obtain a new message id) and passing it to
     * {@code responseConsumer} but do not need to. They can also also indicate that the message could not be handled by
     * returning <code>false</code>, in which case it will be forwarded to other handlers, or throw an exception to
     * signal that handling failed.
     *
     * @param message the custom message to handle
     * @param responseMessageIdSupplier can be used to obtain a new message id for a response
     * @param responseConsumer can be used to send a response
     * @param resultConsumer can be used to set a result (possibly <code>null</code>) to indicate that handling is done
     * @return <code>true</code> if the custom message could be handled, <code>false</code> otherwise
     * @throws Exception if any exception occurs while handling the message
     */
    protected boolean handleCustomMessage(final Message message, final IntSupplier responseMessageIdSupplier,
        final Consumer<Message> responseConsumer, final Consumer<T> resultConsumer) throws Exception {
        throw new UnsupportedOperationException("Custom message types are not supported by this task.");
    }
}
