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
 *   May 13, 2018 (marcel): created
 */
package org.knime.python2.kernel.messaging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import org.knime.python2.kernel.messaging.DefaultMessage.PayloadEncoder;
import org.knime.python2.util.PythonNodeLogger;

/**
 * Convenience base class for {@link MessageHandler message handlers} that answer a single request with a single
 * response.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractRequestHandler extends AbstractTaskHandler<Void> {

    private static final PythonNodeLogger LOGGER = PythonNodeLogger.getLogger(AbstractRequestHandler.class);

    /**
     * Note: This method invokes {@link DefaultMessage#DefaultMessage(int, String, byte[], Map)} whose constraints apply
     * to the arguments given here.
     *
     * @param request the message for which the response is created
     * @param responseMessageId the message id to use for the response
     * @param success <code>true</code> if the response indicates success, <code>false</code> if it indicates failure
     * @param responsePayload the response's payload
     * @param responseAdditionalOptions custom header fields to include in the response, must not contain field key
     *            {@link AbstractTaskHandler#FIELD_KEY_MESSAGE_TYPE} (in addition to the constraints of
     *            {@link DefaultMessage}). These conditions are not checked and must be ensured by the caller.
     * @return the created response
     */
    protected static Message createResponse(final Message request, final int responseMessageId, final boolean success,
        final byte[] responsePayload, final Map<String, String> responseAdditionalOptions) {
        String responseCategory = request.getHeaderField(FIELD_KEY_REPLY_TO);
        if (responseCategory == null) {
            responseCategory = Integer.toString(request.getId());
        }
        final Map<String, String> responseOptions =
            responseAdditionalOptions != null ? responseAdditionalOptions : new HashMap<>(1);
        responseOptions.put(FIELD_KEY_MESSAGE_TYPE, success ? MESSAGE_TYPE_SUCCESS : MESSAGE_TYPE_FAILURE);
        return new DefaultMessage(responseMessageId, responseCategory, responsePayload, responseOptions);
    }

    /**
     * Responds to the given request using the given response message id. You can use
     * {@link #createResponse(Message, int, boolean, byte[], Map)} to create the response.
     *
     * @param request the message to which to respond
     * @param responseMessageId the message id to use for the response
     * @return the response, may not be <code>null</code> (i.e. the given request must be answered, everything else
     *         would hurt the request/response protocol. Throw an exception if giving a response is not possible.)
     * @throws Exception if any exception occurs while responding to the request or if giving a response is not possible
     */
    protected abstract Message respond(Message request, int responseMessageId) throws Exception;

    @Override
    protected boolean handleCustomMessage(final Message message, final IntSupplier responseMessageIdSupplier,
        final Consumer<Message> responseConsumer, final Consumer<Void> resultConsumer) throws Exception {
        final int responseMessageId = responseMessageIdSupplier.getAsInt();
        try {
            LOGGER.debug("Java - Respond to message: " + message);
            Message response = respond(message, responseMessageId);
            LOGGER.debug("Java - Responded to message: " + message + ", response: " + response);
            responseConsumer.accept(response);
        } catch (Exception ex) {
            LOGGER.debug(ex);
            final String errorMessage = ex.getMessage();
            final StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            final String errorTraceback = sw.toString();
            final byte[] errorPayload = new PayloadEncoder().putString(errorMessage).putString(errorTraceback).get();
            // Inform Python that handling the request did not work.
            final Message errorResponse = createResponse(message, responseMessageId, false, errorPayload, null);
            responseConsumer.accept(errorResponse);
        }
        resultConsumer.accept(null); // We are done after the response (either success or failure) is sent.
        return true;
    }
}
