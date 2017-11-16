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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
 *   Nov 16, 2017 (marcel): created
 */
package org.knime.python2.kernel;

import java.io.IOException;

/**
 * Interface for message-based communication between Python and Java.<br>
 * This complements the unidirectional "execute and fetch results" functionality provided by {@link Commands} by means
 * of a bidirectional "message and response" mechanism.
 * <P>
 * {@link PythonToJavaMessage} represents a message that can be sent by Python during execution of a command and is
 * received by Java. A message can be a {@link PythonToJavaMessage#isRequest() request} in which case it requires a
 * {@link JavaToPythonResponse} from Java. On Java side, the message has to be handled by a registered
 * {@link PythonToJavaMessageHandler} and - if it is a request - has to be {@link #answer(JavaToPythonResponse)
 * answered} during {@link PythonToJavaMessageHandler#tryHandle(PythonToJavaMessage) handling}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
public interface Messages {

    /**
     * Register a handler for dealing with a subset of possible {@link PythonToJavaMessage}s from Python. If it is
     * already present in the internal list, nothing happens.
     *
     * @param handler handles {@link PythonToJavaMessage}s having a specific command
     */
    void registerMessageHandler(PythonToJavaMessageHandler handler);

    /**
     * Unregister an existing {@link PythonToJavaMessageHandler}. If it is not present in the internal list, nothing
     * happens.
     *
     * @param handler a {@link PythonToJavaMessageHandler}
     */
    void unregisterMessageHandler(final PythonToJavaMessageHandler handler);

    /**
     * Sends a {@link JavaToPythonResponse} to a specific {@link PythonToJavaMessage} to Python.
     *
     * @param response the response to send to Python
     * @throws IOException if any exception occurs while answering
     */
    void answer(final JavaToPythonResponse response) throws IOException;
}
