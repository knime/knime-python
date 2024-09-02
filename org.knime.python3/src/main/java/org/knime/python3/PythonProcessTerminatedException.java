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
 *   Sep 2, 2024 (benjamin): created
 */
package org.knime.python3;

import java.util.Optional;

import org.knime.core.node.KNIMEException;
import org.knime.core.node.message.MessageBuilder;

import py4j.Py4JException;
import py4j.Py4JNetworkException;

/**
 * Exception that is thrown if the Python process got terminated.
 *
 * TODO(AP-23257) move the creation of this exception into the PythonGateway
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public class PythonProcessTerminatedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final Optional<String> m_resolution;

    private PythonProcessTerminatedException(final String message, final Throwable cause) {
        this(message, null, cause);
    }

    private PythonProcessTerminatedException(final String message, final String resolution, final Throwable cause) {
        super(message, cause);
        m_resolution = Optional.ofNullable(resolution);
    }

    /**
     * Throw a {@link PythonProcessTerminatedException} if the given {@link Py4JException} is caused by a terminated
     * Python process.
     *
     * @param gateway the gateway associated with the entry point that threw the exception
     * @param exception the Py4JException that was thrown
     * @throws PythonProcessTerminatedException
     */
    public static void throwIfTerminated(final PythonGateway<?> gateway, final Py4JException exception)
        throws PythonProcessTerminatedException {
        var ex = ifTerminated(gateway, exception);
        if (ex.isPresent()) {
            throw ex.get();
        }
    }

    /**
     * Get a {@link PythonProcessTerminatedException} if the given {@link Py4JException} is caused by a terminated
     * Python process.
     *
     * @param gateway the gateway associated with the entry point that threw the exception
     * @param exception the Py4JException that was thrown
     * @return an {@link Optional} containing the exception if the given exception was caused by a terminated Python
     */
    public static Optional<PythonProcessTerminatedException> ifTerminated(final PythonGateway<?> gateway,
        final Py4JException exception) {
        if (exception.getCause() instanceof Py4JNetworkException) {
            var terminationReason = gateway.getTerminationReason();
            if (terminationReason != null) {
                return Optional.of(new PythonProcessOOMException(terminationReason, exception));
            } else {
                return Optional
                    .of(new PythonProcessTerminatedException("The Python process got terminated.", exception));
            }
        }
        return Optional.empty();
    }

    /**
     * Converts this exception to a {@link KNIMEException} with the given {@link MessageBuilder}.
     *
     * @param messageBuilder the message builder to use for creating the KNIMEException
     * @return the KNIMEException
     */
    public KNIMEException toKNIMEException(final MessageBuilder messageBuilder) {
        messageBuilder.withSummary(getMessage());
        m_resolution.ifPresent(messageBuilder::addResolutions);
        return KNIMEException.of(messageBuilder.build().orElseThrow(), this);
    }

    /** Exception that is thrown if the Python process got terminated by the watchdog because it ran out of memory. */
    public static final class PythonProcessOOMException extends PythonProcessTerminatedException {

        private static final String RESOLUTION =
            "This can happen if the system ran out of memory, increase the system resources and try again.";

        private static final long serialVersionUID = 1L;

        PythonProcessOOMException(final String message, final Throwable cause) {
            super(message, RESOLUTION, cause);
        }
    }
}
