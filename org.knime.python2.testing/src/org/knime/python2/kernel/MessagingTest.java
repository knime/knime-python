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
 */
package org.knime.python2.kernel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.node.CanceledExecutionException;
import org.knime.python2.kernel.PythonKernelOptions.PythonVersionOption;
import org.knime.python2.kernel.messaging.AbstractRequestHandler;
import org.knime.python2.kernel.messaging.AbstractTaskHandler;
import org.knime.python2.kernel.messaging.DefaultMessage;
import org.knime.python2.kernel.messaging.DefaultMessage.PayloadDecoder;
import org.knime.python2.kernel.messaging.DefaultMessage.PayloadEncoder;
import org.knime.python2.kernel.messaging.Message;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class MessagingTest {

	private PythonKernel m_kernel;

	@Before
	public void setup() throws IOException {
		final PythonKernelOptions kernelOptions = new PythonKernelOptions();
		kernelOptions.setPythonVersionOption(PythonVersionOption.PYTHON3);
		m_kernel = new PythonKernel(kernelOptions);
	}

	@After
	public void cleanup() throws IOException {
		m_kernel.close();
	}

	@Test
	public void testRequestFromJavaToPython()
			throws IOException, CanceledExecutionException, InterruptedException, ExecutionException {
		final String setupSourceCode = "import python3.messaging.testing.MessagingTest as MessagingTest\n" //
				+ "MessagingTest.test_request_from_java_to_python(globals()['workspace'])";
		// Use cancelable method overload to throw exception on error.
		m_kernel.execute(setupSourceCode, PythonCancelable.NOT_CANCELABLE);

		final RunnableFuture<String> myTask = m_kernel.getCommands().createTask(new AbstractTaskHandler<String>() {

			@Override
			protected String handleSuccessMessage(final Message message) throws Exception {
				return new PayloadDecoder(message.getPayload()).getNextString();
			}
		}, new DefaultMessage(m_kernel.getCommands().getMessaging().createNextMessageId(), "my-request-from-java", null,
				null));

		Assert.assertEquals("my-response-from-python", myTask.get());
	}

	@Test
	public void testRequestFromPythonToJava()
			throws IOException, CanceledExecutionException, InterruptedException, ExecutionException {
		try {
			m_kernel.registerTaskHandler("my-request-from-python", new AbstractRequestHandler() {

				@Override
				protected Message respond(final Message request, final int responseMessageId) throws Exception {
					final byte[] payload = new PayloadEncoder().putString("my-response-from-java").get();
					return createResponse(request, responseMessageId, true, payload, null);
				}
			});

			final String sourceCode = "import python3.messaging.testing.MessagingTest as MessagingTest\n" //
					+ "MessagingTest.test_request_from_python_to_java(globals()['workspace'])";
			final String[] output = m_kernel.execute(sourceCode, PythonCancelable.NOT_CANCELABLE);
			Assert.assertTrue(output[0].contains("my-response-from-java"));
		} finally {
			m_kernel.unregisterTaskHandler("my-request-from-python");
		}
	}

	@Test
	public void testNestedRequestFromJavaToPython()
			throws IOException, CanceledExecutionException, InterruptedException, ExecutionException {
		final String setupSourceCode = "import python3.messaging.testing.MessagingTest as MessagingTest\n" //
				+ "MessagingTest.test_nested_request_from_java_to_python(globals()['workspace'])";
		m_kernel.execute(setupSourceCode, PythonCancelable.NOT_CANCELABLE);

		final RunnableFuture<String> myTask = m_kernel.getCommands().createTask(new AbstractTaskHandler<String>() {

			@Override
			protected String handleSuccessMessage(final Message message) throws Exception {
				return new PayloadDecoder(message.getPayload()).getNextString();
			}

			@Override
			protected boolean handleCustomMessage(final Message message, final IntSupplier responseMessageIdSupplier,
					final Consumer<Message> responseConsumer, final Consumer<String> resultConsumer) throws Exception {
				String responseCategory = message.getHeaderField(FIELD_KEY_REPLY_TO);
				if (responseCategory == null) {
					responseCategory = Integer.toString(message.getId());
				}
				final String messageType = message.getHeaderField(FIELD_KEY_MESSAGE_TYPE);
				final String responseStringPayload;
				final String responseMessageType;
				if (messageType.equals("first-request")) {
					responseStringPayload = "first-response";
					responseMessageType = "first";
				} else if (messageType.equals("second-request")) {
					responseStringPayload = "-second-response";
					responseMessageType = "second";
				} else {
					throw new IllegalStateException();
				}
				final Map<String, String> additionalOptions = new HashMap<>(1);
				additionalOptions.put(FIELD_KEY_MESSAGE_TYPE, responseMessageType);
				final Message response = new DefaultMessage(responseMessageIdSupplier.getAsInt(), responseCategory,
						new PayloadEncoder().putString(responseStringPayload).get(), additionalOptions);
				responseConsumer.accept(response);
				return true;
			}
		}, new DefaultMessage(m_kernel.getCommands().getMessaging().createNextMessageId(),
				"my-nested-request-from-java", null, null));

		Assert.assertEquals("first-response-second-response", myTask.get());
	}

	@Test
	public void testRequestFromJavaThatCausesRequestFromPython()
			throws IOException, CanceledExecutionException, InterruptedException, ExecutionException {
		try {
			m_kernel.registerTaskHandler("caused-request-from-python", new AbstractRequestHandler() {

				@Override
				protected Message respond(final Message request, final int responseMessageId) throws Exception {
					final byte[] payload = new PayloadEncoder().putString("my-response-to-the-caused-request").get();
					return createResponse(request, responseMessageId, true, payload, null);
				}
			});

			final String setupSourceCode = "import python3.messaging.testing.MessagingTest as MessagingTest\n" //
					+ "MessagingTest.test_request_from_java_that_causes_request_from_python(globals()['workspace'])";
			m_kernel.execute(setupSourceCode, PythonCancelable.NOT_CANCELABLE);

			final RunnableFuture<String> myTask = m_kernel.getCommands().createTask(new AbstractTaskHandler<String>() {

				@Override
				protected String handleSuccessMessage(final Message message) throws Exception {
					return new PayloadDecoder(message.getPayload()).getNextString();
				}
			}, new DefaultMessage(m_kernel.getCommands().getMessaging().createNextMessageId(),
					"my-request-from-java-that-causes-a-request-from-python", null, null));

			Assert.assertEquals("my-response-to-the-caused-request-made-the-task-succeed", myTask.get());
		} finally {
			m_kernel.unregisterTaskHandler("caused-request-from-python");
		}
	}
}
