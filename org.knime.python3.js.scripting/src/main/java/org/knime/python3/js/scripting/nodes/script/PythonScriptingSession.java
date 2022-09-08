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
 *   Aug 11, 2022 (benjamin): created
 */
package org.knime.python3.js.scripting.nodes.script;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.util.ThreadUtils;
import org.knime.python3.Activator;
import org.knime.python3.Python3SourceDirectory;
import org.knime.python3.PythonCommand;
import org.knime.python3.PythonGateway;
import org.knime.python3.PythonGatewayFactory.PythonGatewayDescription;
import org.knime.python3.arrow.Python3ArrowSourceDirectory;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.knime.python3.js.scripting.PythonJsScriptingEntryPoint;
import org.knime.python3.js.scripting.PythonJsScriptingSourceDirectory;
import org.knime.python3.js.scripting.Utf8StreamConsumer;
import org.knime.python3.types.PythonValueFactoryModule;
import org.knime.python3.types.PythonValueFactoryRegistry;
import org.knime.python3.views.Python3ViewsSourceDirectory;
import org.knime.scripting.editor.ScriptingService.ConsoleText;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A running Python process that is used to execute scripts in the context of the Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
final class PythonScriptingSession implements AutoCloseable {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonScriptingSession.class);

    private static final ExecutorService EXECUTOR_SERVICE = ThreadUtils.executorServiceWithContext(
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("python-js-scripting-%d").build()));

    private static final ArrowColumnStoreFactory ARROW_STORE_FACTORY = new ArrowColumnStoreFactory();

    private static final Path LAUNCHER = PythonJsScriptingSourceDirectory.getPath()//
        .resolve("knime_js_scripting.py");

    private final PythonGateway<PythonJsScriptingEntryPoint> m_gateway;

    private final PythonJsScriptingEntryPoint m_entryPoint;

    private final PythonArrowTableConverter m_tableConverter;

    private final Consumer<ConsoleText> m_consoleTextConsumer;

    PythonScriptingSession(final PythonCommand pythonCommand, final Consumer<ConsoleText> consoleTextConsumer,
        final IWriteFileStoreHandler fileStoreHandler) throws IOException, InterruptedException {
        m_gateway = createGateway(pythonCommand);
        m_entryPoint = m_gateway.getEntryPoint();
        m_tableConverter = new PythonArrowTableConverter(EXECUTOR_SERVICE, ARROW_STORE_FACTORY, fileStoreHandler);
//
//        @SuppressWarnings("resource") // The stdout stream of a process doesn't have to be closed
//        final var stdout = m_gateway.getStandardOutputStream();
//        @SuppressWarnings("resource") // The stderr stream of a process doesn't have to be closed
//        final var stderr = m_gateway.getStandardErrorStream();
//        startStreamConsumer(stdout, stderr, consoleTextConsumer);
        m_consoleTextConsumer = consoleTextConsumer;
    }

    void setupIO(final PortObject[] inData, final int numOutPorts, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // TODO(AP-19337) support all kinds of input and output ports
        final BufferedDataTable[] inTables =
            Arrays.stream(inData).map(BufferedDataTable.class::cast).toArray(BufferedDataTable[]::new);
        final var sources = m_tableConverter.createSources(inTables, exec);
        final var callback = new PythonJsScriptingEntryPoint.Callback() {
            @Override
            public PythonArrowDataSink create_sink() throws IOException {
                return m_tableConverter.createSink();
            }

            @Override
            public void add_stdout(final String text) {
                var str = text.replace("\n", "\\n");
                str = str.replace("\r", "\\r");
                LOGGER.warn(System.currentTimeMillis() + ", stdout: " + str);
                m_consoleTextConsumer.accept(new ConsoleText(text, false));
            }

            @Override
            public void add_stderr(final String text) {
                var str = text.replace("\n", "\\n");
                str = str.replace("\r", "\\r");
                LOGGER.warn(System.currentTimeMillis() + ", stderr: " + str);
                m_consoleTextConsumer.accept(new ConsoleText(text, true));
            }
        };
        m_entryPoint.setupIO(sources, numOutPorts, callback);
    }

    String execute(final String script) {
        return m_entryPoint.execute(script);
    }

    BufferedDataTable[] getOutputs(final boolean allowIncomplete, final ExecutionContext exec)
        // TODO(AP-19337) support all kinds of output ports
        throws IOException, CanceledExecutionException {
        final List<PythonArrowDataSink> sinks = m_entryPoint.getOutputs(allowIncomplete);
        return m_tableConverter.convertToTables(sinks, exec.createSubExecutionContext(0.3));
    }

    private static PythonGateway<PythonJsScriptingEntryPoint> createGateway(final PythonCommand pythonCommand)
        throws IOException, InterruptedException {
        // TODO(AP-19430) set working directory to workflow dir
        final var gatewayDescriptionBuilder = PythonGatewayDescription
            .builder(pythonCommand, LAUNCHER.toAbsolutePath(), PythonJsScriptingEntryPoint.class) //
            .addToPythonPath(Python3SourceDirectory.getPath()) //
            .addToPythonPath(Python3ArrowSourceDirectory.getPath()) //
            .addToPythonPath(Python3ViewsSourceDirectory.getPath());

        // Add the paths to the extension types
        PythonValueFactoryRegistry.getModules().stream() //
            .map(PythonValueFactoryModule::getParentDirectory) //
            .forEach(gatewayDescriptionBuilder::addToPythonPath);

        return Activator.GATEWAY_FACTORY.create(gatewayDescriptionBuilder.build());
    }

    @Override
    public void close() throws Exception {
        m_tableConverter.close();
        m_gateway.close();
    }

    private static void startStreamConsumer(final InputStream stdout, final InputStream stderr,
        final Consumer<ConsoleText> consumer) {
        // NB: We set the charset of the stream to UTF-8 on Python side
        // NB: The threads end automatically whenever the Python process dies
        EXECUTOR_SERVICE.submit(new Utf8StreamConsumer( //
            stdout, //
            s -> consumer.accept(new ConsoleText(s, false)), //
            e -> LOGGER.error("Reading the Python process stdout failed: " + e.getMessage(), e) //
        ));
        EXECUTOR_SERVICE.submit(new Utf8StreamConsumer( //
            stderr, //
            s -> consumer.accept(new ConsoleText(s, true)), //
            e -> LOGGER.error("Reading the Python process stderr failed: " + e.getMessage(), e) //
        ));
    }
}
