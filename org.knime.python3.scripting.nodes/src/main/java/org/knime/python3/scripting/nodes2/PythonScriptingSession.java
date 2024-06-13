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
package org.knime.python3.scripting.nodes2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.commons.lang3.SystemUtils;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.data.filestore.FileStoreKey;
import org.knime.core.data.filestore.FileStoreUtil;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.util.FileUtil;
import org.knime.core.util.ThreadUtils;
import org.knime.core.util.asynclose.AsynchronousCloseable;
import org.knime.core.util.pathresolve.ResolverUtil;
import org.knime.python3.Activator;
import org.knime.python3.Python3SourceDirectory;
import org.knime.python3.PythonCommand;
import org.knime.python3.PythonEntryPointUtils;
import org.knime.python3.PythonFileStoreUtils;
import org.knime.python3.PythonGateway;
import org.knime.python3.PythonGatewayFactory.EntryPointCustomizer;
import org.knime.python3.PythonGatewayFactory.PythonGatewayDescription;
import org.knime.python3.PythonGatewayUtils;
import org.knime.python3.arrow.Python3ArrowSourceDirectory;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.arrow.PythonArrowExtension;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.knime.python3.types.PythonValueFactoryModule;
import org.knime.python3.types.PythonValueFactoryRegistry;
import org.knime.python3.utils.FlowVariableUtils;
import org.knime.python3.utils.ProxyUtils;
import org.knime.python3.views.Python3ViewsSourceDirectory;
import org.knime.python3.views.PythonViewsExtension;
import org.knime.scripting.editor.ScriptingService.ConsoleText;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;

/**
 * A running Python process that is used to execute scripts in the context of the Python scripting node.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
final class PythonScriptingSession implements AsynchronousCloseable<IOException> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PythonScriptingSession.class);

    private static final ExecutorService EXECUTOR_SERVICE = ThreadUtils.executorServiceWithContext(
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("python-js-scripting-%d").build()));

    private static final ArrowColumnStoreFactory ARROW_STORE_FACTORY =
        PythonArrowDataUtils.getArrowColumnStoreFactory();

    private static final Path LAUNCHER = PythonScriptingSourceDirectory.getPath()//
        .resolve("_knime_scripting_launcher.py");

    private static final EntryPointCustomizer<PythonScriptingEntryPoint> REGISTER_VALUE_FACTORIES_CUSTOMIZER =
        PythonEntryPointUtils::registerPythonValueFactories;

    private final PythonGateway<PythonScriptingEntryPoint> m_gateway;

    private final PythonScriptingEntryPoint m_entryPoint;

    private final PythonArrowTableConverter m_tableConverter;

    private final FileStoreHandlerSupplier m_fileStoreHandlerSupplier;

    private final Consumer<ConsoleText> m_consoleTextConsumer;

    private final AutoCloseable m_outputRedirector;

    final AsynchronousCloseable<IOException> m_closer =
        AsynchronousCloseable.createAsynchronousCloser(this::closeInternal);

    private int m_numOutTables;

    private int m_numOutImages;

    private int m_numOutObjects;

    PythonScriptingSession(final PythonCommand pythonCommand, final Consumer<ConsoleText> consoleTextConsumer,
        final FileStoreHandlerSupplier fileStoreHandlerSupplier) throws IOException, InterruptedException {
        m_consoleTextConsumer = consoleTextConsumer;
        m_fileStoreHandlerSupplier = fileStoreHandlerSupplier;
        m_gateway = createGateway(pythonCommand);
        m_entryPoint = m_gateway.getEntryPoint();
        m_tableConverter = new PythonArrowTableConverter(EXECUTOR_SERVICE, ARROW_STORE_FACTORY,
            fileStoreHandlerSupplier.getWriteFileStoreHandler());
        m_outputRedirector = PythonGatewayUtils.redirectGatewayOutput(m_gateway, LOGGER::info, LOGGER::info);

        setCurrentWorkingDirectory();
    }

    void setupIO( //
        final PortObject[] inData, //
        final Collection<FlowVariable> flowVariables, //
        final int numOutTables, //
        final int numOutImages, //
        final int numOutObjects, //
        final boolean hasView, //
        final ExecutionMonitor exec //
    ) throws IOException, CanceledExecutionException {
        m_numOutTables = numOutTables;
        m_numOutImages = numOutImages;
        m_numOutObjects = numOutObjects;

        final var sources = PythonIOUtils.createSources(inData, m_tableConverter, exec);
        final var flowVars = FlowVariableUtils.convertToMap(flowVariables);
        final var callback = new PythonScriptingCallback();
        m_entryPoint.setupIO(sources, flowVars, numOutTables, numOutImages, numOutObjects, hasView, callback);
    }

    private final class PythonScriptingCallback implements PythonScriptingEntryPoint.Callback {

        @Override
        public PythonArrowDataSink create_sink() throws IOException {
            return m_tableConverter.createSink();
        }

        @Override
        public void add_stdout(final String text) {
            m_consoleTextConsumer.accept(new ConsoleText(text, false));
        }

        @Override
        public void add_stderr(final String text) {
            m_consoleTextConsumer.accept(new ConsoleText(text, true));
        }

        @Override
        public String resolve_knime_url(final String knimeUrl) throws IOException {
            try {
                var knimeUri = new URI(fixWindowsUrl(knimeUrl));
                return ResolverUtil.resolveURItoLocalOrTempFile(knimeUri).getAbsolutePath();
            } catch (URISyntaxException | InvalidSettingsException ex) {
                throw new IOException(ex);
            }
        }

        @Override
        public String get_workflow_temp_dir() {
            return FileUtil.getWorkflowTempDir().getAbsolutePath();
        }

        @Override
        public String get_workflow_dir() {
            return NodeContext.getContext().getWorkflowManager().getContextV2().getExecutorInfo().getLocalWorkflowPath()
                .toFile().getAbsolutePath();
        }

        @Override
        public String file_store_key_to_absolute_path(final String fileStoreKey) {
            var key = FileStoreKey.load(fileStoreKey);
            var fileStoreHandler = m_fileStoreHandlerSupplier.getFileStoreHandler(key);
            return PythonFileStoreUtils.getAbsolutePathForKey(fileStoreHandler, key);
        }

        @Override
        public String[] create_file_store() throws IOException {
            final var fileStoreHandler = m_fileStoreHandlerSupplier.getWriteFileStoreHandler();
            final var fileStore = PythonFileStoreUtils.createFileStore(fileStoreHandler);
            return new String[]{fileStore.getFile().getAbsolutePath(),
                FileStoreUtil.getFileStoreKey(fileStore).saveToString()};
        }

        @SuppressWarnings("unused")
        public List<Map<String, String>> get_global_proxy_list() { // NOSONAR
            return ProxyUtils.getGlobalProxyList();
        }
    }

    private static String fixWindowsUrl(String knimeUrl) throws InvalidSettingsException {
        try {
            CheckUtils.checkSourceFile(knimeUrl);
        } catch (final InvalidSettingsException ex) {
            if (SystemUtils.IS_OS_WINDOWS && knimeUrl != null) {
                knimeUrl = knimeUrl.replace("\\", "/");
                CheckUtils.checkSourceFile(knimeUrl);
            } else {
                throw ex;
            }
        }
        return knimeUrl;
    }

    /*
     * @return JSON according to executed lines. (All or any)
     *   can be also an error since we tunnel errors from python side as JSON
     */
    ExecutionInfo execute(final String script, final boolean checkOutputs) {
        String jsonFromExecution = m_entryPoint.execute(script, checkOutputs);
        return new Gson().fromJson(jsonFromExecution, ExecutionInfo.class);
    }

    enum ExecutionStatus {
            SUCCESS, EXECUTION_ERROR, KNIME_ERROR, FATAL_ERROR, CANCELLED
    }

    static class ExecutionInfo {
        private final ExecutionStatus status; // NOSONAR

        private final String description; // NOSONAR

        private final String[] traceback; // NOSONAR

        private final Object data; // NOSONAR

        // if set to true, the iframe containing the view will be re-loaded; defaults to false
        private boolean hasValidView; // NOSONAR

        @SuppressWarnings("hiding")
        ExecutionInfo(final ExecutionStatus status, final String description, final String[] traceback,
            final Object data) {
            this.status = status;
            this.description = description;
            this.traceback = traceback;
            this.data = data;
            this.hasValidView = false;
        }

        @SuppressWarnings("hiding")
        ExecutionInfo(final ExecutionStatus status, final String description) {
            this(status, description, null, null);
        }

        /**
         * @return the status
         */
        public ExecutionStatus getStatus() {
            return status;
        }

        /**
         * @return the traceback
         */
        public String[] getTraceback() {
            return traceback;
        }

        /**
         * @return the data
         */
        public Object getData() {
            return data;
        }

        /**
         * @return the description
         */
        public String getDescription() {
            return description;
        }

        /**
         * @return the hasValidView
         */
        public boolean getHasValidView() { // NOSONAR
            // "get" prefix is required by mapper to work correctly
            return hasValidView;
        }

        /**
         * @param hasValidView the hasValidView to set
         */
        public void setHasValidView(@SuppressWarnings("hiding") final boolean hasValidView) {
            this.hasValidView = hasValidView;
        }

    }

    Collection<FlowVariable> getFlowVariables() {
        return FlowVariableUtils.convertFromMap(m_entryPoint.getFlowVariables(), LOGGER);

    }

    PortObject[] getOutputs(final ExecutionContext exec) throws IOException, CanceledExecutionException {
        m_entryPoint.closeOutputs(true);

        // Progress handling
        final var totalProgress = 3 * m_numOutTables + m_numOutImages + m_numOutObjects;
        final var tableProgress = (3 * m_numOutTables) / (double)totalProgress;
        final var imageProgress = m_numOutImages / (double)totalProgress;
        final var objectProgress = m_numOutObjects / (double)totalProgress;

        // Retrieve the tables
        var execTables = exec.createSubExecutionContext(tableProgress);
        var tables = PythonIOUtils.getOutputTables(m_numOutTables, m_entryPoint, m_tableConverter, execTables);

        // Retrieve the images
        var execImages = exec.createSubProgress(imageProgress);
        var images = PythonIOUtils.getOutputImages(m_numOutImages, m_entryPoint, execImages);

        // Retrieve the objects
        var execObjects = exec.createSubExecutionContext(objectProgress);
        var objects = PythonIOUtils.getOutputObjects(m_numOutObjects, m_entryPoint, execObjects);

        // NB: The output ports always have the order tables, images, objects
        return Stream.of(tables, images, objects) //
            .flatMap(Stream::of) //
            .toArray(PortObject[]::new);
    }

    /**
     * Write the output view to a new temporary file and return the path to the file if an output view is available. The
     * caller must delete the file when it is not needed anymore.
     *
     * @throws IOException if the temporary file could not be created
     */
    Optional<Path> getOutputView() throws IOException {
        return PythonIOUtils.getOutputView(m_entryPoint);
    }

    private static PythonGateway<PythonScriptingEntryPoint> createGateway(final PythonCommand pythonCommand)
        throws IOException, InterruptedException {
        final var gatewayDescriptionBuilder =
            PythonGatewayDescription.builder(pythonCommand, LAUNCHER.toAbsolutePath(), PythonScriptingEntryPoint.class);

        gatewayDescriptionBuilder.withPreloaded(PythonArrowExtension.INSTANCE);
        gatewayDescriptionBuilder.withPreloaded(PythonViewsExtension.INSTANCE);
        gatewayDescriptionBuilder.withCustomizer(REGISTER_VALUE_FACTORIES_CUSTOMIZER);

        getPythonPaths().forEach(gatewayDescriptionBuilder::addToPythonPath);
        return Activator.GATEWAY_FACTORY.create(gatewayDescriptionBuilder.build());
    }

    private static List<Path> getPythonPaths() {
        var paths = new ArrayList<Path>();

        // NB: We do not add the PythonScriptingSourceDirectory
        // It is added automatically because the launcher is in this directory

        // Paths to the common API
        paths.add(Python3SourceDirectory.getPath());
        paths.add(Python3ArrowSourceDirectory.getPath());
        paths.add(Python3ViewsSourceDirectory.getPath());

        // Paths to the extension types
        PythonValueFactoryRegistry.getModules().stream() //
            .map(PythonValueFactoryModule::getParentDirectory) //
            .forEach(paths::add);

        return paths;
    }

    /**
     * @return a list of extra paths containing Python sources for KNIME plugins
     */
    public static List<Path> getExtraPythonPaths() {
        var paths = getPythonPaths();
        paths.add(PythonScriptingSourceDirectory.getPath());
        return paths;
    }

    @Override
    public void close() throws IOException {
        // Calls #closeInternal (but only if #asynchronousClose is not already running)
        m_closer.close();
    }

    @Override
    public Future<Void> asynchronousClose() throws IOException {
        // Calls #closeInternal asynchronously
        return m_closer.asynchronousClose();
    }

    private void closeInternal() throws IOException {
        // close gateway / kill process
        try {
            m_outputRedirector.close();
        } catch (final Exception e) {
            LOGGER.warn("Could not close the Python output redirector. "
                + "Log messages of the Python process might be missing in the console.", e);
        }
        m_tableConverter.close();
        m_gateway.close();
        m_fileStoreHandlerSupplier.close();
    }

    /** Set the current working directory to the workflow directory */
    private void setCurrentWorkingDirectory() {
        try {
            var workflowDirRef = NodeContext.getContext().getWorkflowManager().getNodeContainerDirectory();
            Optional.ofNullable(workflowDirRef).map(r -> r.getFile().toString())
                .ifPresent(m_entryPoint::setCurrentWorkingDirectory);
        } catch (Exception ex) { // NOSONAR: We want to catch any exception here
            // Do not propagate exception since setting the CWD is merely for convenience.
            LOGGER.warn("Python's current working directory could not be set to the workflow directory.", ex);
        }
    }

    /** Supplies the correct file store handler */
    public interface FileStoreHandlerSupplier extends AutoCloseable {

        /**
         * @return the write file store handler for new file stores in output tables
         */
        IWriteFileStoreHandler getWriteFileStoreHandler();

        /**
         * The file store handler that contains the given key
         *
         * @param key the key that must be present in this handler
         * @return the handler that should be used to resolve the file store
         */
        IFileStoreHandler getFileStoreHandler(FileStoreKey key);

        @Override
        void close();
    }
}
