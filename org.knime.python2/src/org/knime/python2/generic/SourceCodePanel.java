/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python2.generic;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.LayoutManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.border.EmptyBorder;
import javax.swing.border.EtchedBorder;

import org.fife.ui.autocomplete.Completion;
import org.fife.ui.autocomplete.CompletionProvider;
import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.ViewUtils;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python2.config.PythonSourceCodeOptionsPanel;
import org.knime.python2.generic.ConsolePanel.Level;

/**
 * Abstract source code panel as basis for source code panels for a specific programming language.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("serial") // Not intended for serialization.
public abstract class SourceCodePanel extends JPanel {

    private final VariableNames m_variableNames;

    private final JSplitPane m_inputVarsFlowVarsSplit;

    private final InputVariablesTree m_inputVars;

    private final FlowVariablesList m_flowVars;

    private final JSplitPane m_inputVarsFlowVarsEditorSplit;

    private final EditorPanel m_editor;

    private final JButton m_exec = new JButton("Execute script");

    private final JButton m_execSelection = new JButton("Execute selected lines");

    private final JSplitPane m_workspaceVarsOutputVarsSplit;

    private final WorkspaceVariablesTable m_workspaceVars;

    private final OutputVariablesList m_outputVars;

    private final JPanel m_workspaceButtons;

    private final JButton m_reset = new JButton("Reset workspace");

    private final JButton m_showImages = new JButton("Show Image");

    private final JSplitPane m_editorWorkspaceVarsOutputVarsSplit;

    private final ConsolePanel m_console = new ConsolePanel();

    private final StatusBar m_statusBar = new StatusBar();

    private AtomicInteger m_rowLimit = new AtomicInteger(Integer.MAX_VALUE);

    private boolean m_interactive = false;

    private boolean m_running = false;

    private boolean m_stopped = true;

    /**
     * Protected editor buttons. Can be overwritten.
     */
    protected JPanel m_editorButtons;

    /**
     * Create a source code panel for the given language style.
     *
     * @param syntaxStyle One of the language styles defined in {@link SyntaxConstants}.
     * @param variableNames An object managing all the known variable names in the workspace.
     */
    public SourceCodePanel(final String syntaxStyle, final VariableNames variableNames) {
        this(syntaxStyle, variableNames, null);
    }

    /**
     * Create a source code panel for the given language style.
     *
     * @param syntaxStyle One of the language styles defined in {@link SyntaxConstants}.
     * @param variableNames An object managing all the known variable names in the workspace.
     * @param optionsPanel The options panel of the node dialog, if any. The constructed source code panel subscribes to
     *            changes in its configured {@link PythonSourceCodeOptionsPanel#getRowLimit() row limit}.
     */
    public SourceCodePanel(final String syntaxStyle, final VariableNames variableNames,
        final PythonSourceCodeOptionsPanel optionsPanel) {
        m_variableNames = variableNames;
        setLayout(new BorderLayout());

        // Status bar:
        m_statusBar.setStatusMessage(" ");
        add(m_statusBar, BorderLayout.SOUTH);

        final Font font = m_console.getFont();
        final Font newFont = new Font("monospaced", font.getStyle(), font.getSize());
        m_console.setFont(newFont);

        // Editor:

        m_editor = new EditorPanel(syntaxStyle) {

            @Override
            protected List<Completion> getCompletionsFor(final CompletionProvider provider, final String sourceCode,
                final int line, final int column) {
                return SourceCodePanel.this.getCompletionsFor(provider, sourceCode, line, column);
            }
        };
        final JPanel editorPanel = m_editor.getPanel();
        m_editorButtons = new JPanel(new FlowLayout(FlowLayout.TRAILING));
        m_exec.addActionListener(e -> runExec(m_editor.getEditor().getText()));
        m_editorButtons.add(m_exec);
        m_execSelection.addActionListener(e -> {
            final String selectedText = m_editor.getSelectedLines();
            if ((selectedText != null) && !selectedText.isEmpty()) {
                runExec(selectedText);
            } else {
                setStatusMessage("Nothing selected");
            }
        });
        m_editorButtons.add(m_execSelection);
        editorPanel.add(m_editorButtons, BorderLayout.SOUTH);

        // Left-hand side of the panel:

        m_inputVarsFlowVarsSplit = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        m_inputVarsFlowVarsSplit.setResizeWeight(0.6);
        m_inputVarsFlowVarsSplit.setOneTouchExpandable(true);
        m_inputVarsFlowVarsSplit.setDividerSize(8);
        m_inputVarsFlowVarsSplit.setPreferredSize(new Dimension(0, 0));

        m_inputVars = new InputVariablesTree(variableNames, m_editor.getEditor(), this::createVariableAccessString);
        final JPanel inputVariablesPanel = m_inputVars.getPanel();
        inputVariablesPanel.setPreferredSize(new Dimension(0, 0));
        m_inputVarsFlowVarsSplit.setTopComponent(inputVariablesPanel);

        m_flowVars = new FlowVariablesList(variableNames.getFlowVariables(), m_editor.getEditor(),
            this::createVariableAccessString);
        final JPanel flowVariablesPanel = m_flowVars.getPanel();
        flowVariablesPanel.setPreferredSize(new Dimension(0, 0));
        m_inputVarsFlowVarsSplit.setBottomComponent(flowVariablesPanel);

        m_inputVarsFlowVarsEditorSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        m_inputVarsFlowVarsEditorSplit.setResizeWeight(0.2);
        m_inputVarsFlowVarsEditorSplit.setOneTouchExpandable(true);
        m_inputVarsFlowVarsEditorSplit.setDividerSize(8);
        m_inputVarsFlowVarsEditorSplit.setPreferredSize(new Dimension(0, 0));

        m_inputVarsFlowVarsEditorSplit.setLeftComponent(m_inputVarsFlowVarsSplit);

        editorPanel.setPreferredSize(new Dimension(0, 0));
        m_inputVarsFlowVarsEditorSplit.setRightComponent(editorPanel);

        setInputVariablesViewVisible(m_inputVars.hasEntries());

        // Right-hand side of the panel:

        m_workspaceVarsOutputVarsSplit = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        m_workspaceVarsOutputVarsSplit.setResizeWeight(0.6);
        m_workspaceVarsOutputVarsSplit.setOneTouchExpandable(true);
        m_workspaceVarsOutputVarsSplit.setDividerSize(8);

        m_workspaceVars = new WorkspaceVariablesTable(newFont, m_console);
        final JPanel workspacePanel = m_workspaceVars.getPanel();
        workspacePanel.setPreferredSize(new Dimension(0, 0));
        m_workspaceVarsOutputVarsSplit.setTopComponent(workspacePanel);

        m_reset.addActionListener(e -> runReset());
        m_workspaceButtons = new JPanel(new FlowLayout(FlowLayout.TRAILING));
        if (variableNames.getOutputImages().length > 1) {
            m_showImages.setText("Show Images");
        }
        m_workspaceButtons.add(m_reset);
        if (variableNames.getOutputImages().length > 0) {
            m_showImages.addActionListener(e -> showImages());
            m_workspaceButtons.add(m_showImages);
        }

        m_outputVars = new OutputVariablesList(variableNames, m_editor.getEditor());
        final JPanel outputVarsPanel = m_outputVars.getPanel();
        outputVarsPanel.setPreferredSize(new Dimension(0, 0));
        m_workspaceVarsOutputVarsSplit.setBottomComponent(outputVarsPanel);

        m_editorWorkspaceVarsOutputVarsSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        m_editorWorkspaceVarsOutputVarsSplit.setResizeWeight(0.8);
        m_editorWorkspaceVarsOutputVarsSplit.setOneTouchExpandable(true);
        m_editorWorkspaceVarsOutputVarsSplit.setDividerSize(8);

        m_editorWorkspaceVarsOutputVarsSplit.setLeftComponent(m_inputVarsFlowVarsEditorSplit);

        m_workspaceVarsOutputVarsSplit.setPreferredSize(new Dimension(0, 0));
        m_editorWorkspaceVarsOutputVarsSplit.setRightComponent(m_workspaceVarsOutputVarsSplit);

        setOutputVariablesViewVisible(m_outputVars.hasEntries());

        // Console:

        final JSplitPane editorConsoleSplit = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        editorConsoleSplit.setResizeWeight(0.8);
        editorConsoleSplit.setOneTouchExpandable(true);
        editorConsoleSplit.setDividerSize(8);

        editorConsoleSplit.setTopComponent(m_editorWorkspaceVarsOutputVarsSplit);

        final JPanel consolePanel = m_console.getPanel();
        consolePanel.setPreferredSize(new Dimension(0, 0));
        editorConsoleSplit.setBottomComponent(consolePanel);

        add(editorConsoleSplit, BorderLayout.CENTER);

        setPreferredSize(new Dimension(1200, 850));

        if (optionsPanel != null) {
            m_rowLimit.set(optionsPanel.getRowLimit());
            optionsPanel.addRowLimitChangeListener(this::setRowLimit);
        }
    }

    @Override
    public final void setLayout(final LayoutManager mgr) {
        super.setLayout(mgr);
    }

    @Override
    public final void add(final Component comp, final Object constraints) {
        super.add(comp, constraints);
    }

    @Override
    public final void setPreferredSize(final Dimension preferredSize) {
        super.setPreferredSize(preferredSize);
    }

    /**
     * @param visible If {@code true}, the "Input variables" view will be shown in the panel, otherwise it will be
     *            hidden.
     */
    private void setInputVariablesViewVisible(final boolean visible) {
        m_inputVarsFlowVarsEditorSplit.setLeftComponent(visible ? m_inputVarsFlowVarsSplit : m_flowVars.getPanel());
    }

    /**
     * @param visible If {@code true}, the "Output variables" view will be shown in the panel, otherwise it will be
     *            hidden.
     */
    private void setOutputVariablesViewVisible(final boolean visible) {
        final JPanel rightComponentWithButtons = new JPanel(new BorderLayout());
        rightComponentWithButtons.setBorder(new EmptyBorder(0, 0, 0, 0));
        rightComponentWithButtons.add(visible ? m_workspaceVarsOutputVarsSplit : m_workspaceVars.getPanel(),
            BorderLayout.CENTER);
        rightComponentWithButtons.add(m_workspaceButtons, BorderLayout.SOUTH);
        rightComponentWithButtons.setPreferredSize(new Dimension(0, 0));
        m_editorWorkspaceVarsOutputVarsSplit.setRightComponent(rightComponentWithButtons);
    }

    /**
     * Subclasses should invoke this if they override {@link #loadSettingsFrom(SourceCodeConfig, PortObjectSpec[])} and
     * do not call super.
     */
    protected final void installAutoCompletion() {
        m_editor.installAutoCompletion();
    }

    /**
     * Sets the image that will be shown in the show image window.
     *
     * @param label The label to put the image into
     * @param imageName Name of the image to load
     */
    private void setImage(final JLabel label, final String imageName) {
        final ImageContainer image = getOutImage(imageName);
        if (image != null) {
            label.setText(null);
            label.setIcon(new ImageIcon(image.getBufferedImage()));
            label.setBorder(null);
        } else {
            label.setIcon(null);
            label.setText("Could not load image: " + imageName);
            label.setBorder(new EmptyBorder(10, 10, 10, 10));
        }
    }

    private void showImages() {
        final JFrame window = new JFrame();
        window.setTitle("Python image");
        final Container contentPane = window.getContentPane();
        contentPane.setLayout(new BorderLayout());
        final JLabel imageLabel = new JLabel();
        imageLabel.setForeground(Color.RED);
        setImage(imageLabel, m_variableNames.getOutputImages()[0]);
        contentPane.add(new JScrollPane(imageLabel), BorderLayout.CENTER);
        final JPanel buttons = new JPanel(new FlowLayout(FlowLayout.TRAILING));
        final JButton closeButton = new JButton("Close");
        buttons.add(closeButton);
        contentPane.add(buttons, BorderLayout.SOUTH);
        if (m_variableNames.getOutputImages().length > 1) {
            window.setTitle("Python images");
            final JComboBox<String> imageSelection = new JComboBox<>(m_variableNames.getOutputImages());
            contentPane.add(imageSelection, BorderLayout.NORTH);
            imageSelection.addActionListener(ev -> {
                setImage(imageLabel, (String)imageSelection.getSelectedItem());
                window.pack();
            });
        }
        closeButton.addActionListener(ev -> window.setVisible(false));
        window.pack();
        window.setLocationRelativeTo(SourceCodePanel.this);
        window.setVisible(true);
    }

    /**
     * Opens and initializes the panel.
     */
    public void open() {
        ViewUtils.invokeAndWaitInEDT(() -> {
            clearConsole();
            m_workspaceVars.clear();
            m_statusBar.getProgressBar().setValue(0);
        });
    }

    /**
     * Closes and cleans up the panel.
     */
    public void close() {
        // Nothing to do.
    }

    /**
     * Save settings to the given {@link SourceCodeConfig}.
     *
     * @param config The config to save to
     * @throws InvalidSettingsException If the current settings are invalid
     */
    public void saveSettingsTo(final SourceCodeConfig config) throws InvalidSettingsException {
        config.setSourceCode(m_editor.getEditor().getText());
    }

    /**
     * Loads settings from the given {@link SourceCodeConfig}. If subclasses override this method and do not invoke
     * super, then they <b>must</b> invoke {@link #installAutoCompletion()} otherwise problems similar to AP-10515 will
     * occur.
     *
     * @param config The config to load from.
     * @param specs The input specs of the node, entries corresponding to input {@link DataTableSpec tables} may not be
     *            {@code null}.
     * @throws NotConfigurableException If the panel is not configurable.
     */
    public void loadSettingsFrom(final SourceCodeConfig config, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        installAutoCompletion();

        m_editor.getEditor().setText(config.getSourceCode());
        final List<DataTableSpec> tableSpecs = new ArrayList<>();
        for (final PortObjectSpec spec : specs) {
            if (spec instanceof DataTableSpec) {
                tableSpecs.add((DataTableSpec)spec);
            }
        }
        updateSpec(tableSpecs.toArray(new DataTableSpec[tableSpecs.size()]));
    }

    /**
     * Updates the spec of the input tables.
     *
     * @param specs The input table specs
     */
    public void updateSpec(final DataTableSpec[] specs) {
        m_inputVars.updateInputs(specs);
    }

    /**
     * Updates the available flow variables.
     *
     * @param flowVariables The flow variables
     */
    public void updateFlowVariables(final FlowVariable[] flowVariables) {
        m_flowVars.updateFlowVariables(flowVariables);
    }

    /**
     * Gets the flow variables in the current workspace.
     *
     * @return The flow variables
     */
    protected List<FlowVariable> getFlowVariables() {
        return m_flowVars.getFlowVariables();
    }

    /**
     * Sets this panel to be interactive (can execute code, inspect variables, ...) or not.
     *
     * @param interactive true if the panel is interactive
     */
    protected void setInteractive(final boolean interactive) {
        runInUiThread(() -> {
            m_interactive = interactive;
            // These buttons can only be clicked if the panel is interactive and no execution is currently running.
            m_execSelection.setEnabled(interactive && !m_running);
            m_exec.setEnabled(interactive && !m_running);
            m_reset.setEnabled((m_stopped || interactive) && !m_running);
            m_showImages.setEnabled(interactive && !m_running);
        });
    }

    /**
     * Sets this panel to display that it is running code.
     *
     * @param running true if code is currently executing, false otherwise
     */
    protected void setRunning(final boolean running) {
        runInUiThread(() -> {
            final boolean wasRunning = m_running;
            m_running = running;
            if (running) {
                m_stopped = false;
            }
            setInteractive(m_interactive);
            m_statusBar.setRunning(running);
            // Update variables if we just finished running
            if (wasRunning && !running) {
                updateVariables();
            }
        });
    }

    /**
     * Set this panel to enable only the "Reset workspace" button.
     */
    protected void setStopped() {
        runInUiThread(() -> {
            m_stopped = true;
            m_statusBar.setRunning(false);
            setInteractive(false);
            setVariables(new Variable[0]);
        });
    }

    /**
     * Appends the given message to the console.
     *
     * @param text The message to append.
     */
    public void messageToConsole(final String text) {
        printToConsole(text, Level.INFO);
    }

    /**
     * Appends the given warning to the console.
     *
     * @param text The warning to append.
     */
    public void warningToConsole(final String text) {
        printToConsole(text, Level.WARNING);
    }

    /**
     * Appends the given error to the console
     *
     * @param text The error to append.
     */
    public void errorToConsole(final String text) {
        printToConsole(text, Level.ERROR);
    }

    private void printToConsole(final String text, final Level level) {
        if (text != null && !text.trim().isEmpty()) {
            runInUiThread(() -> m_console.print(text, level));
        }
    }

    /**
     * Clears the console.
     */
    protected void clearConsole() {
        runInUiThread(() -> m_console.clear());
    }

    /**
     * Sets the given status message as new status in the status bar.
     *
     * @param statusMessage The new status
     */
    protected void setStatusMessage(final String statusMessage) {
        runInUiThread(() -> m_statusBar.setStatusMessage(statusMessage));
    }

    /**
     * Sets the runnable that will be called when the stop button in the status bar is clicked.
     *
     * @param runnable The runnable to call when stop is clicked
     */
    protected void setStopCallback(final Runnable runnable) {
        ViewUtils.invokeAndWaitInEDT(() -> m_statusBar.setStopCallback(runnable));
    }

    /**
     * Returns the progress bar of the status bar.
     *
     * @return The progress bar
     */
    protected JProgressBar getProgressBar() {
        return m_statusBar.getProgressBar();
    }

    /**
     * Executes the given source code.
     *
     * This method should add the output to the console.
     *
     * @param sourceCode The source code to execute
     */
    protected abstract void runExec(final String sourceCode);

    /**
     * Initiates an update of the listed variables.
     *
     * This method should call {@link #setVariables(Variable[])} once the variables have been retrieved.
     */
    protected abstract void updateVariables();

    /**
     * Sets the variables displayed in the variable table
     *
     * @param variables The variables to display
     */
    protected void setVariables(final Variable[] variables) {
        m_workspaceVars.setVariables(variables);
    }

    /**
     * Resets the current workspace.
     */
    protected abstract void runReset();

    /**
     * Retrieves possible completions for the given source code at the given cursor position
     *
     * @param provider {@link CompletionProvider} for creation of the {@link Completion} objects
     * @param sourceCode The source code to complete
     * @param line Cursor position (line)
     * @param column Cursor position (column)
     * @return List of possible completions
     */
    protected abstract List<Completion> getCompletionsFor(final CompletionProvider provider, final String sourceCode,
        final int line, final int column);

    /**
     * Runs the given runnable in the UI thread.
     *
     * @param runnable The runnable to run in the UI thread
     */
    private static void runInUiThread(final Runnable runnable) {
        ViewUtils.runOrInvokeLaterInEDT(runnable);
    }

    /**
     * Gets the output image with the given name
     *
     * @param name The variable name in the workspace
     * @return The image
     */
    protected ImageContainer getOutImage(final String name) {
        return null;
    }

    /**
     * Sets the row limit used to put tables into the workspace.
     *
     * @param rowLimit The maximum number of rows per table
     */
    protected void setRowLimit(final int rowLimit) {
        m_rowLimit.set(rowLimit);
    }

    /**
     * Gets the row limit used to put tables into the workspace.
     *
     * @return The maximum number of rows per table
     */
    protected int getRowLimit() {
        return m_rowLimit.get();
    }

    /**
     * Creates the string used to access a variable in the source code.
     *
     * @param variable The variable name
     * @param field Name of the field inside the variable
     * @return Variable excess string
     */
    protected abstract String createVariableAccessString(final String variable, final String field);

    /**
     * Get the name for the flow variables variable.
     *
     * @return The name
     */
    protected VariableNames getVariableNames() {
        return m_variableNames;
    }

    /**
     * @return the editor widget component
     */
    public RSyntaxTextArea getEditor() {
        return m_editor.getEditor();
    }

    /**
     * Create a preconfigured editor widget component.
     *
     * @param syntaxStyle One of the language styles defined in {@link SyntaxConstants}
     * @return a preconfigured editor widget component
     */
    public static RSyntaxTextArea createEditor(final String syntaxStyle) {
        final RSyntaxTextArea editor = new PythonTextArea();
        editor.setSyntaxEditingStyle(syntaxStyle);
        editor.setCodeFoldingEnabled(true);
        editor.setAntiAliasingEnabled(true);
        editor.setAutoIndentEnabled(true);
        editor.setFadeCurrentLineHighlight(true);
        editor.setHighlightCurrentLine(true);
        editor.setLineWrap(false);
        editor.setRoundedSelectionEdges(true);
        editor.setBorder(new EtchedBorder());
        editor.setTabSize(4);
        return editor;
    }

    /**
     * Sets the workspacePanel to the visibility given value
     *
     * @param show whether to show the interactive components
     */
    public void showWorkspacePanel(final boolean show) {
        m_workspaceVars.getPanel().setVisible(show);
        m_workspaceButtons.setVisible(show);
    }

    /**
     * Represents a variable as it is displayed in the variables table.
     *
     * @author Patrick Winter, KNIME AG, Zurich, Switzerland
     */
    public static class Variable {

        private final String m_name;

        private final String m_type;

        private final String m_value;

        /**
         * Creates a variable.
         *
         * @param name The name of the variable.
         * @param type The type of the variable.
         * @param value The value of the variable.
         */
        public Variable(final String name, final String type, final String value) {
            m_name = name;
            m_type = type;
            m_value = value;
        }

        /**
         * @return The name of the variable.
         */
        public String getName() {
            return m_name;
        }

        /**
         * @return The type of the variable.
         */
        public String getType() {
            return m_type;
        }

        /**
         * @return The value of the variable.
         */
        public String getValue() {
            return m_value;
        }
    }
}
