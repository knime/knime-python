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
 */

package org.knime.python2.config;

import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.JTextComponent;

import org.knime.python2.PythonCommand;
import org.knime.python2.extensions.serializationlibrary.SentinelOption;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
import org.knime.python2.generic.SourceCodeConfig;
import org.knime.python2.kernel.PythonKernelBackendRegistry.PythonKernelBackendType;
import org.knime.python2.kernel.PythonKernelOptions;

/**
 * The options panel of a Python scripting node.
 *
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("serial") // Not intended for serialization.
public class PythonSourceCodeOptionsPanel extends JPanel {

    /** May be {@code null}, in which case the serializer to use is determined by {@link PythonKernelOptions}. */
    private final String m_serializerId;

    private final PythonKernelBackendSelectionPanel m_backendSelection = new PythonKernelBackendSelectionPanel();

    private final JSpinner m_rowLimit =
        new JSpinner(new SpinnerNumberModel(SourceCodeConfig.DEFAULT_ROW_LIMIT, 0, Integer.MAX_VALUE, 100));

    private final JTextComponent m_rowLimitNewBackendInfoText =
        PythonKernelBackendSelectionPanel.createBackendInfoText("");

    private final JCheckBox m_convertMissingToPython =
        new JCheckBox("Convert missing values to sentinel value (to Python)");

    private final JCheckBox m_convertMissingFromPython =
        new JCheckBox("Convert sentinel values to missing value (from Python)");

    private final JRadioButton m_useMinSentinelValueButton = new JRadioButton("MIN_VAL");

    private final JRadioButton m_useMaxSentinelValueButton = new JRadioButton("MAX_VAL");

    private final JRadioButton m_useCustomSentinelValueButton = new JRadioButton("");

    private int m_customSentinelValue = 0;

    private final JTextField m_customSentinelValueInput = new JTextField(Integer.toString(m_customSentinelValue));

    private final JLabel m_invalidCustomSentinelWarningLabel = new JLabel("");

    private final JTextComponent m_missingsNewBackendInfoText =
        PythonKernelBackendSelectionPanel.createBackendInfoText("");

    private final JSpinner m_chunkSize =
        new JSpinner(new SpinnerNumberModel(SerializationOptions.DEFAULT_CHUNK_SIZE, 1, Integer.MAX_VALUE, 1));

    private final JTextComponent m_chunkingNewBackendInfoText = PythonKernelBackendSelectionPanel
        .createBackendInfoText("Note: the new table API handles chunking automatically.");

    private final CopyOnWriteArrayList<IntConsumer> m_rowLimitListeners = new CopyOnWriteArrayList<>();

    // Not intended for serialization.
    private final CopyOnWriteArrayList<Consumer<SerializationOptions>> m_serializationOptionsChangeListeners =
        new CopyOnWriteArrayList<>(); // NOSONAR

    /**
     * Creates a new options panel without a fixed serializer configuration. The serializer to use is derived from the
     * Preferences.
     */
    public PythonSourceCodeOptionsPanel() {
        this(null, true);
    }

    /**
     * @param serializerId The id of the serializer to use in the node.
     * @param showBackendSelection Whether to allow users to select the Python kernel back end via this panel. (E.g.
     *            KNIME Deep Learning does not support the new back end yet.)
     */
    public PythonSourceCodeOptionsPanel(final String serializerId, final boolean showBackendSelection) {
        super(new GridBagLayout());
        m_serializerId = serializerId;

        final GridBagConstraints gbc = createDefaultGbc();

        if (showBackendSelection) {
            add(m_backendSelection, gbc);
            gbc.gridy++;
        }

        final JPanel rowLimitPanel = createRowLimitPanel();
        add(rowLimitPanel, gbc);
        gbc.gridy++;

        final JPanel missingsPanel = createMissingsPanel();
        add(missingsPanel, gbc);
        gbc.gridy++;

        final JPanel chunkingPanel = createChunkingPanel();
        add(chunkingPanel, gbc);
        gbc.gridy++;

        gbc.weighty = 1;
        add(new JLabel(), gbc);

        final List<JPanel> oldBackendPanels = List.of(rowLimitPanel, missingsPanel, chunkingPanel);
        final List<JComponent> newBackendInfos =
            List.of(m_rowLimitNewBackendInfoText, m_missingsNewBackendInfoText, m_chunkingNewBackendInfoText);
        m_backendSelection.addKernelBackendChangeListener(backendType -> { // NOSONAR
            final boolean isOldBackend = backendType == PythonKernelBackendType.PYTHON2;
            final boolean isNewBackend = !isOldBackend;
            for (final JPanel panel : oldBackendPanels) {
                setEnabledRecursively(panel, isOldBackend);
            }
            if (isNewBackend) {
                setNewBackendInfos();
            }
            for (final JComponent info : newBackendInfos) {
                info.setEnabled(isNewBackend);
                info.setVisible(isNewBackend);
            }
        });
    }

    private static GridBagConstraints createDefaultGbc() {
        final var gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;
        return gbc;
    }

    private JPanel createRowLimitPanel() {
        final var panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Row limit (dialog)"));
        final var gbc = createDefaultGbc();

        panel.add(new JLabel("Number of rows"), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_rowLimit, gbc);
        gbc.gridx--;
        gbc.gridy++;
        gbc.gridwidth = 2;
        m_rowLimitNewBackendInfoText.setVisible(false);
        panel.add(m_rowLimitNewBackendInfoText, gbc);

        m_rowLimit.addChangeListener(e -> onRowLimitChanged());

        return panel;
    }

    private JPanel createMissingsPanel() {
        final var changeListener = new OptionsChangeListener();
        final var panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Missing values (Integer, Long)"));
        final var gbc = createDefaultGbc();

        m_convertMissingToPython.addActionListener(changeListener);
        gbc.gridwidth = 5;
        panel.add(m_convertMissingToPython, gbc);
        gbc.gridy++;
        m_convertMissingFromPython.addActionListener(changeListener);
        panel.add(m_convertMissingFromPython, gbc);
        gbc.gridy++;
        gbc.gridwidth = 1;

        final var sentinelValueButtonGroup = new ButtonGroup();
        panel.add(new JLabel("Sentinel value"), gbc);
        gbc.gridx++;
        m_useMinSentinelValueButton.addActionListener(changeListener);
        sentinelValueButtonGroup.add(m_useMinSentinelValueButton);
        panel.add(m_useMinSentinelValueButton, gbc);
        gbc.gridx++;
        m_useMaxSentinelValueButton.addActionListener(changeListener);
        sentinelValueButtonGroup.add(m_useMaxSentinelValueButton);
        panel.add(m_useMaxSentinelValueButton, gbc);
        gbc.gridx++;
        m_useCustomSentinelValueButton.addActionListener(changeListener);
        sentinelValueButtonGroup.add(m_useCustomSentinelValueButton);
        panel.add(m_useCustomSentinelValueButton, gbc);
        gbc.gridx++;
        m_customSentinelValueInput
            .setPreferredSize(new Dimension(70, m_customSentinelValueInput.getPreferredSize().height));
        m_customSentinelValueInput.getDocument().addDocumentListener(new SentinelInputListener());
        panel.add(m_customSentinelValueInput, gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.gridwidth = 5;
        m_useMinSentinelValueButton.setSelected(true);
        m_invalidCustomSentinelWarningLabel.setVisible(false);
        panel.add(m_invalidCustomSentinelWarningLabel, gbc);
        gbc.gridy++;
        m_missingsNewBackendInfoText.setVisible(false);
        panel.add(m_missingsNewBackendInfoText, gbc);

        return panel;
    }

    private JPanel createChunkingPanel() {
        final var panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Chunking"));
        final GridBagConstraints gbc = createDefaultGbc();

        panel.add(new JLabel("Rows per chunk"), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_chunkSize, gbc);
        gbc.gridx--;
        gbc.gridy++;
        gbc.gridwidth = 2;
        m_chunkingNewBackendInfoText.setVisible(false);
        panel.add(m_chunkingNewBackendInfoText, gbc);

        return panel;
    }

    static void setEnabledRecursively(final Component component, final boolean enabled) {
        component.setEnabled(enabled);
        if (component instanceof Container) {
            for (final Component child : ((Container)component).getComponents()) {
                setEnabledRecursively(child, enabled);
            }
        }
    }

    private void setNewBackendInfos() {
        m_rowLimitNewBackendInfoText.setText("<table width=800><tr>" //
            + "You can use" //
            + "<blockquote>" //
            + "input_table_1 = input_table_1.to_pandas(rows=" + getRowLimit() + "), or<br>"
            + "input_table_1 = input_table_1.to_pyarrow(rows=" + getRowLimit() + ")" //
            + "</blockquote>" //
            + "in your script to set a row limit. Note that, unlike before, the limit does not only apply to the "
            + "dialog but also to the actual node execution." //
            + "</tr></table>");

        final var sentinel = getSentinelOption();
        final String sentinelText;
        if (sentinel == SentinelOption.MIN_VAL) {
            sentinelText = "\"min\"";
        } else if (sentinel == SentinelOption.MAX_VAL) {
            sentinelText = "\"max\"";
        } else {
            sentinelText = Integer.toString(m_customSentinelValue);
        }
        m_missingsNewBackendInfoText.setText("<table width=800><tr>" //
            + "You can use" //
            + "<blockquote>" //
            + "input_table_1 = input_table_1.to_pandas(sentinel=" + sentinelText + ")" //
            + "</blockquote>" //
            + "in your script to set a sentinel. When using to_pyarrow, this is not needed as pyarrow supports missing "
            + "values in integer and long columns." //
            + "</tr></table>");
    }

    @Override
    public final void add(final Component comp, final Object constraints) {
        super.add(comp, constraints);
    }

    /**
     * @return The type of the configured {@code link PythonKernelBackend}.
     */
    public PythonKernelBackendType getKernelBackendType() {
        return m_backendSelection.getKernelBackendType();
    }

    /**
     * @param listener The listener to add. The value passed to the listener is the type of the back end that has been
     *            selected.
     */
    public void addKernelBackendChangeListener(final Consumer<PythonKernelBackendType> listener) {
        m_backendSelection.addKernelBackendChangeListener(listener);
    }

    /**
     * @param listener The listener to remove.
     */
    public void removeKernelBackendChangeListener(final Consumer<PythonKernelBackendType> listener) {
        m_backendSelection.removeKernelBackendChangeListener(listener);
    }

    /**
     * @param listener The listener to add. The value passed to the listener is the new preferred command.
     */
    public void addCommandPreferenceChangeListener(final Consumer<PythonCommand> listener) {
        m_backendSelection.addCommandPreferenceChangeListener(listener);
    }

    /**
     * @param listener The listener to remove.
     */
    public void removeCommandPreferenceChangeListener(final Consumer<PythonCommand> listener) {
        m_backendSelection.removeCommandPreferenceChangeListener(listener);
    }

    /**
     * @return The configured row limit.
     */
    public int getRowLimit() {
        return (int)m_rowLimit.getValue();
    }

    /**
     * @param listener The listener to add. Accepts the newly configured row limit
     */
    public void addRowLimitChangeListener(final IntConsumer listener) {
        m_rowLimitListeners.add(listener); // NOSONAR Small collection, not performance critical.
    }

    /**
     * @param listener The listener to remove.
     */
    public void removeRowLimitChangeListener(final IntConsumer listener) {
        m_rowLimitListeners.remove(listener); // NOSONAR Small collection, not performance critical.
    }

    private void onRowLimitChanged() {
        final int rowLimit = getRowLimit();
        for (IntConsumer listener : m_rowLimitListeners) {
            listener.accept(rowLimit);
        }
    }

    /**
     * @return The configured serialization options.
     */
    public SerializationOptions getSerializationOptions() {
        final int chunkSize = getChunkSize();
        final boolean convertMissingToPython = m_convertMissingToPython.isSelected();
        final boolean convertMissingFromPython = m_convertMissingFromPython.isSelected();
        final SentinelOption sentinelOption = getSentinelOption();
        return new SerializationOptions(chunkSize, convertMissingToPython, convertMissingFromPython, sentinelOption,
            m_customSentinelValue).forSerializerId(m_serializerId);
    }

    /**
     * @param listener The listener to add. Accepts the newly configured serialization options.
     */
    public void addSerializationOptionsChangeListener(final Consumer<SerializationOptions> listener) {
        m_serializationOptionsChangeListeners.add(listener); // NOSONAR Small collection, not performance critical.
    }

    /**
     * @param listener The listener to remove.
     */
    public void removeSerializationOptionsChangeListener(final Consumer<SerializationOptions> listener) {
        m_serializationOptionsChangeListeners.remove(listener); // NOSONAR Small collection, not performance critical.
    }

    private void onSerializationOptionsChanged() {
        final SerializationOptions serializationOptions = getSerializationOptions();
        for (final Consumer<SerializationOptions> listener : m_serializationOptionsChangeListeners) {
            listener.accept(serializationOptions);
        }
    }

    /**
     * @param config The configuration from which to load the options of this panel.
     */
    public void loadSettingsFrom(final PythonSourceCodeConfig config) {
        m_backendSelection.loadSettingsFrom(config);
        m_rowLimit.setValue(config.getRowLimit());
        // TODO: check if each of these trigger an on-options-changed event; consolidate
        m_convertMissingToPython.setSelected(config.isConvertingMissingToPython());
        m_convertMissingFromPython.setSelected(config.isConvertingMissingFromPython());
        setSentinelOption(config.getSentinelOption());
        m_customSentinelValueInput.setText(config.getSentinelValue() + "");
        m_customSentinelValue = config.getSentinelValue();
        m_chunkSize.setValue(config.getChunkSize());

        onSerializationOptionsChanged();
    }

    private void setSentinelOption(final SentinelOption sentinelOption) {
        if (sentinelOption == SentinelOption.MIN_VAL) {
            m_useMinSentinelValueButton.setSelected(true);
        } else if (sentinelOption == SentinelOption.MAX_VAL) {
            m_useMaxSentinelValueButton.setSelected(true);
        } else {
            m_useCustomSentinelValueButton.setSelected(true);
        }
    }

    /**
     * @param config The configuration to which to save the options of this panel.
     */
    public void saveSettingsTo(final PythonSourceCodeConfig config) {
        m_backendSelection.saveSettingsTo(config);
        config.setRowLimit(getRowLimit());
        config.setConvertMissingToPython(m_convertMissingToPython.isSelected());
        config.setConvertMissingFromPython(m_convertMissingFromPython.isSelected());
        config.setSentinelOption(getSentinelOption());
        config.setSentinelValue(m_customSentinelValue);
        config.setChunkSize(getChunkSize());
    }

    private SentinelOption getSentinelOption() {
        final SentinelOption sentinelOption;
        if (m_useMinSentinelValueButton.isSelected()) {
            sentinelOption = SentinelOption.MIN_VAL;
        } else if (m_useMaxSentinelValueButton.isSelected()) {
            sentinelOption = SentinelOption.MAX_VAL;
        } else {
            sentinelOption = SentinelOption.CUSTOM;
        }
        return sentinelOption;
    }

    private int getChunkSize() {
        return ((Integer)m_chunkSize.getValue()).intValue();
    }

    private final class OptionsChangeListener implements ActionListener {

        @Override
        public void actionPerformed(final ActionEvent e) {
            onSerializationOptionsChanged();
        }
    }

    private final class SentinelInputListener implements DocumentListener {

        @Override
        public void removeUpdate(final DocumentEvent e) {
            try {
                m_customSentinelValue = Integer.parseInt(m_customSentinelValueInput.getText());
                m_invalidCustomSentinelWarningLabel.setVisible(false);
            } catch (final NumberFormatException ex) {
                m_customSentinelValue = 0;
                m_invalidCustomSentinelWarningLabel.setText("<html><font color=\"red\"><b>Sentinel value cannot be "
                    + "parsed. Default value " + m_customSentinelValue + " is used instead.</b></font></html>");
                m_invalidCustomSentinelWarningLabel.setVisible(true);
            }
            onSerializationOptionsChanged();
        }

        @Override
        public void insertUpdate(final DocumentEvent e) {
            removeUpdate(e);
        }

        @Override
        public void changedUpdate(final DocumentEvent e) {
            // Does not get fired.
        }
    }
}
