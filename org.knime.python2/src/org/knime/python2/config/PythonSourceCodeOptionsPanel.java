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

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import org.knime.python2.extensions.serializationlibrary.SentinelOption;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
import org.knime.python2.generic.SourceCodeOptionsPanel;
import org.knime.python2.kernel.PythonKernelOptions;

/**
 * The options panel of a Python scripting node.
 *
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("serial") // Not intended for serialization.
public class PythonSourceCodeOptionsPanel extends SourceCodeOptionsPanel<PythonSourceCodeConfig> {

    /** May be {@code null}, in which case the serializer to use is determined by {@link PythonKernelOptions}. */
    private final String m_serializerId;

    private JCheckBox m_convertMissingToPython;

    private JCheckBox m_convertMissingFromPython;

    private JRadioButton m_useMinSentinelValueButton;

    private JRadioButton m_useMaxSentinelValueButton;

    private JRadioButton m_useCustomSentinelValueButton;

    private JTextField m_customSentinelValueInput;

    private JLabel m_invalidCustomSentinelWarningLabel;

    private int m_customSentinelValue;

    private JSpinner m_chunkSize;

    // Not intended for serialization.
    private final CopyOnWriteArrayList<Consumer<SerializationOptions>> m_listeners = new CopyOnWriteArrayList<>(); // NOSONAR

    public PythonSourceCodeOptionsPanel() {
        this(null);
    }

    public PythonSourceCodeOptionsPanel(final String serializerId) {
        m_serializerId = serializerId;
    }

    @Override
    protected JPanel getAdditionalOptionsPanel() {
        final OptionsChangeListener changeListener = new OptionsChangeListener();

        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.gridx = 0;
        gbc.gridy = 0;

        final JPanel missingPanel = new JPanel(new GridLayout(0, 1));
        missingPanel.setBorder(BorderFactory.createTitledBorder("Missing values (Integer, Long)"));
        m_convertMissingToPython = new JCheckBox("Convert missing values to sentinel value (to Python)");
        m_convertMissingToPython.addActionListener(changeListener);
        missingPanel.add(m_convertMissingToPython);
        m_convertMissingFromPython = new JCheckBox("Convert sentinel values to missing value (from Python)");
        m_convertMissingFromPython.addActionListener(changeListener);
        missingPanel.add(m_convertMissingFromPython);

        final JPanel sentinelPanel = new JPanel(new FlowLayout());
        final ButtonGroup sentinelValueButtonGroup = new ButtonGroup();

        sentinelPanel.add(new JLabel("Sentinel value"));
        m_useMinSentinelValueButton = new JRadioButton("MIN_VAL");
        m_useMinSentinelValueButton.addActionListener(changeListener);
        sentinelValueButtonGroup.add(m_useMinSentinelValueButton);
        sentinelPanel.add(m_useMinSentinelValueButton);
        m_useMaxSentinelValueButton = new JRadioButton("MAX_VAL");
        m_useMaxSentinelValueButton.addActionListener(changeListener);
        sentinelValueButtonGroup.add(m_useMaxSentinelValueButton);
        sentinelPanel.add(m_useMaxSentinelValueButton);
        m_useCustomSentinelValueButton = new JRadioButton("");
        m_useCustomSentinelValueButton.addActionListener(changeListener);
        sentinelValueButtonGroup.add(m_useCustomSentinelValueButton);
        sentinelPanel.add(m_useCustomSentinelValueButton);
        m_customSentinelValue = 0;
        // TODO: Enable only if radio button is enabled.
        m_customSentinelValueInput = new JTextField(Integer.toString(m_customSentinelValue));
        m_customSentinelValueInput
            .setPreferredSize(new Dimension(70, m_customSentinelValueInput.getPreferredSize().height));
        m_customSentinelValueInput.getDocument().addDocumentListener(new SentinelInputListener());
        sentinelPanel.add(m_customSentinelValueInput);
        m_useMinSentinelValueButton.setSelected(true);

        missingPanel.add(sentinelPanel);

        m_invalidCustomSentinelWarningLabel = new JLabel("");
        missingPanel.add(m_invalidCustomSentinelWarningLabel);

        panel.add(missingPanel, gbc);

        final JPanel chunkingPanel = new JPanel(new FlowLayout());
        chunkingPanel.setBorder(BorderFactory.createTitledBorder("Chunking"));
        chunkingPanel.add(new JLabel("Rows per chunk"));
        m_chunkSize =
            new JSpinner(new SpinnerNumberModel(SerializationOptions.DEFAULT_CHUNK_SIZE, 1, Integer.MAX_VALUE, 1));
        chunkingPanel.add(m_chunkSize);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(chunkingPanel, gbc);

        return panel;
    }

    public SerializationOptions getSerializationOptions() {
        final int chunkSize = getChunkSize();
        final boolean convertMissingToPython = m_convertMissingToPython.isSelected();
        final boolean convertMissingFromPython = m_convertMissingFromPython.isSelected();
        final SentinelOption sentinelOption = getSentinelOption();
        return new SerializationOptions(chunkSize, convertMissingToPython, convertMissingFromPython, sentinelOption,
            m_customSentinelValue).forSerializerId(m_serializerId);
    }

    public void addSerializationOptionsChangeListener(final Consumer<SerializationOptions> listener) {
        m_listeners.add(listener); // NOSONAR Small collection, not performance critical.
    }

    public void removeSerializationOptionsChangeListener(final Consumer<SerializationOptions> listener) {
        m_listeners.remove(listener); // NOSONAR Small collection, not performance critical.
    }

    private void onSerializationOptionsChanged() {
        final SerializationOptions serializationOptions = getSerializationOptions();
        for (final Consumer<SerializationOptions> listener : m_listeners) {
            listener.accept(serializationOptions);
        }
    }

    @Override
    public void loadSettingsFrom(final PythonSourceCodeConfig config) {
        super.loadSettingsFrom(config);

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

    @Override
    public void saveSettingsTo(final PythonSourceCodeConfig config) {
        super.saveSettingsTo(config);

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
                m_invalidCustomSentinelWarningLabel.setText("");
            } catch (final NumberFormatException ex) {
                m_customSentinelValue = 0;
                m_invalidCustomSentinelWarningLabel.setText("<html><font color=\"red\"><b>Sentinel value cannot be "
                    + "parsed.<br/>Default value " + m_customSentinelValue + " is used instead.</b></font></html>");
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
