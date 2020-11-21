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
import java.util.function.Supplier;

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

import org.knime.python2.PythonCommand;
import org.knime.python2.PythonVersion;
import org.knime.python2.extensions.serializationlibrary.SentinelOption;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
import org.knime.python2.generic.SourceCodeOptionsPanel;
import org.knime.python2.kernel.PythonKernelOptions;

/**
 * The options panel for nodes concerned with Python scripting.
 *
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 */
public class PythonSourceCodeOptionsPanel
    extends SourceCodeOptionsPanel<PythonSourceCodePanel, PythonSourceCodeConfig> {

    private static final long serialVersionUID = -5612311503547573497L;

    private final EnforcePythonVersion m_enforcedVersion;

    private JPanel m_versionPanel;

    private JRadioButton m_python2;

    private JRadioButton m_python3;

    private JCheckBox m_convertToPython;

    private JCheckBox m_convertFromPython;

    private JRadioButton m_minVal;

    private JRadioButton m_maxVal;

    private JRadioButton m_sentinelOptionUserInput;

    private JTextField m_sentinelInput;

    private JLabel m_missingWarningLabel;

    private int m_sentinelValue;

    private JSpinner m_chunkSize;

    private PythonCommand m_python2Command;

    private PythonCommand m_python3Command;

    // Note that this is a supplier such that the default Python command can
    // change without calling the constructor again
    private final Supplier<PythonCommand> m_defaultPython2Command;

    private final Supplier<PythonCommand> m_defaultPython3Command;

    /**
     * Contains options for enforcing a specific Python version (or not).
     */
    public enum EnforcePythonVersion {
            /**
             * Enforce Python 2.
             */
            PYTHON2,
            /**
             * Enforce Python 3.
             */
            PYTHON3,
            /**
             * Enforce no Python version.
             */
            NONE;
    }

    /**
     * Create a source code options panel and enforce a certain Python version, that is, hide the Python version
     * selection.
     *
     * @param sourceCodePanel The corresponding source code panel.
     * @param version Enforce the given Python version or give the user the option to choose (if
     *            {@link EnforcePythonVersion#NONE} is passed).
     * @param defaultPython2Command a supplier that returns the Python 2 command which is used if no command has been configured
     * @param defaultPython3Command a supplier that returns the Python 3 command which is used if no command has been configured
     */
    public PythonSourceCodeOptionsPanel(final PythonSourceCodePanel sourceCodePanel, final EnforcePythonVersion version,
        final Supplier<PythonCommand> defaultPython2Command, final Supplier<PythonCommand> defaultPython3Command) {
        super(sourceCodePanel);
        m_enforcedVersion = version;
        m_defaultPython2Command = defaultPython2Command;
        m_defaultPython3Command = defaultPython3Command;
        if (m_enforcedVersion != EnforcePythonVersion.NONE) {
            m_versionPanel.setVisible(false);
        }
    }

    /**
     * Create a source code options panel and enforce a certain Python version, that is, hide the Python version
     * selection.
     *
     * @param sourceCodePanel The corresponding source code panel.
     * @param version Enforce the given Python version or give the user the option to choose (if
     *            {@link EnforcePythonVersion#NONE} is passed).
     */
    public PythonSourceCodeOptionsPanel(final PythonSourceCodePanel sourceCodePanel,
        final EnforcePythonVersion version) {
        this(sourceCodePanel, version, () -> null, () -> null);
    }

    /**
     * Create a source code options panel.
     *
     * @param sourceCodePanel the associated source code panel
     */
    public PythonSourceCodeOptionsPanel(final PythonSourceCodePanel sourceCodePanel) {
        this(sourceCodePanel, EnforcePythonVersion.NONE);
    }

    @Override
    protected JPanel getAdditionalOptionsPanel() {
        final ButtonGroup pythonVersion = new ButtonGroup();
        m_python2 = new JRadioButton("Python 2");
        m_python3 = new JRadioButton("Python 3");
        pythonVersion.add(m_python2);
        pythonVersion.add(m_python3);
        final PythonKernelOptionsListener pkol = new PythonKernelOptionsListener();
        m_python2.addActionListener(pkol);
        m_python3.addActionListener(pkol);
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.gridx = 0;
        gbc.gridy = 0;
        m_versionPanel = new JPanel(new FlowLayout());
        m_versionPanel.setBorder(BorderFactory.createTitledBorder("Use Python Version"));
        m_versionPanel.add(m_python2);
        m_versionPanel.add(m_python3);
        panel.add(m_versionPanel, gbc);
        // Missing value handling for int and long.
        final JPanel missingPanel = new JPanel(new GridLayout(0, 1));
        missingPanel.setBorder(BorderFactory.createTitledBorder("Missing Values (Int, Long)"));
        m_convertToPython = new JCheckBox("convert missing values to sentinel value (to python)");
        m_convertToPython.addActionListener(pkol);
        missingPanel.add(m_convertToPython);
        m_convertFromPython = new JCheckBox("convert sentinel values to missing value (from python)");
        m_convertFromPython.addActionListener(pkol);
        missingPanel.add(m_convertFromPython);
        final JPanel sentinelPanel = new JPanel(new FlowLayout());
        final JLabel sentinelLabel = new JLabel("Sentinel value: ");
        final ButtonGroup sentinelValueGroup = new ButtonGroup();
        m_minVal = new JRadioButton("MIN_VAL");
        m_minVal.addActionListener(pkol);
        m_maxVal = new JRadioButton("MAX_VAL");
        m_maxVal.addActionListener(pkol);
        m_sentinelOptionUserInput = new JRadioButton("");
        m_sentinelOptionUserInput.addActionListener(pkol);
        sentinelValueGroup.add(m_minVal);
        sentinelValueGroup.add(m_maxVal);
        sentinelValueGroup.add(m_sentinelOptionUserInput);
        m_minVal.setSelected(true);
        // TODO: Enable only if radio button is enabled.
        m_sentinelValue = 0;
        m_sentinelInput = new JTextField("0");
        m_sentinelInput.setPreferredSize(new Dimension(70, m_sentinelInput.getPreferredSize().height));
        m_sentinelInput.getDocument().addDocumentListener(new DocumentListener() {

            @Override
            public void removeUpdate(final DocumentEvent e) {
                updateSentinelValue();
                getSourceCodePanel().setKernelOptions(getSelectedOptions());
            }

            @Override
            public void insertUpdate(final DocumentEvent e) {
                removeUpdate(e);
            }

            @Override
            public void changedUpdate(final DocumentEvent e) {
                // Does not get fired.
            }
        });
        sentinelPanel.add(sentinelLabel);
        sentinelPanel.add(m_minVal);
        sentinelPanel.add(m_maxVal);
        sentinelPanel.add(m_sentinelOptionUserInput);
        sentinelPanel.add(m_sentinelInput);
        missingPanel.add(sentinelPanel);
        m_missingWarningLabel = new JLabel("");
        missingPanel.add(m_missingWarningLabel);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(missingPanel, gbc);
        // Give user control over the number of rows to transfer per chunk.
        final JPanel chunkingPanel = new JPanel(new FlowLayout());
        chunkingPanel.setBorder(BorderFactory.createTitledBorder("Chunking"));
        chunkingPanel.add(new JLabel("Rows per chunk: "));
        m_chunkSize =
            new JSpinner(new SpinnerNumberModel(SerializationOptions.DEFAULT_CHUNK_SIZE, 1, Integer.MAX_VALUE, 1));
        chunkingPanel.add(m_chunkSize);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(chunkingPanel, gbc);

        return panel;
    }

    /**
     * Read the sentinel value from its input component. Show warning if it cannot be parsed.
     */
    private void updateSentinelValue() {
        try {
            m_sentinelValue = Integer.parseInt(m_sentinelInput.getText());
            m_missingWarningLabel.setText("");
        } catch (final NumberFormatException ex) {
            m_sentinelValue = 0;
            m_missingWarningLabel.setText(
                "<html><font color=\"red\"><b>Sentinel value cannot be parsed. <br /> Default value 0 is used instead!</b></font></html>");
        }
    }

    /**
     * Update this panel with a new Python 2 command.
     *
     * @param python2Command The command.
     */
    public void updatePython2Command(final PythonCommand python2Command) {
        m_python2Command = python2Command;
        getSourceCodePanel().setPython2Command(python2Command);
    }

    /**
     * Update this panel with a new Python 3 command.
     *
     * @param python3Command The command.
     */
    public void updatePython3Command(final PythonCommand python3Command) {
        m_python3Command = python3Command;
        getSourceCodePanel().setPython3Command(python3Command);
    }

    @Override
    public void saveSettingsTo(final PythonSourceCodeConfig config) {
        super.saveSettingsTo(config);
        config.setPythonVersion(getSelectedPythonVersion());
        config.setConvertMissingToPython(m_convertToPython.isSelected());
        config.setConvertMissingFromPython(m_convertFromPython.isSelected());
        config.setSentinelOption(getSelectedSentinelOption());
        config.setSentinelValue(m_sentinelValue);
        config.setChunkSize(((Integer)m_chunkSize.getValue()).intValue());
        config.setPython2Command(m_python2Command);
        config.setPython3Command(m_python3Command);
    }

    @Override
    public void loadSettingsFrom(final PythonSourceCodeConfig config) {
        super.loadSettingsFrom(config);
        if (config.getPythonVersion() == PythonVersion.PYTHON3) {
            m_python3.setSelected(true);
        } else {
            m_python2.setSelected(true);
        }
        // Missing value handling.
        m_convertToPython.setSelected(config.getConvertMissingToPython());
        m_convertFromPython.setSelected(config.getConvertMissingFromPython());
        if (config.getSentinelOption() == SentinelOption.MIN_VAL) {
            m_minVal.setSelected(true);
        } else if (config.getSentinelOption() == SentinelOption.MAX_VAL) {
            m_maxVal.setSelected(true);
        } else {
            m_sentinelOptionUserInput.setSelected(true);
        }
        m_sentinelInput.setText(config.getSentinelValue() + "");
        m_sentinelValue = config.getSentinelValue();
        m_chunkSize.setValue(config.getChunkSize());
        m_python2Command = config.getPython2Command();
        m_python3Command = config.getPython3Command();
        getSourceCodePanel().setKernelOptions(getSelectedOptions());
    }

    private PythonKernelOptions getSelectedOptions() {
        final SerializationOptions serializationOptions =
            new SerializationOptions(((Integer)m_chunkSize.getValue()).intValue(), m_convertToPython.isSelected(),
                m_convertFromPython.isSelected(), getSelectedSentinelOption(), m_sentinelValue);
        final PythonCommand python2Command =
            m_python2Command == null ? m_defaultPython2Command.get() : m_python2Command;
        final PythonCommand python3Command =
            m_python3Command == null ? m_defaultPython3Command.get() : m_python3Command;
        return new PythonKernelOptions(getSelectedPythonVersion(), python2Command, python3Command,
            serializationOptions);
    }

    /**
     * @return The {@link PythonVersion} associated with the current enforced Python version or user selection.
     */
    private PythonVersion getSelectedPythonVersion() {
        if (m_enforcedVersion == EnforcePythonVersion.PYTHON2) {
            return PythonVersion.PYTHON2;
        } else if (m_enforcedVersion == EnforcePythonVersion.PYTHON3) {
            return PythonVersion.PYTHON3;
        } else {
            if (m_python2.isSelected()) {
                return PythonVersion.PYTHON2;
            } else {
                return PythonVersion.PYTHON3;
            }
        }
    }

    /**
     * @return The {@link SentinelOption} associated with the current user selection.
     */
    private SentinelOption getSelectedSentinelOption() {
        SentinelOption so = SentinelOption.MIN_VAL;
        if (m_minVal.isSelected()) {
            so = SentinelOption.MIN_VAL;
        } else if (m_maxVal.isSelected()) {
            so = SentinelOption.MAX_VAL;
        } else if (m_sentinelOptionUserInput.isSelected()) {
            so = SentinelOption.CUSTOM;
        }
        return so;
    }

    /**
     * Internal action listener used for most components. On change the {@link PythonKernelOptions} in the
     * {@link PythonSourceCodePanel} are updated.
     */
    private class PythonKernelOptionsListener implements ActionListener {

        @Override
        public void actionPerformed(final ActionEvent e) {
            getSourceCodePanel().setKernelOptions(getSelectedOptions());
        }
    }
}
