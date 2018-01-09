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
import org.knime.python2.generic.SourceCodeOptionsPanel;
import org.knime.python2.kernel.PythonKernelOptions;
import org.knime.python2.kernel.PythonKernelOptions.PythonVersionOption;

/**
 * The options panel for every node concerned with python scripting.
 *
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 *
 */

public class PythonSourceCodeOptionsPanel
extends SourceCodeOptionsPanel<PythonSourceCodePanel, PythonSourceCodeConfig> {

    private static final long serialVersionUID = -5612311503547573497L;

    private ButtonGroup m_pythonVersion;

    private JRadioButton m_python2;

    private JRadioButton m_python3;

    private JCheckBox m_convertToPython;

    private JCheckBox m_convertFromPython;

    private ButtonGroup m_sentinelValueGroup;

    private JRadioButton m_minVal;

    private JRadioButton m_maxVal;

    private JRadioButton m_useInput;

    private JTextField m_sentinelInput;

    private JLabel m_missingWarningLabel;

    private int m_sentinelValue;

    private JSpinner m_chunkSize;

    private JPanel m_versionPanel;

    private final EnforcePythonVersion m_enforcedVersion;

    /**
     * Enum containing options for enforcing a python version.
     */
    public enum EnforcePythonVersion {
        PYTHON2, PYTHON3, NONE;
    }

    /**
     * Create a source code options panel.
     *
     * @param sourceCodePanel The corresponding source code panel
     * @param version Whether to enforce a certain python version or give the user the option to choose
     */
    public PythonSourceCodeOptionsPanel(final PythonSourceCodePanel sourceCodePanel, final EnforcePythonVersion version) {
        super(sourceCodePanel);
        m_enforcedVersion = version;
        if(m_enforcedVersion != EnforcePythonVersion.NONE) {
            m_versionPanel.setVisible(false);
        }
    }

    /**
     * Constructor.
     *
     * @param sourceCodePanel the associated source code panel
     */
    public PythonSourceCodeOptionsPanel(final PythonSourceCodePanel sourceCodePanel) {
        this(sourceCodePanel, EnforcePythonVersion.NONE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JPanel getAdditionalOptionsPanel() {
        m_pythonVersion = new ButtonGroup();
        m_python2 = new JRadioButton("Python 2");
        m_python3 = new JRadioButton("Python 3");
        m_pythonVersion.add(m_python2);
        m_pythonVersion.add(m_python3);
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
        //Missing value handling for Int and Long
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
        m_sentinelValueGroup = new ButtonGroup();
        m_minVal = new JRadioButton("MIN_VAL");
        m_minVal.addActionListener(pkol);
        m_maxVal = new JRadioButton("MAX_VAL");
        m_maxVal.addActionListener(pkol);
        m_useInput = new JRadioButton("");
        m_useInput.addActionListener(pkol);
        m_sentinelValueGroup.add(m_minVal);
        m_sentinelValueGroup.add(m_maxVal);
        m_sentinelValueGroup.add(m_useInput);
        m_minVal.setSelected(true);
        //TODO enable only if radio button is enabled
        m_sentinelValue = 0;
        m_sentinelInput = new JTextField("0");
        m_sentinelInput.setPreferredSize(new Dimension(70, m_sentinelInput.getPreferredSize().height));
        m_sentinelInput.getDocument().addDocumentListener(new DocumentListener() {

            @Override
            public void removeUpdate(final DocumentEvent e) {
                updateSentinelValue();
                getSourceCodePanel().setKernelOptions(getSelectedOpitons());
            }

            @Override
            public void insertUpdate(final DocumentEvent e) {
                updateSentinelValue();
                getSourceCodePanel().setKernelOptions(getSelectedOpitons());
            }

            @Override
            public void changedUpdate(final DocumentEvent e) {
                //does not get fired
            }
        });
        sentinelPanel.add(sentinelLabel);
        sentinelPanel.add(m_minVal);
        sentinelPanel.add(m_maxVal);
        sentinelPanel.add(m_useInput);
        sentinelPanel.add(m_sentinelInput);
        missingPanel.add(sentinelPanel);
        m_missingWarningLabel = new JLabel("");
        missingPanel.add(m_missingWarningLabel);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(missingPanel, gbc);

        //Give user some control over the number of rows to transfer per chunk
        final JPanel chunkingPanel = new JPanel(new FlowLayout());
        chunkingPanel.setBorder(BorderFactory.createTitledBorder("Chunking"));
        chunkingPanel.add(new JLabel("Rows per chunk: "));
        m_chunkSize = new JSpinner(new SpinnerNumberModel(PythonKernelOptions.DEFAULT_CHUNK_SIZE, 1, Integer.MAX_VALUE, 1));
        chunkingPanel.add(m_chunkSize);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(chunkingPanel, gbc);

        return panel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadSettingsFrom(final PythonSourceCodeConfig config) {
        super.loadSettingsFrom(config);
        final PythonKernelOptions kopts = config.getKernelOptions();
        if (kopts.getPythonVersionOption() == PythonKernelOptions.PythonVersionOption.PYTHON3) {
            m_python3.setSelected(true);
        } else {
            m_python2.setSelected(true);
        }
        //Missing value handling
        m_convertToPython.setSelected(kopts.getConvertMissingToPython());
        m_convertFromPython.setSelected(kopts.getConvertMissingFromPython());
        if (kopts.getSentinelOption() == SentinelOption.MIN_VAL) {
            m_minVal.setSelected(true);
        } else if (kopts.getSentinelOption() == SentinelOption.MAX_VAL) {
            m_maxVal.setSelected(true);
        } else {
            m_useInput.setSelected(true);
        }
        m_sentinelInput.setText(kopts.getSentinelValue() + "");
        m_sentinelValue = kopts.getSentinelValue();
        m_chunkSize.setValue(kopts.getChunkSize());
        getSourceCodePanel().setKernelOptions(getSelectedOpitons());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveSettingsTo(final PythonSourceCodeConfig config) {
        super.saveSettingsTo(config);
        config.setKernelOptions(getSelectedPythonVersion(), m_convertToPython.isSelected(),
            m_convertFromPython.isSelected(), getSelectedSentinelOption(), m_sentinelValue,
            ((Integer)m_chunkSize.getValue()).intValue());
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
     * Internal ActionListener used for most components. On change the {@link PythonKernelOptions} in the
     * {@link PythonSourceCodePanel} are updated.
     *
     * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
     *
     */
    private class PythonKernelOptionsListener implements ActionListener {

        @Override
        public void actionPerformed(final ActionEvent e) {

            getSourceCodePanel().setKernelOptions(getSelectedOpitons());

        }

    }

    /**
     * @return the {@link SentinelOption} associated with the current radio button selection
     */

    private SentinelOption getSelectedSentinelOption() {
        SentinelOption so = SentinelOption.MIN_VAL;
        if (m_minVal.isSelected()) {
            so = SentinelOption.MIN_VAL;
        } else if (m_maxVal.isSelected()) {
            so = SentinelOption.MAX_VAL;
        } else if (m_useInput.isSelected()) {
            so = SentinelOption.CUSTOM;
        }
        return so;
    }

    /**
     * Get the python version to use based on the EnforePythonVersion option or the user selection.
     * @return the {@link PythonVersionOption} associated with the current EnforePythonVersion or radio button selection
     */
    private PythonKernelOptions.PythonVersionOption getSelectedPythonVersion() {
        if(m_enforcedVersion == EnforcePythonVersion.PYTHON2) {
            return PythonKernelOptions.PythonVersionOption.PYTHON2;
        } else if (m_enforcedVersion == EnforcePythonVersion.PYTHON3) {
            return PythonKernelOptions.PythonVersionOption.PYTHON3;
        } else {
            if (m_python2.isSelected()) {
                return PythonKernelOptions.PythonVersionOption.PYTHON2;
            } else {
                return PythonKernelOptions.PythonVersionOption.PYTHON3;
            }
        }
    }

    private PythonKernelOptions getSelectedOpitons() {
        return new PythonKernelOptions(getSelectedPythonVersion(),
            m_convertToPython.isSelected(), m_convertFromPython.isSelected(), getSelectedSentinelOption(),
            m_sentinelValue, ((Integer)m_chunkSize.getValue()).intValue());
    }

}
