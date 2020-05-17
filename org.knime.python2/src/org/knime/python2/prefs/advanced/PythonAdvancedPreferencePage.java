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
 */
package org.knime.python2.prefs.advanced;

import java.util.List;

import org.eclipse.swt.widgets.Composite;
import org.knime.python2.config.PythonConfig;
import org.knime.python2.config.advanced.PythonKernelQueueConfig;
import org.knime.python2.kernel.PythonKernelQueue;
import org.knime.python2.prefs.AbstractPythonPreferencePage;

/**
 * Preference page for advanced configurations of the org.knime.python2 plug-in (e.g. of the {@link PythonKernelQueue}).
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class PythonAdvancedPreferencePage extends AbstractPythonPreferencePage {

    /**
     * Constructs the advanced preference page of the KNIME Python integration.
     */
    public PythonAdvancedPreferencePage() {
        super(PythonAdvancedPreferences.CURRENT, PythonAdvancedPreferences.DEFAULT);
    }

    @Override
    @SuppressWarnings("unused") // References to some panels are not needed; everything is done in their constructor.
    protected void populatePageBody(final Composite container, final List<PythonConfig> configs) {
        // Kernel queue configuration:
        final PythonKernelQueueConfig kernelQueueConfig = new PythonKernelQueueConfig();
        configs.add(kernelQueueConfig);
        new PythonKernelQueuePreferencePanel(kernelQueueConfig, container);
    }

    @Override
    protected void reflectLoadedConfigurations() {
        // Nothing to do.
    }

    @Override
    protected void setupHooks() {
        // Nothing to do.
    }

    @Override
    public boolean performOk() {
        final boolean result = super.performOk();
        if (result) {
            reconfigureKernelQueue();
        }
        return result;
    }

    @Override
    protected void performApply() {
        super.performApply();
        reconfigureKernelQueue();
    }

    @Override
    protected void performDefaults() {
        super.performDefaults();
        reconfigureKernelQueue();
    }

    private static void reconfigureKernelQueue() {
        PythonKernelQueue.reconfigureKernelQueue(PythonAdvancedPreferences.getMaximumNumberOfIdlingProcesses(),
            PythonAdvancedPreferences.getExpirationDurationInMinutes());
    }
}
