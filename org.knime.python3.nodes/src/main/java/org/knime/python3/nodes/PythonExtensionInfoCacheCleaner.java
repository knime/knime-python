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
 *   15 Nov 2024 (chaubold): created
 */
package org.knime.python3.nodes;

import java.util.EventObject;

import org.eclipse.equinox.internal.p2.engine.PhaseEvent;
import org.eclipse.equinox.internal.provisional.p2.core.eventbus.ProvisioningListener;
import org.eclipse.equinox.p2.engine.PhaseSetFactory;

/**
 * We cache Python extension information to make AP startup and node repository loading faster. However, if nodes are
 * only available based on the presence of optional dependencies, installing or uninstalling them might change the set
 * of nodes.
 *
 * To fix that, this {@link PythonExtensionInfoCacheCleaner} will clear the cached extension information whenever an
 * installation or uninstallation begins.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"restriction", "javadoc"})
final class PythonExtensionInfoCacheCleaner implements ProvisioningListener {

    public static final PythonExtensionInfoCacheCleaner INSTANCE = new PythonExtensionInfoCacheCleaner();

    private PythonExtensionInfoCacheCleaner() {

    }

    public static void clearExtensionCaches() {
        PythonExtensionRegistry.PY_EXTENSIONS.stream()//
            .forEach(e -> PythonExtensionParser.clearCache(e.path(), e.bundleVersion()));
    }

    @Override
    public void notify(final EventObject o) {
        if (o instanceof PhaseEvent phaseEvent) {
            if (phaseEvent.getType() == PhaseEvent.TYPE_START
                && (phaseEvent.getPhaseId().equals(PhaseSetFactory.PHASE_INSTALL)
                    || phaseEvent.getPhaseId().equals(PhaseSetFactory.PHASE_UNINSTALL))) {
                clearExtensionCaches();
            }
        }
    }
}
