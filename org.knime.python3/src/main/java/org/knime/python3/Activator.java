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
 *   May 22, 2022 (marcel): created
 */
package org.knime.python3;

import java.util.EventObject;
import java.util.function.Consumer;

import org.eclipse.equinox.internal.p2.engine.PhaseEvent;
import org.eclipse.equinox.internal.p2.engine.RollbackOperationEvent;
import org.eclipse.equinox.internal.provisional.p2.core.eventbus.IProvisioningEventBus;
import org.eclipse.equinox.internal.provisional.p2.core.eventbus.ProvisioningListener;
import org.eclipse.equinox.p2.core.IProvisioningAgent;
import org.eclipse.equinox.p2.engine.PhaseSetFactory;
import org.knime.conda.envbundling.action.InstallCondaEnvironment;
import org.knime.conda.envbundling.action.InstallCondaEnvironment.EnvironmentInstallListener;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // because we are using internal Eclipse API to be notified for ongoing installations
public final class Activator implements BundleActivator, ProvisioningListener, EnvironmentInstallListener {

    /**
     * Singleton gateway queue instance for this bundle and its dependents.
     */
    public static final QueuedPythonGatewayFactory GATEWAY_FACTORY = new QueuedPythonGatewayFactory();

    @Override
    public void start(final BundleContext context) throws Exception {
        applyToProvisioningEventBus(eventBus -> eventBus.addListener(this));
        InstallCondaEnvironment.registerEnvironmentInstallListener(this);
        PythonGatewayCreationGate.INSTANCE.registerListener(PythonGatewayTracker.INSTANCE);
    }

    @Override
    public void stop(final BundleContext bundleContext) throws Exception {
        applyToProvisioningEventBus(eventBus -> eventBus.removeListener(this));
        InstallCondaEnvironment.deregisterEnvironmentInstallListener(this);
        PythonGatewayCreationGate.INSTANCE.deregisterListener(PythonGatewayTracker.INSTANCE);
        GATEWAY_FACTORY.close();
    }


    /* --------------------------------------------------------------------- */
    /* Registering with Eclipse plugin installation/uninstallation           */
    /* --------------------------------------------------------------------- */
    /**
     * Called whenever a ProvisioningEvent is fired by Eclipse's event bus
     */
    @Override
    public void notify(final EventObject o) {
        var logger = NodeLogger.getLogger(Activator.class);
        if (o instanceof PhaseEvent e) {
            logger.info("Received PhaseEvent: " + e.getPhaseId() + " - " + e.getType());
        } else {
            logger.info("Received event: " + o.getClass().getName() + " - " + o);
        }
        if (o instanceof PhaseEvent //
            && (((PhaseEvent)o).getPhaseId().equals(PhaseSetFactory.PHASE_INSTALL) //
                || ((PhaseEvent)o).getPhaseId().equals(PhaseSetFactory.PHASE_UNINSTALL)) //
            && ((PhaseEvent)o).getType() == PhaseEvent.TYPE_START) {
            PythonGatewayCreationGate.INSTANCE.blockPythonCreation();
        } else if (o instanceof PhaseEvent && ((PhaseEvent)o).getPhaseId().equals(PhaseSetFactory.PHASE_CONFIGURE)
            && ((PhaseEvent)o).getType() == PhaseEvent.TYPE_START) {
            // "configure" is the normal phase after install, so we can unlock Python processes again
            PythonGatewayCreationGate.INSTANCE.allowPythonCreation();
        } else if (o instanceof RollbackOperationEvent
            && !PythonGatewayCreationGate.INSTANCE.isPythonGatewayCreationAllowed()) {
            // According to org.eclipse.equinox.internal.p2.engine.Engine.perform() -> L92,
            // a RollbackOperationEvent will be fired if an operation failed, and this event is only fired in that case,
            // so we unlock if we are currently locked.
            PythonGatewayCreationGate.INSTANCE.allowPythonCreation();
        }
    }

    private void applyToProvisioningEventBus(final Consumer<IProvisioningEventBus> consumer) {
        BundleContext context = FrameworkUtil.getBundle(getClass()).getBundleContext();
        ServiceReference<IProvisioningAgent> ref = context.getServiceReference(IProvisioningAgent.class);
        if (ref != null) {
            IProvisioningAgent agent = context.getService(ref);
            try {
                IProvisioningEventBus eventBus =
                    (IProvisioningEventBus)agent.getService(IProvisioningEventBus.SERVICE_NAME);
                if (eventBus != null) {
                    // is null if started from the SDK
                    consumer.accept(eventBus);
                }
            } finally {
                context.ungetService(ref);
            }
        }
    }

    /* --------------------------------------------------------------------- */
    /* Registering with Conda environment creations                          */
    /* --------------------------------------------------------------------- */
    @Override
    public void onInstallStart(final String environmentName) {
        PythonGatewayCreationGate.INSTANCE.blockPythonCreation();
    }

    @Override
    public void onInstallEnd(final String environmentName) {
        PythonGatewayCreationGate.INSTANCE.allowPythonCreation();
    }
}
