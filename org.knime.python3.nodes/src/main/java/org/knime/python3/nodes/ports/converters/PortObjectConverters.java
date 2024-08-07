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
 *   2 August 2024 (Ivan Prigarin): created
 */
package org.knime.python3.nodes.ports.converters;

import java.io.IOException;
import java.util.Base64;

import org.knime.base.data.xml.SvgCell;
import org.knime.core.data.image.png.PNGImageContent;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.port.image.ImagePortObjectSpec;
import org.knime.core.node.workflow.capture.WorkflowPortObject;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.nodes.ports.PythonBinaryBlobFileStorePortObject;
import org.knime.python3.nodes.ports.PythonHubAuthenticationPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonBinaryPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonConnectionPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonCredentialPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonImagePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonTablePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonBinaryPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonBinaryPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonConnectionPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonConnectionPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonCredentialPortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonCredentialPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonImagePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonTablePortObject;
import org.knime.python3.nodes.ports.PythonTransientConnectionPortObject;
import org.knime.python3.nodes.ports.PythonWorkflowPortObject;
import org.knime.python3.nodes.ports.converters.PortObjectConverterInterfaces.KnimeToPythonPortObjectConverter;
import org.knime.python3.nodes.ports.converters.PortObjectConverterInterfaces.PythonToKnimePortObjectConverter;
import org.knime.workflowservices.connection.AbstractHubAuthenticationPortObject;

/**
 * Concrete implementations of Port Object Converters.
 *
 * Most Port Objects' converters will need to implement both {@link KnimeToPythonPortObjectConverter} and {@link PythonToKnimePortObjectConverter}
 * to allow bi-directional flow of Port Object data.
 *
 * @author Ivan Prigarin, KNIME GmbH, Konstanz, Germany
 */
public final class PortObjectConverters {

    private PortObjectConverters() {
    }


    /**
     * Bi-directional Port Object converter for {@link BufferedDataTable}.
     */
    public static final class TablePortObjectConverter implements KnimeToPythonPortObjectConverter<BufferedDataTable, PythonTablePortObject>,
    PythonToKnimePortObjectConverter<PurePythonTablePortObject, BufferedDataTable> {

        @Override
        public PythonTablePortObject toPython(final BufferedDataTable portObject, final PortObjectConversionContext context) {
            return new PythonTablePortObject(portObject, context.tableConverter());
        }

        @Override
        public BufferedDataTable fromPython(final PurePythonTablePortObject purePythonPortObject, final PortObjectConversionContext context) {
            try {
                PythonArrowDataSink sink = purePythonPortObject.getPythonArrowDataSink();
                var bdt = context.tableConverter().convertToTable(sink, context.execContext());
                var pythonPortObject = new PythonTablePortObject(bdt, context.tableConverter());
                return pythonPortObject.getPortObject();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while converting from Python", ex);
            } catch (IOException ex) {
                throw new RuntimeException("Failed to convert Python table to KNIME BufferedDataTable", ex);
            }
        }
    }

    /**
     * Bi-directional Port Object converter for {@link PythonBinaryPortObject}.
     */
    public static final class PythonBinaryPortObjectConverter implements KnimeToPythonPortObjectConverter<PythonBinaryBlobFileStorePortObject, PythonBinaryPortObject>,
    PythonToKnimePortObjectConverter<PurePythonBinaryPortObject, PythonBinaryBlobFileStorePortObject> {

        @Override
        public PythonBinaryPortObject toPython(final PythonBinaryBlobFileStorePortObject binaryData, final PortObjectConversionContext context) {
            return new PythonBinaryPortObject(binaryData);
        }

        @Override
        public PythonBinaryBlobFileStorePortObject fromPython(final PurePythonBinaryPortObject purePythonPortObject, final PortObjectConversionContext context) {
            try {
                final var key = purePythonPortObject.getFileStoreKey();
                final var fileStore = context.fileStoresByKey().get(key);
                var spec = PythonBinaryPortObjectSpec.fromJsonString(purePythonPortObject.getSpec().toJsonString()).getPortObjectSpec();
                var pythonPortObject = new PythonBinaryPortObject(PythonBinaryBlobFileStorePortObject.create(fileStore, spec));
                return pythonPortObject.getPortObject();
            } catch (IOException ex) {
                throw new RuntimeException("Failed to convert Python binary data to KNIME", ex);
            }

        }
    }

    /**
     * Bi-directional Port Object converter for {@link PythonTransientConnectionPortObject}.
     */
    public static final class PythonConnectionPortObjectConverter implements KnimeToPythonPortObjectConverter<PythonTransientConnectionPortObject, PythonConnectionPortObject>,
    PythonToKnimePortObjectConverter<PurePythonConnectionPortObject, PythonTransientConnectionPortObject> {

        @Override
        public PythonConnectionPortObject toPython(final PythonTransientConnectionPortObject binaryData, final PortObjectConversionContext context) {
            return new PythonConnectionPortObject(binaryData);
        }

        @Override
        public PythonTransientConnectionPortObject fromPython(final PurePythonConnectionPortObject purePythonPortObject, final PortObjectConversionContext context) {
            var spec = PythonConnectionPortObjectSpec.fromJsonString(purePythonPortObject.getSpec().toJsonString()).getPortObjectSpec();
            final var pid = purePythonPortObject.getPid();
            var pythonPortObject =  new PythonConnectionPortObject(PythonTransientConnectionPortObject.create(spec, pid));
            return pythonPortObject.getPortObject();
        }
    }

    /**
     * Uni-directional Port Object converter for {@link ImagePortObject}.
     */
    public static final class ImagePortObjectConverter implements PythonToKnimePortObjectConverter<PurePythonImagePortObject, ImagePortObject> {

        @Override
        public ImagePortObject fromPython(final PurePythonImagePortObject purePythonPortObject, final PortObjectConversionContext context) {
            final var bytes = Base64.getDecoder().decode(purePythonPortObject.getImageBytes().getBytes());
            ImagePortObjectSpec spec;

            if (PythonImagePortObject.isPngBytes(bytes)) {
                spec = new ImagePortObjectSpec(PNGImageContent.TYPE);
            } else if (PythonImagePortObject.isSvgBytes(bytes)) {
                spec = new ImagePortObjectSpec(SvgCell.TYPE);
            } else {
                throw new UnsupportedOperationException("Unsupported image format detected.");
            }

            var pythonPortObject = new PythonImagePortObject(bytes, spec);
            return pythonPortObject.getPortObject();
        }
    }

    /**
     * Bi-directional Port Object converter for {@link CredentialPortObject}.
     */
    public static final class PythonCredentialPortObjectConverter implements KnimeToPythonPortObjectConverter<CredentialPortObject, PythonCredentialPortObject>,
    PythonToKnimePortObjectConverter<PurePythonCredentialPortObject, CredentialPortObject> {

        @Override
        public PythonCredentialPortObject toPython(final CredentialPortObject portObject, final PortObjectConversionContext context) {
            return new PythonCredentialPortObject(portObject);
        }

        @Override
        public CredentialPortObject fromPython(final PurePythonCredentialPortObject purePythonPortObject, final PortObjectConversionContext context) {
            var spec = PythonCredentialPortObjectSpec.fromJsonString(purePythonPortObject.getSpec().toJsonString()).getPortObjectSpec();
            var pythonPortObject = PythonCredentialPortObject.createFromKnimeSpec(spec);
            return pythonPortObject.getPortObject();

        }
    }

    /**
     * Uni-directional Port Object converter for {@link WorkflowPortObject}.
     */
    public static final class PythonWorkflowPortObjectConverter implements KnimeToPythonPortObjectConverter<WorkflowPortObject, PythonWorkflowPortObject> {

        @Override
        public PythonWorkflowPortObject toPython(final WorkflowPortObject workflow, final PortObjectConversionContext context) {
            return new PythonWorkflowPortObject(workflow, context.tableConverter());
        }
    }

    /**
     * Uni-directional Port Object converter for `HubAuthenticationPortObject`.
     *
     * The actual class of the Port Object is not available to us due to being closed-source, so we use the open source
     * {@link AbstractHubAuthenticationPortObject} interface instead.
     */
    public static final class PythonHubAuthenticationPortObjectConverter implements KnimeToPythonPortObjectConverter<AbstractHubAuthenticationPortObject, PythonHubAuthenticationPortObject> {

        @Override
        public PythonHubAuthenticationPortObject toPython(final AbstractHubAuthenticationPortObject credentialsPortObject, final PortObjectConversionContext context) {
            return new PythonHubAuthenticationPortObject(credentialsPortObject);
        }
    }

}
