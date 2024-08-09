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

import org.knime.base.data.xml.SvgCell;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.image.png.PNGImageContent;
import org.knime.core.node.port.image.ImagePortObjectSpec;
import org.knime.core.node.workflow.capture.WorkflowPortObjectSpec;
import org.knime.core.table.virtual.serialization.AnnotatedColumnarSchemaSerializer;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec;
import org.knime.python3.nodes.ports.PythonHubAuthenticationPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonBinaryPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonConnectionPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonCredentialPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonImagePortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonTablePortObjectSpec;
import org.knime.python3.nodes.ports.PythonTransientConnectionPortObjectSpec;
import org.knime.python3.nodes.ports.PythonWorkflowPortObject.PythonWorkflowPortObjectSpec;
import org.knime.python3.types.port.KnimeToPythonPortObjectSpecConverter;
import org.knime.python3.types.port.PortObjectSpecConversionContext;
import org.knime.python3.types.port.PythonToKnimePortObjectSpecConverter;
import org.knime.workflowservices.connection.AbstractHubAuthenticationPortObjectSpec;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Concrete implementations of Port Object Spec Converters.
 *
 * Most Port Object Specs' converters will need to implement both {@link KnimeToPythonPortObjectSpecConverter} and
 * {@link PythonToKnimePortObjectSpecConverter} to allow bi-directional flow of Port Object Specs.
 *
 * @author Ivan Prigarin, KNIME GmbH, Konstanz, Germany
 */
public final class PortObjectSpecConverters {

    private PortObjectSpecConverters() {
    }

    /**
     * Bi-directional Port Object Spec converter for {@link DataTableSpec}.
     */
    public static final class TablePortObjectSpecConverter
        extends AbstractJsonPythonToKnimePortObjectSpecConverter<PythonTablePortObjectSpec, DataTableSpec>
        implements KnimeToPythonPortObjectSpecConverter<DataTableSpec, PythonTablePortObjectSpec> {

        @Override
        public PythonTablePortObjectSpec convert(final DataTableSpec spec,
            final PortObjectSpecConversionContext context) {
            return new PythonTablePortObjectSpec(spec);
        }

        @Override
        public DataTableSpec parse(final JsonNode rootNode) {
            final var acs = AnnotatedColumnarSchemaSerializer.load(rootNode);
            var spec =
                new PythonTablePortObjectSpec(PythonArrowDataUtils.createDataTableSpec(acs, acs.getColumnNames()));
            return spec.getPortObjectSpec();
        }
    }

    /**
     * Bi-directional Port Object Spec converter for {@link PythonBinaryBlobPortObjectSpec}.
     */
    public static final class PythonBinaryPortObjectSpecConverter extends
        AbstractJsonPythonToKnimePortObjectSpecConverter<PythonBinaryPortObjectSpec, PythonBinaryBlobPortObjectSpec>
        implements KnimeToPythonPortObjectSpecConverter<PythonBinaryBlobPortObjectSpec, PythonBinaryPortObjectSpec> {

        @Override
        public PythonBinaryPortObjectSpec convert(final PythonBinaryBlobPortObjectSpec spec,
            final PortObjectSpecConversionContext context) {
            return new PythonBinaryPortObjectSpec(spec);
        }

        @Override
        public PythonBinaryBlobPortObjectSpec parse(final JsonNode rootNode) {
            var spec = new PythonBinaryPortObjectSpec(PythonBinaryBlobPortObjectSpec.fromJson(rootNode));
            return spec.getPortObjectSpec();
        }
    }

    /**
     * Bi-directional Port Object Spec converter for {@link ImagePortObjectSpec}.
     */
    public static final class PythonImagePortObjectSpecConverter
        extends AbstractJsonPythonToKnimePortObjectSpecConverter<PythonImagePortObjectSpec, ImagePortObjectSpec>
        implements KnimeToPythonPortObjectSpecConverter<ImagePortObjectSpec, PythonImagePortObjectSpec> {

        @Override
        public PythonImagePortObjectSpec convert(final ImagePortObjectSpec spec,
            final PortObjectSpecConversionContext context) {
            return new PythonImagePortObjectSpec(spec);
        }

        @Override
        public ImagePortObjectSpec parse(final JsonNode rootNode) {
            final var format = rootNode.get("format").asText();
            if (format.equals("png")) {
                var spec = new PythonImagePortObjectSpec(new ImagePortObjectSpec(PNGImageContent.TYPE));
                return spec.getPortObjectSpec();
            } else if (format.equals("svg")) {
                var spec = new PythonImagePortObjectSpec(new ImagePortObjectSpec(SvgCell.TYPE));
                return spec.getPortObjectSpec();
            } else {
                throw new IllegalStateException("Unsupported image format: " + format + ".");
            }
        }
    }

    /**
     * Bi-directional Port Object Spec converter for {@link PythonTransientConnectionPortObjectSpec}.
     */
    public static final class PythonConnectionPortObjectSpecConverter extends
        AbstractJsonPythonToKnimePortObjectSpecConverter<PythonConnectionPortObjectSpec, PythonTransientConnectionPortObjectSpec>
        implements
        KnimeToPythonPortObjectSpecConverter<PythonTransientConnectionPortObjectSpec, PythonConnectionPortObjectSpec> {

        @Override
        public PythonConnectionPortObjectSpec convert(final PythonTransientConnectionPortObjectSpec spec,
            final PortObjectSpecConversionContext context) {
            return new PythonConnectionPortObjectSpec(spec);
        }

        @Override
        public PythonTransientConnectionPortObjectSpec parse(final JsonNode rootNode) {
            var spec = new PythonConnectionPortObjectSpec(PythonTransientConnectionPortObjectSpec.fromJson(rootNode));
            return spec.getPortObjectSpec();
        }
    }

    /**
     * Bi-directional Port Object Spec converter for {@link CredentialPortObjectSpec}.
     */
    public static final class PythonCredentialPortObjectSpecConverter extends
        AbstractJsonPythonToKnimePortObjectSpecConverter<PythonCredentialPortObjectSpec, CredentialPortObjectSpec>
        implements KnimeToPythonPortObjectSpecConverter<CredentialPortObjectSpec, PythonCredentialPortObjectSpec> {

        @Override
        public PythonCredentialPortObjectSpec convert(final CredentialPortObjectSpec spec,
            final PortObjectSpecConversionContext context) {
            return new PythonCredentialPortObjectSpec(spec);
        }

        @Override
        public CredentialPortObjectSpec parse(final JsonNode rootNode) {
            try {
                final String serializedXMLString = rootNode.get("data").asText();

                CredentialPortObjectSpec credentialPortObjectSpec =
                    PythonCredentialPortObjectSpec.loadFromXMLCredentialPortObjectSpecString(serializedXMLString);
                var spec = new PythonCredentialPortObjectSpec(credentialPortObjectSpec);
                return spec.getPortObjectSpec();

            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException ex) {
                throw new IllegalStateException("Could not parse PythonCredentialPortObject from given JSON data", ex);
            }
        }
    }

    /**
     * Uni-directional Port Object Spec converter for {@link WorkflowPortObjectSpec}.
     */
    public static final class PythonWorkflowPortObjectSpecConverter
        implements KnimeToPythonPortObjectSpecConverter<WorkflowPortObjectSpec, PythonWorkflowPortObjectSpec> {

        @Override
        public PythonWorkflowPortObjectSpec convert(final WorkflowPortObjectSpec spec,
            final PortObjectSpecConversionContext context) {
            return new PythonWorkflowPortObjectSpec(spec);
        }
    }

    /**
     * Uni-directional Port Object Spec converter for `HubAuthenticationPortObjectSpec`.
     */
    public static final class PythonHubAuthenticationPortObjectSpecConverter implements
        KnimeToPythonPortObjectSpecConverter<AbstractHubAuthenticationPortObjectSpec, PythonHubAuthenticationPortObjectSpec> {

        @Override
        public PythonHubAuthenticationPortObjectSpec convert(final AbstractHubAuthenticationPortObjectSpec spec,
            final PortObjectSpecConversionContext context) {
            return new PythonHubAuthenticationPortObjectSpec(spec);
        }
    }

}
