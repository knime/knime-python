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

import org.knime.base.data.xml.SvgCell;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.image.png.PNGImageContent;
import org.knime.core.node.port.image.ImagePortObjectSpec;
import org.knime.core.node.workflow.capture.WorkflowPortObjectSpec;
import org.knime.core.table.virtual.serialization.AnnotatedColumnarSchemaSerializer;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonBinaryPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonConnectionPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonImagePortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonTablePortObjectSpec;
import org.knime.python3.nodes.ports.PythonTransientConnectionPortObjectSpec;
import org.knime.python3.nodes.ports.PythonWorkflowPortObject.PythonWorkflowPortObjectSpec;
import org.knime.python3.nodes.ports.converters.PortObjectSpecConverterInterfaces.KnimeToPythonPortObjectSpecConverter;
import org.knime.python3.nodes.ports.converters.PortObjectSpecConverterInterfaces.PythonToKnimePortObjectSpecConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
        implements KnimeToPythonPortObjectSpecConverter<DataTableSpec, PythonTablePortObjectSpec>,
        PythonToKnimePortObjectSpecConverter<DataTableSpec> {

        @Override
        public PythonTablePortObjectSpec toPython(final DataTableSpec spec) {
            return new PythonTablePortObjectSpec(spec);
        }

        @Override
        public DataTableSpec fromJsonString(final String jsonData) {
            final var om = new ObjectMapper();
            try {
                final var rootNode = om.readTree(jsonData);
                final var acs = AnnotatedColumnarSchemaSerializer.load(rootNode);
                var spec =
                    new PythonTablePortObjectSpec(PythonArrowDataUtils.createDataTableSpec(acs, acs.getColumnNames()));
                return spec.getPortObjectSpec();
            } catch (JsonMappingException ex) {
                throw new IllegalStateException("Could not parse PythonTablePortObjectSpec from given JSON data", ex);
            } catch (JsonProcessingException ex) { // NOSONAR: if we don't split this block up, Eclipse doesn't like it for some reason
                throw new IllegalStateException("Could not parse PythonTablePortObjectSpec from given JSON data", ex);
            }
        }
    }

    /**
     * Bi-directional Port Object Spec converter for {@link PythonBinaryBlobPortObjectSpec}.
     */
    public static final class PythonBinaryPortObjectSpecConverter
        implements KnimeToPythonPortObjectSpecConverter<PythonBinaryBlobPortObjectSpec, PythonBinaryPortObjectSpec>,
        PythonToKnimePortObjectSpecConverter<PythonBinaryBlobPortObjectSpec> {

        @Override
        public PythonBinaryPortObjectSpec toPython(final PythonBinaryBlobPortObjectSpec spec) {
            return new PythonBinaryPortObjectSpec(spec);
        }

        @Override
        public PythonBinaryBlobPortObjectSpec fromJsonString(final String jsonData) {
            final var om = new ObjectMapper();
            try {
                final var rootNode = om.readTree(jsonData);
                var spec = new PythonBinaryPortObjectSpec(PythonBinaryBlobPortObjectSpec.fromJson(rootNode));
                return spec.getPortObjectSpec();
            } catch (JsonMappingException ex) {
                throw new IllegalStateException("Could not parse PythonBinaryPortObjectSpec from given Json data", ex);
            } catch (JsonProcessingException ex) { // NOSONAR: if we don't split this block up, Eclipse doesn't like it for some reason
                throw new IllegalStateException("Could not parse PythonBinaryPortObjectSpec from given Json data", ex);
            }
        }
    }

    /**
     * Bi-directional Port Object Spec converter for {@link ImagePortObjectSpec}.
     */
    public static final class PythonImagePortObjectSpecConverter
        implements KnimeToPythonPortObjectSpecConverter<ImagePortObjectSpec, PythonImagePortObjectSpec>,
        PythonToKnimePortObjectSpecConverter<ImagePortObjectSpec> {

        @Override
        public PythonImagePortObjectSpec toPython(final ImagePortObjectSpec spec) {
            return new PythonImagePortObjectSpec(spec);
        }

        @Override
        public ImagePortObjectSpec fromJsonString(final String jsonData) {
            final var om = new ObjectMapper();
            try {
                final var rootNode = om.readTree(jsonData);
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
            } catch (JsonMappingException ex) {
                throw new IllegalStateException("Could not parse PythonImagePortObjectSpec from given JSON data", ex);
            } catch (JsonProcessingException ex) { // NOSONAR: Eclipse requires explicit handling of this exception
                throw new IllegalStateException("Could not parse PythonImagePortObjectSpec from given Json data", ex);
            }
        }
    }

    /**
     * Bi-directional Port Object Spec converter for {@link PythonTransientConnectionPortObjectSpec}.
     */
    public static final class PythonConnectionPortObjectSpecConverter implements
        KnimeToPythonPortObjectSpecConverter<PythonTransientConnectionPortObjectSpec, PythonConnectionPortObjectSpec>,
        PythonToKnimePortObjectSpecConverter<PythonTransientConnectionPortObjectSpec> {

        @Override
        public PythonConnectionPortObjectSpec toPython(final PythonTransientConnectionPortObjectSpec spec) {
            return new PythonConnectionPortObjectSpec(spec);
        }

        @Override
        public PythonTransientConnectionPortObjectSpec fromJsonString(final String jsonData) {
            final var om = new ObjectMapper();
            try {
                final var rootNode = om.readTree(jsonData);
                var spec =
                    new PythonConnectionPortObjectSpec(PythonTransientConnectionPortObjectSpec.fromJson(rootNode));
                return spec.getPortObjectSpec();
            } catch (JsonMappingException ex) {
                throw new IllegalStateException("Could not parse PythonConnectionPortObjectSpec from given Json data",
                    ex);
            } catch (JsonProcessingException ex) { // NOSONAR: if we don't split this block up, Eclipse doesn't like it for some reason
                throw new IllegalStateException("Could not parse PythonConnectionPortObjectSpec from given Json data",
                    ex);
            }
        }
    }

    /**
     * Uni-directional Port Object Spec converter for {@link WorkflowPortObjectSpec}.
     */
    public static final class PythonWorkflowPortObjectSpecConverter
        implements KnimeToPythonPortObjectSpecConverter<WorkflowPortObjectSpec, PythonWorkflowPortObjectSpec> {

        @Override
        public PythonWorkflowPortObjectSpec toPython(final WorkflowPortObjectSpec spec) {
            return new PythonWorkflowPortObjectSpec(spec);
        }
    }

}
