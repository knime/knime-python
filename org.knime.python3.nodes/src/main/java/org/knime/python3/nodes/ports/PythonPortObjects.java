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
 *   11 May 2022 (Carsten Haubold): created
 */
package org.knime.python3.nodes.ports;

import java.io.IOException;
import java.util.Arrays;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.table.virtual.serialization.AnnotatedColumnarSchemaSerializer;
import org.knime.python3.PythonDataSource;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataSource;
import org.knime.python3.arrow.PythonArrowDataUtils;
import org.knime.python3.arrow.PythonArrowTableConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * Collection of Interfaces and adapters to pass PortObjects between knime and Python
 *
 * @author Carsten Haubold
 */
public final class PythonPortObjects {

    private PythonPortObjects() {
    }

    /**
     * General PortObject interface used to pass data between Python and KNIME
     *
     * @author Carsten Haubold
     */
    public interface PythonPortObject {
        /**
         * @return the class name of the Java {@link PortObject} that is being wrapped here. Used for registration
         */
        String getJavaClassName();
    }

    /**
     * When tabular data is passed to KNIME from Python, it will be packaged as {@link PurePythonTablePortObject}
     *
     * @author Carsten Haubold
     */
    public interface PurePythonTablePortObject extends PythonPortObject {
        /**
         * @return The data sink containing the table
         */
        PythonArrowDataSink getPythonArrowDataSink();
    }

    /**
     * When binary data is passed to KNIME from Python, it will be packaged as {@link PurePythonBinaryPortObject}
     *
     * @author Carsten Haubold
     */
    public interface PurePythonBinaryPortObject extends PythonPortObject {
        /**
         * @return the binary data
         */
        byte[] getBinaryData();

        /**
         * @return The string that identifies the binary port object content, must match the ID of the corresponding
         *         {@link PythonBinaryPortObjectSpec}
         */
        String getPortId();
    }

    /**
     * KNIME-side interface to obtain a {@link PortObject} from a {@link PythonPortObject}
     */
    public interface PortObjectProvider {
        /**
         * @return the wrapped port object
         */
        PortObject getPortObject();
    }

    /**
     * {@link PythonPortObject} implementation for {@link BufferedDataTable}s used and populated on the Java side.
     */
    public static final class PythonTablePortObject implements PythonPortObject, PortObjectProvider {
        private final BufferedDataTable m_data;

        private final PythonArrowTableConverter m_tableConverter;

        /**
         * Create a {@link PythonTablePortObject} from a {@link BufferedDataTable}.
         *
         * @param table The table data going along this port
         * @param tableConverter Used to convert the table to an {@link PythonArrowDataSource}
         */
        public PythonTablePortObject(final BufferedDataTable table, final PythonArrowTableConverter tableConverter) {
            m_data = table;
            m_tableConverter = tableConverter;
        }

        /**
         * Construct a {@link PythonTablePortObject} from a {@link PurePythonTablePortObject}
         *
         * @param portObject The {@link PurePythonTablePortObject} received from Python
         * @param tableConverter The {@link PythonArrowTableConverter} used to convert tables from {@link PythonArrowDataSink}s
         * @param execContext The current {@link ExecutionContext}
         * @return the {@link PythonTablePortObject}
         */
        public static PythonTablePortObject fromPurePython(final PurePythonTablePortObject portObject,
            final PythonArrowTableConverter tableConverter, final ExecutionContext execContext) {
            try {
                final var sink = portObject.getPythonArrowDataSink();
                final var bdt = tableConverter.convertToTable(sink, execContext);
                return new PythonTablePortObject(bdt, tableConverter);
            } catch (IOException | InterruptedException ex) {
                // TODO: better error handling!
                throw new IllegalStateException("Could not retrieve BufferedDataTable from Python", ex);
            }
        }

        @Override
        public PortObject getPortObject() {
            return m_data;
        }

        @Override
        public String getJavaClassName() {
            return BufferedDataTable.class.getName();
        }

        /**
         * Used on the Python side to obtain access to the table.
         *
         * @return The PythonDataSource
         * @throws IOException In case of a I/O error
         */
        public PythonDataSource getDataSource() throws IOException {
            // TODO: dispatch the task _before_ we actually access the data from Python?!
            return m_tableConverter.createSource(m_data);
        }
    }

    /**
     * {@link PythonPortObject} implementation for {@link PythonBinaryBlobFileStorePortObject}s used and populated on
     * the Java side.
     */
    public static final class PythonBinaryPortObject implements PythonPortObject, PortObjectProvider {
        private final PythonBinaryBlobFileStorePortObject m_data;

        /**
         * Create a {@link PythonBinaryPortObject} with data
         *
         * @param binaryData The data to wrap in this port object
         * @param tableConverter Unused here, but required because fromPurePython is called via reflection from
         *            {@link PythonPortObjectTypeRegistry}
         */
        public PythonBinaryPortObject(final PythonBinaryBlobFileStorePortObject binaryData,
            final PythonArrowTableConverter tableConverter) {
            m_data = binaryData;
        }

        /**
         * Create a PythonBinaryPortObject from a PurePythonBinaryPortObject
         *
         * @param portObject The {@link PurePythonBinaryPortObject} coming from Python
         * @param tableConverter Not used here, just needed because fromPurePython is called via reflection from
         *            {@link PythonPortObjectTypeRegistry}
         * @param execContext The current {@link ExecutionContext}
         * @return new {@link PythonBinaryPortObject} wrapping the binary data
         */
        public static PythonBinaryPortObject fromPurePython(final PurePythonBinaryPortObject portObject,
            final PythonArrowTableConverter tableConverter, final ExecutionContext execContext) {
            try {
                return new PythonBinaryPortObject(PythonBinaryBlobFileStorePortObject.create(portObject.getBinaryData(),
                    portObject.getPortId(), execContext), null);
            } catch (IOException ex) {
                throw new IllegalStateException("Could not create PythonBinaryPortObject", ex);
            }
        }

        @Override
        public PortObject getPortObject() {
            return m_data;
        }

        @Override
        public String getJavaClassName() {
            return PythonBinaryBlobFileStorePortObject.class.getName();
        }

        /**
         * Used on the Python side to access the binary data.
         *
         * @return The binary data of this port object
         */
        public byte[] getBinaryData() {
            return m_data.getBinaryBlob();
        }
    }

    /**
     * A {@link PortObjectSpec} variant that is used to communicate between Python and KNIME
     */
    public interface PythonPortObjectSpec {
        /**
         * @return the class name of the Java PortObjectSpec that is being wrapped here. Used for registration
         */
        String getJavaClassName();

        /**
         * @return a JSON string representation of this spec
         */
        String toJsonString();
    }

    /**
     * KNIME-side interface to obtain a {@link PortObjectSpec} from a {@link PythonPortObjectSpec}
     */
    public interface PortObjectSpecProvider {
        /**
         * @return the wrapped port object
         */
        PortObjectSpec getPortObjectSpec();
    }

    /**
     * {@link PythonPortObjectSpec} specialization wrapping a {@link DataTableSpec}
     */
    public static final class PythonTablePortObjectSpec implements PythonPortObjectSpec, PortObjectSpecProvider {
        private final DataTableSpec m_spec;

        /**
         * Create a {@link PythonTablePortObjectSpec} with a {@link DataTableSpec}
         *
         * @param spec The {@link DataTableSpec} to wrap
         */
        public PythonTablePortObjectSpec(final DataTableSpec spec) {
            m_spec = spec;
        }

        /**
         * Create a {@link PythonTablePortObjectSpec} by parsing the given JSON data.
         *
         * Note: Objects created this way cannot be converted to JSON again because the writeFileStoreHandler is not
         * set!
         *
         * @param jsonData The JSON encoded data table spec
         * @return A new {@link PythonTablePortObjectSpec}
         */
        public static PythonTablePortObjectSpec fromJsonString(final String jsonData) {
            final var om = new ObjectMapper();
            try {
                final var rootNode = om.readTree(jsonData);
                final var acs = AnnotatedColumnarSchemaSerializer.load(rootNode);
                return new PythonTablePortObjectSpec(
                    PythonArrowDataUtils.createDataTableSpec(acs, acs.getColumnNames()));
            } catch (JsonMappingException ex) {
                throw new IllegalStateException("Could not parse PythonTablePortObjectSpec from given Json data", ex);
            } catch (JsonProcessingException ex) { // NOSONAR: if we don't split this block up, Eclipse doesn't like it for some reason
                throw new IllegalStateException("Could not parse PythonTablePortObjectSpec from given Json data", ex);
            }
        }

        @Override
        public String toJsonString() {
            return TableSpecSerializationUtils.serializeTableSpec(m_spec);
        }

        @Override
        public String getJavaClassName() {
            return DataTableSpec.class.getName();
        }

        @Override
        public PortObjectSpec getPortObjectSpec() {
            return m_spec;
        }
    }

    /**
     * The {@link PythonBinaryPortObjectSpec} specifies the contents of a Port of binary type that can be populated and
     * read on the Python side.
     */
    public static final class PythonBinaryPortObjectSpec implements PythonPortObjectSpec, PortObjectSpecProvider {
        private final PythonBinaryBlobPortObjectSpec m_spec;

        /**
         * Create a {@link PythonBinaryPortObjectSpec}
         *
         * @param spec
         */
        public PythonBinaryPortObjectSpec(final PythonBinaryBlobPortObjectSpec spec) { // NOSONAR
            m_spec = spec;
        }

        /**
         * Construct a {@link PythonBinaryPortObjectSpec} from a JSON representation
         *
         * @param jsonData The JSON serialized spec
         * @return the {@link PythonBinaryPortObjectSpec} as read from JSON
         */
        public static PythonBinaryPortObjectSpec fromJsonString(final String jsonData) {
            final var om = new ObjectMapper();
            try {
                final var rootNode = om.readTree(jsonData);
                return new PythonBinaryPortObjectSpec(PythonBinaryBlobPortObjectSpec.fromJson(rootNode));
            } catch (JsonMappingException ex) {
                throw new IllegalStateException("Could not parse PythonTablePortObjectSpec from given Json data", ex);
            } catch (JsonProcessingException ex) { // NOSONAR: if we don't split this block up, Eclipse doesn't like it for some reason
                throw new IllegalStateException("Could not parse PythonTablePortObjectSpec from given Json data", ex);
            }
        }

        @Override
        public String toJsonString() {
            return m_spec.toJson(JsonNodeFactory.instance).toString();
        }

        @Override
        public String getJavaClassName() {
            return PythonBinaryBlobPortObjectSpec.class.getName();
        }

        @Override
        public PortObjectSpec getPortObjectSpec() {
            return m_spec;
        }
    }

    /**
     * Convert port type encoded as string to a {@link PortType}. Possible values are TABLE and BYTES, where BYTES is
     * followed by a Port Type ID as in "BYTES=org.knime.python3.nodes.test.porttype"
     *
     * @param identifier Port type identifier (TABLE or BYTES currently).
     * @return {@link PortType}
     */
    public static PortType getPortTypeForIdentifier(final String identifier) {
        if (identifier.equals("PortType.TABLE")) {
            return BufferedDataTable.TYPE;
        } else if (identifier.startsWith("PortType.BYTES")) {
            return PythonBinaryBlobFileStorePortObject.TYPE;
        }

        throw new IllegalStateException("Found unknown PortType: " + identifier);
    }

    /**
     * Convert port types encoded as string to {@link PortType}. The order is important. Possible values are TABLE and
     * BYTES, where BYTES is followed by a Port Type ID as in "BYTES=org.knime.python3.nodes.test.porttype"
     *
     * @param identifiers Port type identifiers (TABLE or BYTES currently).
     * @return {@link PortType}s
     */
    public static PortType[] getPortTypesForIdentifiers(final String[] identifiers) {
        return Arrays.stream(identifiers).map(PythonPortObjects::getPortTypeForIdentifier).toArray(PortType[]::new);
    }
}
