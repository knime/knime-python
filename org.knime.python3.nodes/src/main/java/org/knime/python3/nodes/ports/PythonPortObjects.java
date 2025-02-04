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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import javax.xml.parsers.DocumentBuilderFactory;

import org.knime.base.data.xml.SvgCell;
import org.knime.base.data.xml.SvgImageContent;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.image.ImageContent;
import org.knime.core.data.image.png.PNGImageContent;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.port.AbstractSimplePortObjectSpec.AbstractSimplePortObjectSpecSerializer;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.port.image.ImagePortObjectSpec;
import org.knime.credentials.base.Credential;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.credentials.base.oauth.api.HttpAuthorizationHeaderCredentialValue;
import org.knime.python3.PythonDataSource;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataSource;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
         * @return the spec
         */
        PythonPortObjectSpec getSpec();

        /**
         * @return The key identifying the file store
         */
        String getFileStoreKey();
    }

    /**
     * When connection data is passed to KNIME from Python, it will be packaged as
     * {@link PurePythonConnectionPortObject}
     *
     * @author Carsten Haubold
     */
    public interface PurePythonConnectionPortObject extends PurePythonBinaryPortObject {

        /**
         * @return The process ID of the Python process in which the port object was created
         */
        int getPid();
    }

    /**
     * When image data in bytes is passed to KNIME from Python, it will be packaged as {@link PurePythonImagePortObject}
     *
     * @author Ivan Prigarin
     */
    public interface PurePythonImagePortObject extends PythonPortObject {
        /**
         * @return The string containing the encoded image bytes
         */
        String getImageBytes();

    }

    /**
     * When connection info is passed to KNIME from Python, it will be packaged as
     * {@link PurePythonCredentialPortObject}
     *
     * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
     */
    public interface PurePythonCredentialPortObject extends PythonPortObject {
        /**
         * @return the spec
         */
        PythonPortObjectSpec getSpec();
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

        @Override
        public BufferedDataTable getPortObject() {
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

        private final PythonBinaryPortObjectSpec m_spec;

        /**
         * Create a {@link PythonBinaryPortObject} with data
         *
         * @param binaryData The data to wrap in this port object
         */
        public PythonBinaryPortObject(final PythonBinaryBlobFileStorePortObject binaryData) {
            m_data = binaryData;
            m_spec = new PythonBinaryPortObjectSpec(binaryData.getSpec());
        }

        @Override
        public PythonBinaryBlobFileStorePortObject getPortObject() {
            return m_data;
        }

        @Override
        public String getJavaClassName() {
            return PythonBinaryBlobFileStorePortObject.class.getName();
        }

        /**
         * Used on the Python side to get the file path where to read the binary data
         *
         * @return The file path where to read the binary data
         */
        public String getFilePath() {
            return m_data.getFilePath();
        }

        /**
         * @return the {@link PythonBinaryPortObjectSpec} for this port object
         */
        public PythonBinaryPortObjectSpec getSpec() {
            return m_spec;
        }
    }

    /**
     * {@link PythonPortObject} implementation for {@link PythonTransientConnectionPortObject}s used and populated on
     * the Java side.
     *
     * @since 5.1
     */
    public static final class PythonConnectionPortObject implements PythonPortObject, PortObjectProvider {
        private final PythonTransientConnectionPortObject m_data;

        private final PythonConnectionPortObjectSpec m_spec;

        /**
         * Create a {@link PythonBinaryPortObject} with data
         *
         * @param binaryData The data to wrap in this port object
         */
        public PythonConnectionPortObject(final PythonTransientConnectionPortObject binaryData) {
            m_data = binaryData;
            m_spec = new PythonConnectionPortObjectSpec(binaryData.getSpec());
        }

        @Override
        public PythonTransientConnectionPortObject getPortObject() {
            return m_data;
        }

        @Override
        public String getJavaClassName() {
            return PythonTransientConnectionPortObject.class.getName();
        }

        /**
         * @return The {@link PythonConnectionPortObjectSpec}
         */
        public PythonConnectionPortObjectSpec getSpec() {
            return m_spec;
        }
    }

    /**
     * {@link PythonPortObject} implementation for {@link PythonImagePortObject}s used and populated on the Java side.
     */
    public static final class PythonImagePortObject implements PythonPortObject, PortObjectProvider {
        private final byte[] m_bytes;

        private final PythonImagePortObjectSpec m_spec;

        /**
         * Constructor for creating a PythonImagePortObject.
         *
         * @param imgBytes the array of bytes containing the image data
         * @param spec the spec containing the image format
         */
        public PythonImagePortObject(final byte[] imgBytes, final ImagePortObjectSpec spec) {
            m_bytes = imgBytes;
            m_spec = new PythonImagePortObjectSpec(spec);
        }

        /**
         * @return the spec of this {@link PythonImagePortObject}
         */
        public PythonImagePortObjectSpec getSpec() {
            return m_spec;
        }

        /**
         * Check whether the provided array of bytes represents a PNG image.
         *
         * @param bytes
         * @return True if bytes represent a PNG image, False otherwise
         */
        public static boolean isPngBytes(final byte[] bytes) {
            if (bytes.length < 8) {
                return false;
            }

            byte[] pngHeader = {(byte)0x89, 'P', 'N', 'G', '\r', '\n', 0x1A, '\n'};
            for (int i = 0; i < 8; i++) {
                if (bytes[i] != pngHeader[i]) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Check whether the provided array of bytes represents an SVG image.
         *
         * @param bytes
         * @return True if bytes represent an SVG image, False otherwise
         */
        public static boolean isSvgBytes(final byte[] bytes) {
            try {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
                Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(byteArrayInputStream);
                Element rootElement = doc.getDocumentElement();
                return rootElement.getTagName().equals("svg");
            } catch (Exception e) { // NOSONAR
                // the bytes don't represent a valid XML
                return false;
            }
        }

        @Override
        public ImagePortObject getPortObject() {
            try {
                if (m_spec.isPng()) {
                    return convertBytesToPNG();
                } else if (m_spec.isSvg()) {
                    return convertBytesToSVG();
                } else {
                    throw new UnsupportedOperationException("Unsupported image format detected.");
                }
            } catch (IOException ex) { // NOSONAR
                throw new UnsupportedOperationException("Unable to convert image data.");
            }
        }

        @Override
        public String getJavaClassName() {
            return PythonImagePortObject.class.getName();
        }

        /**
         * @return an {@link ImagePortObject} corresponding to the PNG bytes
         */
        private ImagePortObject convertBytesToPNG() {
            ImageContent content = new PNGImageContent(m_bytes);
            ImagePortObjectSpec spec = new ImagePortObjectSpec(PNGImageContent.TYPE);
            return new ImagePortObject(content, spec);
        }

        /**
         * @return an {@link ImagePortObject} corresponding to the SVG bytes
         * @throws IOException if the bytes stream can't be converted to an {@link SvgImageContent}
         */
        private ImagePortObject convertBytesToSVG() throws IOException {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(m_bytes);
            ImageContent content = new SvgImageContent(inputStream);
            ImagePortObjectSpec spec = new ImagePortObjectSpec(SvgCell.TYPE);
            return new ImagePortObject(content, spec);
        }
    }

    /**
     * {@link PythonPortObject} implementation for {@link PythonCredentialPortObject}s used and populated on the Java
     * side.
     */
    public static class PythonCredentialPortObject implements PythonPortObject, PortObjectProvider {

        private final PythonCredentialPortObjectSpec m_spec;

        /**
         * Constructor for creating a PythonCredentialPortObject.
         *
         * @param credentialPortObject The CredentialPortObject to be used for initialization.
         */
        public PythonCredentialPortObject(final CredentialPortObject credentialPortObject) {
            var cpos = credentialPortObject.getSpec();
            m_spec = new PythonCredentialPortObjectSpec(cpos);
        }

        /**
         * Constructor for creating a PythonCredentialPortObject.
         *
         * @param spec the spec containing the credential format
         */
        private PythonCredentialPortObject(final CredentialPortObjectSpec spec) {
            this(new PythonCredentialPortObjectSpec(spec));
        }

        /**
         * @param spec the python spec of the credentials
         */
        protected PythonCredentialPortObject(final PythonCredentialPortObjectSpec spec) {
            m_spec = spec;
        }

        /**
         * @return the spec of this {@link PythonCredentialPortObjectSpec}
         */
        public PythonCredentialPortObjectSpec getSpec() {
            return m_spec;
        }

        /**
         * Exposes the private constructor for creating an instance with a KNIME-native Port Object Spec.
         *
         * @param spec
         * @return An instance of the Python Port Object
         */
        public static PythonCredentialPortObject createFromKnimeSpec(final CredentialPortObjectSpec spec) {
            return new PythonCredentialPortObject(spec);
        }

        @Override
        public CredentialPortObject getPortObject() {
            return new CredentialPortObject(m_spec.getPortObjectSpec());
        }

        @Override
        public String getJavaClassName() {
            return PythonCredentialPortObject.class.getName();
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

        /**
         * @return the PortObjectSpecs referenced by this PortObjectSpec
         */
        default Map<String, PythonPortObjectSpec> getReferencedSpecs() {
            return Map.of();
        }
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

        @Override
        public String toJsonString() {
            return TableSpecSerializationUtils.serializeTableSpec(m_spec);
        }

        @Override
        public String getJavaClassName() {
            return DataTableSpec.class.getName();
        }

        @Override
        public DataTableSpec getPortObjectSpec() {
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
                // TODO get referenced spec from python
                return new PythonBinaryPortObjectSpec(PythonBinaryBlobPortObjectSpec.fromJson(rootNode, Map.of()));
            } catch (JsonMappingException ex) {
                throw new IllegalStateException("Could not parse PythonBinaryPortObjectSpec from given Json data", ex);
            } catch (JsonProcessingException ex) { // NOSONAR: if we don't split this block up, Eclipse doesn't like it for some reason
                throw new IllegalStateException("Could not parse PythonBinaryPortObjectSpec from given Json data", ex);
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
        public PythonBinaryBlobPortObjectSpec getPortObjectSpec() {
            return m_spec;
        }
    }

    /**
     * The {@link PythonConnectionPortObjectSpec} specifies the contents of a Port of binary type that can be populated
     * and read on the Python side.
     *
     * @since 5.1
     */
    public static final class PythonConnectionPortObjectSpec implements PythonPortObjectSpec, PortObjectSpecProvider {
        private final PythonTransientConnectionPortObjectSpec m_spec;

        /**
         * Create a {@link PythonBinaryPortObjectSpec}
         *
         * @param spec
         */
        public PythonConnectionPortObjectSpec(final PythonTransientConnectionPortObjectSpec spec) { // NOSONAR
            m_spec = spec;
        }

        /**
         * Construct a {@link PythonBinaryPortObjectSpec} from a JSON representation
         *
         * @param jsonData The JSON serialized spec
         * @return the {@link PythonBinaryPortObjectSpec} as read from JSON
         */
        public static PythonConnectionPortObjectSpec fromJsonString(final String jsonData) {
            final var om = new ObjectMapper();
            try {
                final var rootNode = om.readTree(jsonData);
                return new PythonConnectionPortObjectSpec(PythonTransientConnectionPortObjectSpec.fromJson(rootNode));
            } catch (JsonMappingException ex) {
                throw new IllegalStateException("Could not parse PythonConnectionPortObjectSpec from given Json data",
                    ex);
            } catch (JsonProcessingException ex) { // NOSONAR: if we don't split this block up, Eclipse doesn't like it for some reason
                throw new IllegalStateException("Could not parse PythonConnectionPortObjectSpec from given Json data",
                    ex);
            }
        }

        @Override
        public String toJsonString() {
            return m_spec.toJson(JsonNodeFactory.instance).toString();
        }

        @Override
        public String getJavaClassName() {
            return PythonTransientConnectionPortObjectSpec.class.getName();
        }

        @Override
        public PythonTransientConnectionPortObjectSpec getPortObjectSpec() {
            return m_spec;
        }
    }

    /**
     *
     * @author ivan Wrapper for {@link ImagePortObjectSpec} handling serialization for the Python side.
     */
    public static final class PythonImagePortObjectSpec implements PythonPortObjectSpec, PortObjectSpecProvider {
        private final ImagePortObjectSpec m_spec;

        /**
         * @param spec a {@link ImagePortObjectSpec} that contains the data type (PNG or SVG) used during serialization
         *            to JSON.
         */
        public PythonImagePortObjectSpec(final ImagePortObjectSpec spec) {
            m_spec = spec;
        }

        @Override
        public ImagePortObjectSpec getPortObjectSpec() {
            return m_spec;
        }

        /**
         * @return true if the type of the wrapped spec is PNG.
         */
        public boolean isPng() {
            return m_spec.getDataType().equals(PNGImageContent.TYPE);
        }

        /**
         * @return true if the type of the wrapped spec is SVG.
         */
        public boolean isSvg() {
            return m_spec.getDataType().equals(SvgCell.TYPE);
        }

        @Override
        public String getJavaClassName() {
            return ImagePortObjectSpec.class.getName();
        }

        @Override
        public String toJsonString() {
            final var om = new ObjectMapper();
            final var rootNode = om.createObjectNode();
            if (isPng()) {
                rootNode.put("format", "png");
            } else if (isSvg()) {
                rootNode.put("format", "svg");
            } else {
                throw new IllegalStateException("Unsupported image format.");
            }
            try {
                return om.writeValueAsString(rootNode);
            } catch (JsonProcessingException ex) {
                throw new IllegalStateException("Could not generate JSON data for PythonImagePortObjectSpec", ex);
            }
        }

    }

    /**
     *
     * @author Carsten Haubold, KNIME GmbH, Berlin, Germany
     */
    public static class PythonCredentialPortObjectSpec implements PythonPortObjectSpec, PortObjectSpecProvider {
        private final CredentialPortObjectSpec m_spec;

        /**
         * @param spec a {@link CredentialPortObjectSpec} that contains the data type (credential type) used during
         *            serialization to JSON.
         */
        public PythonCredentialPortObjectSpec(final CredentialPortObjectSpec spec) {
            m_spec = spec;
        }

        @Override
        public CredentialPortObjectSpec getPortObjectSpec() {
            return m_spec;
        }

        /**
         * @return credential authentication scheme
         * @throws IOException When not logged in.
         */
        public String getAuthSchema() throws IOException {
            Optional<Credential> credential = m_spec.getCredential(Credential.class);
            final Credential cred = credential.orElseThrow();
            if (cred instanceof HttpAuthorizationHeaderCredentialValue val) {
                return val.getAuthScheme();
            }
            throw new IOException("Not logged in");
        }

        /**
         * @return credential authentication parameters
         * @throws IOException When not logged in.
         */
        public String getAuthParameters() throws IOException {
            Optional<Credential> credential = m_spec.getCredential(Credential.class);
            final Credential cred = credential.orElseThrow();
            if (cred instanceof HttpAuthorizationHeaderCredentialValue val) {
                return val.getAuthParameters();
            }
            throw new IOException("Not logged in");
        }

        @Override
        public String getJavaClassName() {
            return CredentialPortObjectSpec.class.getName();
        }

        @Override
        public String toJsonString() {
            final var om = new ObjectMapper();
            final var rootNode = om.createObjectNode();
            rootNode.put("data", getData());
            fill(rootNode);
            try {
                return om.writeValueAsString(rootNode);
            } catch (JsonProcessingException ex) {
                throw new IllegalStateException("Could not generate JSON data for PythonCredentialPortObjectSpec", ex);
            }
        }

        /**
         * Subclasses can implement this class to add custom content to the JSON representation.
         *
         * @param node to fill with content
         */
        protected void fill(final ObjectNode node) {
            // optional hook for subclasses
        }

        private String getData() {
            ModelContent fakeConfig = new ModelContent("fakeConfig");

            AbstractSimplePortObjectSpecSerializer.savePortObjectSpecToModelSettings(m_spec, fakeConfig);
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                fakeConfig.saveToXML(byteArrayOutputStream);
                byteArrayOutputStream.close(); // NOSONAR we have to close here
                return byteArrayOutputStream.toString(StandardCharsets.UTF_8);
            } catch (IOException ex) {
                throw new IllegalStateException("Could not save the PythonCredentialPortObjectSpec to XML.", ex);
            }
        }

        /**
         * @param jsonData the spec serialized as JSON
         * @return the corresponding CredentialPortObjectSpec
         * @throws IllegalStateException if a problem is encountered during the parsing of the JSON data
         */
        public static PythonCredentialPortObjectSpec fromJsonString(final String jsonData) {

            final var om = new ObjectMapper();
            try {
                final var rootNode = om.readTree(jsonData);
                final String serializedXMLString = rootNode.get("data").asText();

                CredentialPortObjectSpec credentialPortObjectSpec =
                    loadFromXMLCredentialPortObjectSpecString(serializedXMLString);
                return new PythonCredentialPortObjectSpec(credentialPortObjectSpec);

            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException ex) {
                throw new IllegalStateException("Could not parse PythonCredentialPortObject from given JSON data", ex);
            }
        }

        /**
         * Loads a CredentialPortObjectSpec from a serialized XML string.
         *
         * This method deserializes the provided XML string to create a CredentialPortObjectSpec.
         *
         * @param serializedXMLString The serialized XML string representing the CredentialPortObjectSpec.
         * @return The loaded CredentialPortObjectSpec.
         * @throws ClassNotFoundException If a required class is not found during deserialization.
         * @throws InstantiationException If an error occurs during object instantiation during deserialization.
         * @throws IllegalAccessException If there is illegal access during deserialization.
         * @throws IOException If an I/O error occurs during deserialization.
         */
        public static CredentialPortObjectSpec
            loadFromXMLCredentialPortObjectSpecString(final String serializedXMLString)
                throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {

            ByteArrayInputStream dataArrayInputStream =
                new ByteArrayInputStream(serializedXMLString.getBytes(StandardCharsets.UTF_8));
            ModelContentRO fakeConfig = ModelContent.loadFromXML(dataArrayInputStream);

            return AbstractSimplePortObjectSpecSerializer.loadPortObjectSpecFromModelSettings(fakeConfig);

        }

    }

    /**
     * Convert port type encoded as string to a {@link PortType}. Possible values are TABLE and BINARY, where BINARY is
     * followed by a Port Type ID as in "BINARY=org.knime.python3.nodes.test.porttype", or PortType(...) for general
     * custom port objects and ConnectionPortObject for connections, as well as IMAGE.
     *
     * @param identifier Port type identifier (TABLE, BINARY, IMAGE or CREDENTIAL currently).
     * @return {@link PortType}
     */
    @Deprecated
    public static PortType getPortTypeForIdentifier(final String identifier) {
        return PythonPortTypeRegistry.getPortTypeForIdentifier(identifier);
    }

}
