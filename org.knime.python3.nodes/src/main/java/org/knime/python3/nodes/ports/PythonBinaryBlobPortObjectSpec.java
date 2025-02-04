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
 *   4 May 2022 (chaubold): created
 */
package org.knime.python3.nodes.ports;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.swing.JComponent;

import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.core.node.port.AbstractSimplePortObjectSpec;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;
import org.knime.core.node.port.PortTypeRegistry;
import org.knime.core.node.port.PortUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.util.RawValue;

/**
 * Specification for the {@link PythonBinaryBlobPortObjectSpec}.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class PythonBinaryBlobPortObjectSpec extends AbstractSimplePortObjectSpec {
    /**
     * The serializer for the PickeledObject portspec type
     */
    public static final class Serializer
        extends AbstractSimplePortObjectSpecSerializer<PythonBinaryBlobPortObjectSpec> {

        private final Function<String, Optional<PortObjectSpecSerializer<?>>> m_specClassNameToSerializer;

        private static Optional<PortObjectSpecSerializer<?>>
            getSerializerFromPortTypeRegistry(final String specClassName) {
            var portTypeRegistry = PortTypeRegistry.getInstance();
            return portTypeRegistry.getSpecClass(specClassName).flatMap(portTypeRegistry::getSpecSerializer);
        }

        /**
         * Constructor for production.
         */
        public Serializer() {
            this(Serializer::getSerializerFromPortTypeRegistry);
        }

        /**
         * Constructor for testing purposes.
         *
         * @param specClassNameToSerializer provider for serializers
         */
        Serializer(
            final Function<String, Optional<PortObjectSpecSerializer<?>>> specClassNameToSerializer) {
            m_specClassNameToSerializer = specClassNameToSerializer;
        }

        private static final String REFERENCED_SPEC_CLASSES = "referencedSpecClasses.xml";

        @Override
        public void savePortObjectSpec(final PythonBinaryBlobPortObjectSpec spec,
            final PortObjectSpecZipOutputStream out) throws IOException {
            saveMainSpec(spec, out);
            var referencedSpecs = spec.getReferencedSpecs();
            if (referencedSpecs.isEmpty()) {
                // no references to save
                return;
            }
            saveReferencedSpecs(out, referencedSpecs);
        }

        /**
         * Similar to AbstractSimplePortObjectSpecSerializer#savePortObjectSpec but keeps the stream open
         *
         * @param spec to serialize
         * @param out stream to write to
         * @throws IOException if writing fails for some reason
         */
        private static void saveMainSpec(final PythonBinaryBlobPortObjectSpec spec,
            final PortObjectSpecZipOutputStream out)
            throws IOException {
            var model = new ModelContent("model.xml");
            savePortObjectSpecToModelSettings(spec, model);
            out.putNextEntry(new ZipEntry("content.xml"));
            // keep stream open for subsequent reference specs
            try (var nonCloseable = entryClosing(out)) {
                model.saveToXML(nonCloseable);
            }
        }

        private void saveReferencedSpecs(final PortObjectSpecZipOutputStream out,
            final Map<UUID, PortObjectSpec> referencedSpecs) throws IOException {
            var referencedSpecClassesModel = new ModelContent(REFERENCED_SPEC_CLASSES);
            for (var refSpecEntry : referencedSpecs.entrySet()) {
                referencedSpecClassesModel.addString(refSpecEntry.getKey().toString(),
                    refSpecEntry.getValue().getClass().getName());
            }
            out.putNextEntry(new ZipEntry(REFERENCED_SPEC_CLASSES));
            try (var nonCloseable = new NonClosableOutputStream.Zip(out)) {
                referencedSpecClassesModel.saveToXML(nonCloseable);
            }

            for (var refSpecEntry : referencedSpecs.entrySet()) {
                out.putNextEntry(new ZipEntry(refSpecEntry.getKey().toString()));
                writeRefSpecWithoutClosing(refSpecEntry.getValue(), out);
            }
        }

        private void writeRefSpecWithoutClosing(final PortObjectSpec spec, final PortObjectSpecZipOutputStream out)
            throws IOException {
            try (var entryClosing = entryClosing(out);
                    var pos = PortUtil.getPortObjectSpecZipOutputStream(entryClosing)) {
                var specClassName = spec.getClass().getName();
                var serializer = m_specClassNameToSerializer.apply(specClassName)
                    .orElseThrow(ioEx("No serializer for %s".formatted(specClassName)));
                unsafeSave(serializer, spec, pos);
            }
        }

        private static OutputStream entryClosing(final ZipOutputStream outputStream) {
            return new NonClosableOutputStream.Zip(outputStream);
        }

        private static InputStream entryClosing(final ZipInputStream inputStream) {
            return new NonClosableInputStream.Zip(inputStream);
        }

        @SuppressWarnings("unchecked") // when saving, we don't know T
        private static <T extends PortObjectSpec> void unsafeSave(final PortObjectSpecSerializer<T> serializer,
            final PortObjectSpec spec, final PortObjectSpecZipOutputStream out) throws IOException {
            serializer.savePortObjectSpec((T)spec, out);
        }


        @Override
        public PythonBinaryBlobPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in)
            throws IOException {
            var spec = super.loadPortObjectSpec(in);
            var refSpecs = loadRefSpecs(in);
            spec.m_referencedSpecs = refSpecs;
            return spec;
        }


        /**
         * Loads the referenced specs from the input stream. Package private for testing purposes.
         */
        private Map<UUID, PortObjectSpec> loadRefSpecs(final PortObjectSpecZipInputStream in) throws IOException {

            var entry = in.getNextEntry();
            if (entry == null) {
                // no references to load
                return Map.of();
            }
            if (!REFERENCED_SPEC_CLASSES.equals(entry.getName())) {
                throw new IOException("Expected zip entry references, got " + entry.getName());
            }

            ModelContentRO referencedSpecClassesModel = loadModelContentWithoutClosing(in);

            var refSpecs = new HashMap<UUID, PortObjectSpec>();

            for (int i = 0; i < referencedSpecClassesModel.getChildCount(); i++) {
                var specEntry = in.getNextEntry();
                var id = specEntry.getName();
                var specClassName = getString(referencedSpecClassesModel, id);
                var serializer = m_specClassNameToSerializer.apply(specClassName)
                    .orElseThrow(ioEx("No serializer registered for %s.".formatted(specClassName)));
                // TODO NonCloseable?
                var refSpec = loadSpecFromNestedZip(serializer, in);
                refSpecs.put(UUID.fromString(id), refSpec);
            }
            return refSpecs;
        }

        private static PortObjectSpec loadSpecFromNestedZip(final PortObjectSpecSerializer<?> serializer,
            final PortObjectSpecZipInputStream in) throws IOException {
            try (var innerZip = PortUtil.getPortObjectSpecZipInputStream(entryClosing(in))) {
                return serializer.loadPortObjectSpec(innerZip);
            }
        }

        private static ModelContentRO loadModelContentWithoutClosing(final PortObjectSpecZipInputStream in)
            throws IOException {
            try (var nonCloseable = entryClosing(in)) {
                return ModelContent.loadFromXML(nonCloseable);
            }
        }

        private static String getString(final ModelContentRO model, final String key) throws IOException {
            try {
                return model.getString(key);
            } catch (InvalidSettingsException ex) {
                throw new IOException(ex);
            }
        }

        private static Supplier<IOException> ioEx(final String reason) {
            return () -> new IOException(reason);
        }
    }

    /**
     * The unique identifier of this port object type (defined in Python)
     *
     * Effectively final.
     */
    protected String m_id;

    /**
     * The JSON data of this port object spec encoded as string
     *
     * Effectively final.
     */
    protected String m_data;

    /**
     * Other specs referenced by this spec.
     *
     * Effectively final.
     */
    protected Map<UUID, PortObjectSpec> m_referencedSpecs;

    /**
     * Deserialization constructor. Fields will be populated in load()
     */
    public PythonBinaryBlobPortObjectSpec() {
    }

    /**
     * Constructor.
     *
     * @param id An ID describing the type of data inside the binary blob
     */
    PythonBinaryBlobPortObjectSpec(final String id, final String data,
        final Map<UUID, PortObjectSpec> referencedSpecs) {
        m_id = id;
        m_data = data;
        m_referencedSpecs = referencedSpecs == null ? Map.of() : referencedSpecs;
    }

    String getId() {
        return m_id;
    }

    /**
     * @param factory The factory to use when creating JSON nodes
     * @return A JSON representation of this {@link PythonBinaryBlobPortObjectSpec}
     */
    public JsonNode toJson(final JsonNodeFactory factory) {
        final var node = factory.objectNode();
        node.put("id", m_id);
        if (m_data != null) {
            node.putRawValue("data", new RawValue(m_data));
        }
        return node;
    }

    /**
     * @return the specs referenced by this {@link PortObjectSpec}
     */
    public Map<UUID, PortObjectSpec> getReferencedSpecs() {
        return m_referencedSpecs;
    }

    /**
     * Construct a {@link PythonBinaryBlobPortObjectSpec} from its JSON representation
     *
     * @param data the JSON data
     * @param referencedSpecs other specs referenced by the spec
     * @return a new {@link PythonBinaryBlobPortObjectSpec} object
     */
    // TODO rename or introduce alternative method for bw compatibility?
    public static PythonBinaryBlobPortObjectSpec fromJson(final JsonNode data,
        final Map<UUID, PortObjectSpec> referencedSpecs) {
        var specData = data.get("data");
        return new PythonBinaryBlobPortObjectSpec(data.get("id").asText(),
            specData == null ? null : specData.toString(), referencedSpecs);
    }

    @Override
    protected void save(final ModelContentWO model) {
        model.addString("id", m_id);
        model.addString("data", m_data);
    }

    @Override
    protected void load(final ModelContentRO model) throws InvalidSettingsException {
        m_id = model.getString("id", null);
        m_data = model.getString("data", null);
    }

    @Override
    public boolean equals(final Object ospec) {
        if (this == ospec) {
            return true;
        }
        if (ospec == null || ospec.getClass() != this.getClass()) {
            return false;
        }
        final PythonBinaryBlobPortObjectSpec spec = (PythonBinaryBlobPortObjectSpec)ospec;
        return Objects.equals(m_id, spec.m_id) && Objects.equals(m_data, spec.m_data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_id, m_data);
    }

    @Override
    public JComponent[] getViews() {
        String text;
        if (m_id != null) {
            text = "<html><b>" + m_id + "</b></html>";
        } else {
            text = "No object available";
        }
        return PortObjectSpecUtils.stringViewForSpec(text);
    }

}