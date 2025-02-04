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
 *   Feb 3, 2025 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.ports;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.swing.JComponent;

import org.junit.Before;
import org.junit.Test;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpec.PortObjectSpecSerializer;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;
import org.knime.core.node.port.PortUtil;
import org.knime.python3.nodes.ports.PythonBinaryBlobPortObjectSpec.Serializer;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class TestPythonBinaryBlobPortObjectSpec {

    private Serializer m_serializer;

    private PythonBinaryBlobPortObjectSpec m_specWithoutRefs;

    private PythonBinaryBlobPortObjectSpec m_specWithRefs;

    private DummyReferencedSpec m_referencedSpec;

    private DummyReferencedSpecSerializer m_referencedSpecSerializer;

    private DataTableSpec m_tableSpec;

    /**
     * Sets up the text fixtures.
     */
    @Before
    public void setup() {
        m_referencedSpec = new DummyReferencedSpec("foo");
        m_referencedSpecSerializer = new DummyReferencedSpecSerializer();
        var serializerMap = new HashMap<String, PortObjectSpecSerializer<?>>();
        Function<String, Optional<PortObjectSpecSerializer<?>>> mapping = Function.<String> identity()//
            .andThen(serializerMap::get)//
            .andThen(Optional::ofNullable);
        m_serializer = new Serializer(mapping);

        // the map needs to include m_serializer, so it has to be filled after we have the instance
        serializerMap.put(DummyReferencedSpec.class.getName(), m_referencedSpecSerializer);
        serializerMap.put(DataTableSpec.class.getName(), new DataTableSpec.Serializer());
        serializerMap.put(PythonBinaryBlobPortObjectSpec.class.getName(), m_serializer);

        m_specWithoutRefs = new PythonBinaryBlobPortObjectSpec("noRefs", "test", Map.of());

        m_tableSpec = new DataTableSpec(new DataColumnSpecCreator("bar", StringCell.TYPE).createSpec());
        m_specWithRefs = new PythonBinaryBlobPortObjectSpec("withRefs", "test",
            Map.of(UUID.randomUUID(), m_tableSpec, UUID.randomUUID(), m_referencedSpec));
    }

    /**
     * Test that when the port object has no referenced specs the extra reference entry is not written.
     *
     * @throws IOException not thrown
     */
    @Test
    public void testSavePortObjectSpecNoReferences() throws IOException {
        var bais = new ByteArrayInputStream(saveToByteArray(m_specWithoutRefs));
        try (var zipIn = new ZipInputStream(bais)) {
            ZipEntry entry;
            boolean foundReferencedEntry = false;
            while ((entry = zipIn.getNextEntry()) != null) {
                if ("referencedSpecClasses.xml".equals(entry.getName())) {
                    foundReferencedEntry = true;
                }
                zipIn.closeEntry();
                assertFalse("No referenced specs should be saved when the map is empty.", foundReferencedEntry);
            }
        }

    }

    /**
     * Test that when one referenced spec is present the serializer writes both the XML mapping and the referenced spec
     * entry.
     *
     * @throws IOException not thrown
     */
    @Test
    public void testSaveReferencedSpecsWithReference() throws IOException {

        // Read back the zip entries.
        var bais = new ByteArrayInputStream(saveToByteArray(m_specWithRefs));
        try (var zipIn = new ZipInputStream(bais)) {
            boolean foundMappingEntry = false;
            ZipEntry entry;
            Set<String> expectedRefEntryNames = m_specWithRefs.getReferencedSpecs().keySet().stream()//
                .map(Object::toString)//
                .collect(Collectors.toCollection(HashSet::new));
            while ((entry = zipIn.getNextEntry()) != null) {
                if ("referencedSpecClasses.xml".equals(entry.getName())) {
                    foundMappingEntry = true;
                }
                expectedRefEntryNames.remove(entry.getName());
                zipIn.closeEntry();
            }

            assertTrue("The XML mapping entry should be present.", foundMappingEntry);
            assertTrue("The referenced specs entries should be present.", expectedRefEntryNames.isEmpty());
        }
    }

    /**
     * Test that when no reference entry exists (i.e. getNextEntry returns null after the main spec) the
     * loadPortObjectSpec method leaves the referenced specs map empty.
     *
     * @throws IOException not thrown
     */
    @Test
    public void testLoadPortObjectSpecNoReferences() throws IOException {
        var spec = saveAndLoad(m_specWithoutRefs);
        assertTrue("Referenced specs should be empty when none were saved.", spec.getReferencedSpecs().isEmpty());
    }

    /**
     * Test that if the zip entry following the main spec is not named "referencedSpecClasses.xml" an IOException is
     * thrown.
     *
     * @throws IOException caught by the test
     */
    @Test
    public void testLoadPortObjectSpecWrongReferenceEntryName() throws IOException {
        // Create a zip stream with a main spec and then an entry with a wrong name.
        var baos = new ByteArrayOutputStream();
        try (var zos = PortUtil.getPortObjectSpecZipOutputStream(baos)) {
            // Main spec entry.
            m_serializer.savePortObjectSpec(m_specWithoutRefs, zos);
            // Wrong entry instead of "referencedSpecClasses.xml"
            var wrongEntry = new ZipEntry("wrongName.xml");
            zos.putNextEntry(wrongEntry);
            zos.write("dummy".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }

        var bais = new ByteArrayInputStream(baos.toByteArray());
        try (var zipIn = PortUtil.getPortObjectSpecZipInputStream(bais)) {
            IOException thrown = assertThrows(IOException.class, () -> {
                m_serializer.loadPortObjectSpec(zipIn);
            });
            assertTrue("An exception should be thrown for an incorrect reference entry name.",
                thrown.getMessage().contains("Expected zip entry references"));
        }
    }

    /**
     * Test that loadRefSpecs correctly reads the XML mapping and then calls the provided serializer to load the
     * referenced spec.
     *
     * @throws IOException
     */
    @Test
    public void testLoadRefSpecs() throws IOException {
        var spec = saveAndLoad(m_specWithRefs);
        assertEquals("The loaded references should be equal to the saved references.",
            m_specWithRefs.getReferencedSpecs(), spec.getReferencedSpecs());
    }

    /**
     * Test that saving and loading nested specs with references works.
     *
     * @throws IOException
     */
    @Test
    public void testNestedRefSpecs() throws IOException {
        var saved =
            new PythonBinaryBlobPortObjectSpec("outer", "outer data", Map.of(UUID.randomUUID(), m_specWithRefs));

        var loaded = saveAndLoad(saved);

        assertEquals("The loaded spec should be equal to the saved spec.", saved, loaded);

    }

    private PythonBinaryBlobPortObjectSpec saveAndLoad(final PythonBinaryBlobPortObjectSpec spec) throws IOException {
        byte[] byteArray = saveToByteArray(spec);
        var bais = new ByteArrayInputStream(byteArray);
        try (var zipIn = PortUtil.getPortObjectSpecZipInputStream(bais)) {
            return m_serializer.loadPortObjectSpec(zipIn);
        }
    }

    private byte[] saveToByteArray(final PythonBinaryBlobPortObjectSpec spec) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var zos = PortUtil.getPortObjectSpecZipOutputStream(baos)) {
            m_serializer.savePortObjectSpec(spec, zos);
        }
        return baos.toByteArray();
    }

    /** A dummy referenced port object spec. */
    class DummyReferencedSpec implements PortObjectSpec {

        private String m_value;

        DummyReferencedSpec(final String value) {
            m_value = value;
        }

        @Override
        public JComponent[] getViews() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int hashCode() {
            return m_value.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof DummyReferencedSpec other) {
                return m_value.equals(other.m_value);
            }
            return false;
        }

    }

    /**
     * A dummy serializer for DummyReferencedSpec. It writes a fixed marker string and reads it back.
     */
    class DummyReferencedSpecSerializer extends PortObjectSpecSerializer<PortObjectSpec> {
        @Override
        public void savePortObjectSpec(final PortObjectSpec spec, final PortObjectSpecZipOutputStream out)
            throws IOException {
            out.write(((DummyReferencedSpec)spec).m_value.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public DummyReferencedSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in) throws IOException {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[16];
            int bytesRead;

            // Read all bytes from the stream
            while ((bytesRead = in.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            in.closeEntry();
            return new DummyReferencedSpec(outputStream.toString(StandardCharsets.UTF_8));
        }
    }
}
