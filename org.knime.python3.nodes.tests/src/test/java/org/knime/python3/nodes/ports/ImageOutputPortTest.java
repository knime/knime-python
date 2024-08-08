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
 *   May 24, 2023 (Ivan Prigarin, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.ports;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.knime.base.data.xml.SvgCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.image.png.PNGImageContent;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.port.image.ImagePortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PurePythonImagePortObject;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonImagePortObject;
import org.knime.python3.nodes.ports.converters.PortObjectConversionContext;
import org.knime.python3.nodes.ports.converters.PortObjectConverters;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


/**
 * Contains unit tests for {@link PurePythonImagePortObject} handling image bytes from the Python side.
 *
 * @author Ivan Prigarin, KNIME GmbH, Konstanz, Germany
 */
@RunWith(MockitoJUnitRunner.class)
public class ImageOutputPortTest {

    static final byte[] SVG_BYTES = "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"50\" height=\"50\"></svg>".getBytes();
    static final byte[] PNG_BYTES = {(byte)0x89, 'P', 'N', 'G', '\r', '\n', 0x1A, '\n'};
    static final byte[] UNSUPPORTED_BYTES = "unsupported".getBytes();
    static final String PATH_TO_PNG_FILE = "src/test/resources/test_png.png";

    @Mock
    private PurePythonImagePortObject purePythonImagePortObject;


    /**
     * Test that PNG bytes from Python are correctly detected.
     */
    @Test
    public void testFromPurePythonPng() {
        byte[] pngBytes = getPngBytes();
        when(purePythonImagePortObject.getImageBytes()).thenReturn(toBase64String(pngBytes));

        var converter = new PortObjectConverters.ImagePortObjectConverter();
        var conversionContext = new PortObjectConversionContext(null, null, null);

        ImagePortObject result = converter.fromPython(purePythonImagePortObject, conversionContext);

        final ImagePortObjectSpec imgSpec = result.getSpec();

        assertEquals(PNGImageContent.TYPE, imgSpec.getDataType()); // NOSONAR
    }

    /**
     * Test that SVG bytes from Python are correctly detected.
     */
    @Test
    public void testFromPurePythonSvg() {
        when(purePythonImagePortObject.getImageBytes()).thenReturn(toBase64String(SVG_BYTES));

        var converter = new PortObjectConverters.ImagePortObjectConverter();
        var conversionContext = new PortObjectConversionContext(null, null, null);

        ImagePortObject result = converter.fromPython(purePythonImagePortObject, conversionContext);

        final ImagePortObjectSpec imgSpec = result.getSpec();

        assertEquals(SvgCell.TYPE, imgSpec.getDataType()); // NOSONAR
    }

    /**
     * Test that non-PNG and non-SVG bytes throw an exception.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testFromPurePythonUnsupported() {
        when(purePythonImagePortObject.getImageBytes()).thenReturn(toBase64String(UNSUPPORTED_BYTES));

        var converter = new PortObjectConverters.ImagePortObjectConverter();
        var conversionContext = new PortObjectConversionContext(null, null, null);

        converter.fromPython(purePythonImagePortObject, conversionContext);
    }


    /**
     * Test getting the appropriate ImagePortObject for PNG bytes.
     */
    @Test
    public void testGetPortObjectPng() {
        byte[] pngBytes = getPngBytes();

        ImagePortObjectSpec spec = new ImagePortObjectSpec(PNGImageContent.TYPE);
        PythonImagePortObject pythonImagePortObject = new PythonImagePortObject(pngBytes, spec);

        ImagePortObject result = pythonImagePortObject.getPortObject();

        assertEquals(PNGImageContent.TYPE, result.getSpec().getDataType()); // NOSONAR
    }

    /**
     * Test getting the appropriate ImagePortObject for SVG bytes.
     */
    @Test
    public void testGetPortObjectSvg() {
        ImagePortObjectSpec spec = new ImagePortObjectSpec(SvgCell.TYPE);
        PythonImagePortObject pythonImagePortObject = new PythonImagePortObject(SVG_BYTES, spec);

        ImagePortObject result = pythonImagePortObject.getPortObject();

        assertEquals(SvgCell.TYPE, result.getSpec().getDataType()); // NOSONAR
    }

    /**
     * Test that getting an ImagePortObject for an unsupported file type throws an exception.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testGetPortObjectUnsupported() {
        ImagePortObjectSpec spec = new ImagePortObjectSpec(StringCell.TYPE);
        PythonImagePortObject pythonImagePortObject = new PythonImagePortObject(UNSUPPORTED_BYTES, spec);

        pythonImagePortObject.getPortObject();
    }

    /**
     * Helper utility to encode the provided byte array to Base64.
     */
    private static String toBase64String(final byte[] input) {
        return Base64.getEncoder().encodeToString(input);
    }

    /**
     * Read the locally stored 1x1 pixel PNG image as bytes.
     */
    private static byte[] getPngBytes() {
        try {
            return Files.readAllBytes(Paths.get(PATH_TO_PNG_FILE));
        } catch (IOException e) {
            return PNG_BYTES;
        }
    }

}
