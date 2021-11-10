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
 *   Nov 9, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python2.kernel;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.function.Supplier;

import javax.imageio.ImageIO;

import org.apache.batik.anim.dom.SAXSVGDocumentFactory;
import org.apache.batik.transcoder.TranscoderException;
import org.apache.batik.util.XMLResourceDescriptor;
import org.knime.python2.generic.ImageContainer;
import org.w3c.dom.svg.SVGDocument;

/**
 * Contains utility methods that are needed by multiple {@link PythonKernelBackend} implementations.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PythonKernelBackendUtils {

    private PythonKernelBackendUtils() {

    }

    /**
     * Convert a string containing the XML content of a svg image to a {@link SVGDocument}.
     *
     * @param inputStreamSupplier supplies {@link InputStream} objects to read from
     * @return an {@link ImageContainer}
     * @throws IOException if the svg file cannot be created or written
     */
    public static ImageContainer createImage(final InputStreamSupplier inputStreamSupplier) throws IOException {
        var svgImage = readSvg(inputStreamSupplier);
        if (svgImage != null) {
            return svgImage;
        } else {
            return readBufferedImage(inputStreamSupplier);
        }
    }

    private static ImageContainer readSvg(final InputStreamSupplier inputStreamSupplier) throws IOException {
        try (var reader = new BufferedReader(new InputStreamReader(inputStreamSupplier.get()))) {
            final String parser = XMLResourceDescriptor.getXMLParserClassName();
            final var factory = new SAXSVGDocumentFactory(parser);
            return new ImageContainer(factory.createSVGDocument("file:/file.svg", reader));
        } catch (TranscoderException | IOException ex) {//NOSONAR
            // apparently the image is not an SVG (IOExceptions thrown for a different reason will likely also occur
            // when readBufferedImage is called)
            return null;
        }
    }

    private static ImageContainer readBufferedImage(final InputStreamSupplier inputStreamSupplier) throws IOException {
        try (var inputStream = new BufferedInputStream(inputStreamSupplier.get())) {
            return new ImageContainer(ImageIO.read(inputStream));
        }
    }

    /**
     * {@link Supplier} for {@link InputStream} that can throw an {@link IOException}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public interface InputStreamSupplier {

        /**
         * Provides a new {@link InputStream}
         * @return a new {@link InputStream}
         * @throws IOException if the {@link InputStream} can't be opened
         */
        InputStream get() throws IOException;
    }

}
