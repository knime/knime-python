/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 */

package org.knime.python.typeextension.builtin.image;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.batik.transcoder.TranscoderException;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;
import org.apache.batik.transcoder.image.PNGTranscoder;
import org.knime.base.data.xml.SvgCell;
import org.knime.base.data.xml.SvgImageContent;
import org.knime.core.data.image.ImageContent;
import org.knime.core.data.image.ImageValue;
import org.knime.core.data.image.png.PNGImageContent;
import org.knime.core.data.util.LockedSupplier;
import org.knime.python.typeextension.Serializer;
import org.knime.python.typeextension.SerializerFactory;
import org.w3c.dom.svg.SVGDocument;

/**
 * Serialize {@link PNGImageContent} from KNIME to python.
 * 
 * @author Patrick Winter, Universität Konstanz, Konstanz, Germany
 */

public class ImageSerializerFactory extends SerializerFactory<ImageValue> {

    public ImageSerializerFactory() {
        super(ImageValue.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Serializer<? extends ImageValue> createSerializer() {
        return new Serializer<ImageValue>() {

            @Override
            public byte[] serialize(ImageValue value) throws IOException {
                ImageContent content = value.getImageContent();
                if (content instanceof PNGImageContent) {
                    return ((PNGImageContent) content).getByteArray();
                } else if (content instanceof SvgImageContent) {
                    SvgImageContent svgContent = (SvgImageContent) content;

                    try (LockedSupplier<SVGDocument> supplier = ((SvgCell) svgContent.toImageCell())
                            .getDocumentSupplier()) {
                        SVGDocument svg = supplier.get();
                        TranscoderInput input = new TranscoderInput(svg);
                        ByteArrayOutputStream ostream = new ByteArrayOutputStream();
                        TranscoderOutput output = new TranscoderOutput(ostream);
                        PNGTranscoder converter = new PNGTranscoder();
                        try {
                            converter.transcode(input, output);
                            return ostream.toByteArray();
                        } catch (TranscoderException e) {
                            throw new IOException(e);
                        }
                    }
                }
                return null;
            }
        };
    }
}
