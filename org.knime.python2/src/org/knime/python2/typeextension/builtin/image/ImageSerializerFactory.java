package org.knime.python2.typeextension.builtin.image;

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
import org.knime.python2.typeextension.Serializer;
import org.knime.python2.typeextension.SerializerFactory;
import org.w3c.dom.svg.SVGDocument;

public class ImageSerializerFactory extends SerializerFactory<ImageValue> {

	public ImageSerializerFactory() {
		super(ImageValue.class);
	}

	@Override
	public Serializer<? extends ImageValue> createSerializer() {
		return new Serializer<ImageValue>() {

			@Override
			public byte[] serialize(ImageValue value) throws IOException {
				ImageContent content = value.getImageContent();
				if (content instanceof PNGImageContent) {
					return ((PNGImageContent)content).getByteArray();
				} else if (content instanceof SvgImageContent) {
					SvgImageContent svgContent = (SvgImageContent)content;
					SVGDocument svg = ((SvgCell)svgContent.toImageCell()).getDocument();
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
				return null;
			}
		};
	}
}
