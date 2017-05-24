package org.knime.python2.typeextension.builtin.image;


import java.io.IOException;
import org.knime.core.data.image.ImageContent;
import org.knime.core.data.image.png.PNGImageContent;
import org.knime.core.data.image.png.PNGImageValue;
import org.knime.python2.typeextension.Serializer;
import org.knime.python2.typeextension.SerializerFactory;

public class PngSerializerFactory extends SerializerFactory<PNGImageValue> {

	public PngSerializerFactory() {
		super(PNGImageValue.class);
	}

	@Override
	public Serializer<? extends PNGImageValue> createSerializer() {
		return new Serializer<PNGImageValue>() {

			@Override
			public byte[] serialize(PNGImageValue value) throws IOException {
				ImageContent content = value.getImageContent();
				return ((PNGImageContent)content).getByteArray();
			}
		};
	}
}
