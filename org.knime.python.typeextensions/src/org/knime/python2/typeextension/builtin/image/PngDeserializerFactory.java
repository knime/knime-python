package org.knime.python2.typeextension.builtin.image;

import java.io.IOException;

import org.knime.core.data.DataCell;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.data.image.png.PNGImageContent;
import org.knime.python2.typeextension.Deserializer;
import org.knime.python2.typeextension.DeserializerFactory;

public class PngDeserializerFactory extends DeserializerFactory {

	public PngDeserializerFactory() {
		super(PNGImageContent.TYPE);
	}

	@Override
	public Deserializer createDeserializer() {

		return new Deserializer() {

			@Override
			public DataCell deserialize(byte[] bytes,
					FileStoreFactory fileStoreFactory) throws IOException {

				return new PNGImageContent(bytes).toImageCell();
			}

		};
	}
}
