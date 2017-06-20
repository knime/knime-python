package org.knime.python.typeextension.builtin.image;

import java.io.IOException;

import org.knime.core.data.DataCell;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.data.image.png.PNGImageContent;
import org.knime.python.typeextension.Deserializer;
import org.knime.python.typeextension.DeserializerFactory;

public class ImageDeserializerFactory extends DeserializerFactory {

	public ImageDeserializerFactory() {
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
