package org.knime.python2.typeextension.builtin.localdate;

import java.io.IOException;
import java.time.format.DateTimeFormatter;

import org.knime.core.data.DataCell;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.data.time.localdate.LocalDateCellFactory;
import org.knime.python2.typeextension.Deserializer;
import org.knime.python2.typeextension.DeserializerFactory;

public class LocalDateDeserializerFactory extends DeserializerFactory {

	public LocalDateDeserializerFactory() {
		super(LocalDateCellFactory.TYPE);
	}

	@Override
	public Deserializer createDeserializer() {
		return new LocalDateDeserializer();
	}

	private class LocalDateDeserializer implements Deserializer {

		@Override
		public DataCell deserialize(byte[] bytes, FileStoreFactory fileStoreFactory) throws IOException {
			String string = new String(bytes, "UTF-8");
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(LocalDateSerializerFactory.FORMAT);
			return LocalDateCellFactory.create(string, formatter);
		}

	}

}
