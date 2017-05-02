package org.knime.python2.typeextension.builtin.datetime2;

import java.io.IOException;
import java.time.format.DateTimeFormatter;

import org.knime.core.data.DataCell;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.data.time.localdatetime.LocalDateTimeCellFactory;
import org.knime.python2.typeextension.Deserializer;
import org.knime.python2.typeextension.DeserializerFactory;

public class DateTime2DeserializerFactory extends DeserializerFactory {

	public DateTime2DeserializerFactory() {
		super(LocalDateTimeCellFactory.TYPE);
	}

	@Override
	public Deserializer createDeserializer() {
		return new DateTimeDeserializer();
	}

	private class DateTimeDeserializer implements Deserializer {

		@Override
		public DataCell deserialize(byte[] bytes, FileStoreFactory fileStoreFactory) throws IOException {
			String string = new String(bytes, "UTF-8");
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DateTime2SerializerFactory.FORMAT);
			return LocalDateTimeCellFactory.create(string, formatter);
		}

	}

}
