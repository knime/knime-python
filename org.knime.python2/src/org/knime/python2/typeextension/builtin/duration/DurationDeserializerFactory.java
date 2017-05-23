package org.knime.python2.typeextension.builtin.duration;

import java.io.IOException;
import java.time.format.DateTimeFormatter;

import org.knime.core.data.DataCell;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.data.time.duration.DurationCellFactory;
import org.knime.core.data.time.localtime.LocalTimeCellFactory;
import org.knime.python2.typeextension.Deserializer;
import org.knime.python2.typeextension.DeserializerFactory;
import org.knime.python2.typeextension.builtin.localtime.LocalTimeSerializerFactory;

public class DurationDeserializerFactory extends DeserializerFactory {

	public DurationDeserializerFactory() {
		super(DurationCellFactory.TYPE);
	}

	@Override
	public Deserializer createDeserializer() {
		return new DurationDeserializer();
	}

	private class DurationDeserializer implements Deserializer {

		@Override
		public DataCell deserialize(byte[] bytes, FileStoreFactory fileStoreFactory) throws IOException {
			String string = new String(bytes, "UTF-8");
			return DurationCellFactory.create(string);
		}

	}

}