package org.knime.python2.typeextension.builtin.localtime;

import java.io.IOException;
import java.time.format.DateTimeFormatter;

import org.knime.core.data.DataCell;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.data.time.localtime.LocalTimeCellFactory;
import org.knime.python2.typeextension.Deserializer;
import org.knime.python2.typeextension.DeserializerFactory;

/**
 * Is used to deserialize python time objects to java8 LocalTime objects.
 * 
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 */

public class LocalTimeDeserializerFactory extends DeserializerFactory {

	public LocalTimeDeserializerFactory() {
		super(LocalTimeCellFactory.TYPE);
	}

	@Override
	public Deserializer createDeserializer() {
		return new LocalTimeDeserializer();
	}

	private class LocalTimeDeserializer implements Deserializer {

		@Override
		public DataCell deserialize(byte[] bytes, FileStoreFactory fileStoreFactory) throws IOException {
			String string = new String(bytes, "UTF-8");
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(LocalTimeSerializerFactory.FORMAT);
			return LocalTimeCellFactory.create(string, formatter);
		}

	}

}