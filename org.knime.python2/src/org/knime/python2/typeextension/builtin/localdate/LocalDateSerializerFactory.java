package org.knime.python2.typeextension.builtin.localdate;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.knime.core.data.time.localdate.LocalDateValue;
import org.knime.python2.typeextension.Serializer;
import org.knime.python2.typeextension.SerializerFactory;

public class LocalDateSerializerFactory extends SerializerFactory<LocalDateValue> {
	
	static final String FORMAT = "yyyy-MM-dd";
	
	public LocalDateSerializerFactory() {
		super(LocalDateValue.class);
	}
	
	@Override
	public Serializer<? extends LocalDateValue> createSerializer() {
		return new LocalDateSerializer();
	}
	
	private class LocalDateSerializer implements Serializer<LocalDateValue> {

		@Override
		public byte[] serialize(LocalDateValue value) throws IOException {
			LocalDate date = value.getLocalDate();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FORMAT);
			return date.format(formatter).getBytes("UTF-8");
		}
		
	}

}
