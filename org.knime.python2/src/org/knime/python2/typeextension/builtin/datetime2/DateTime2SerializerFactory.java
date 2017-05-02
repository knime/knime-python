package org.knime.python2.typeextension.builtin.datetime2;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.knime.core.data.time.localdatetime.LocalDateTimeValue;
import org.knime.python2.typeextension.Serializer;
import org.knime.python2.typeextension.SerializerFactory;

public class DateTime2SerializerFactory extends SerializerFactory<LocalDateTimeValue> {
	
	static final String FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	
	public DateTime2SerializerFactory() {
		super(LocalDateTimeValue.class);
	}
	
	@Override
	public Serializer<? extends LocalDateTimeValue> createSerializer() {
		return new DateAndTimeSerializer();
	}
	
	private class DateAndTimeSerializer implements Serializer<LocalDateTimeValue> {

		@Override
		public byte[] serialize(LocalDateTimeValue value) throws IOException {
			LocalDateTime date = value.getLocalDateTime();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FORMAT);
			return date.format(formatter).getBytes("UTF-8");
		}
		
	}

}
