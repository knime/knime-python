package org.knime.python2.typeextension.builtin.zoneddatetime;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.knime.core.data.time.zoneddatetime.ZonedDateTimeValue;
import org.knime.python2.typeextension.Serializer;
import org.knime.python2.typeextension.SerializerFactory;

public class ZonedDateTimeSerializerFactory extends SerializerFactory<ZonedDateTimeValue> {
	
	public static final String FORMAT = "yyyy-MM-dd HH:mm:ss.SSSxxx'['z']'";
	
	public ZonedDateTimeSerializerFactory() {
		super(ZonedDateTimeValue.class);
	}
	
	@Override
	public Serializer<? extends ZonedDateTimeValue> createSerializer() {
		return new ZonedDateTimeSerializer();
	}
	
	private class ZonedDateTimeSerializer implements Serializer<ZonedDateTimeValue> {

		@Override
		public byte[] serialize(ZonedDateTimeValue value) throws IOException {
			ZonedDateTime date = value.getZonedDateTime();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FORMAT);
			return date.format(formatter).getBytes("UTF-8");
		}
		
	}

}