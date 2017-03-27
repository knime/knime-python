package org.knime.python2.typeextension.builtin.datetime;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.knime.core.data.date.DateAndTimeValue;
import org.knime.python2.typeextension.Serializer;
import org.knime.python2.typeextension.SerializerFactory;

public class DateTimeSerializerFactory extends SerializerFactory<DateAndTimeValue> {
	
	static final String FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	
	public DateTimeSerializerFactory() {
		super(DateAndTimeValue.class);
	}
	
	@Override
	public Serializer<? extends DateAndTimeValue> createSerializer() {
		return new DateAndTimeSerializer();
	}
	
	private class DateAndTimeSerializer implements Serializer<DateAndTimeValue> {

		@Override
		public byte[] serialize(DateAndTimeValue value) throws IOException {
			Date date = value.getUTCCalendarClone().getTime();
			SimpleDateFormat sdf = new SimpleDateFormat(FORMAT);
			return sdf.format(date).getBytes("UTF-8");
		}
		
	}

}
