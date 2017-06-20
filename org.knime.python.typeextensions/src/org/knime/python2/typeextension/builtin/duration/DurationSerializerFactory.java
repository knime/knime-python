package org.knime.python2.typeextension.builtin.duration;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.knime.core.data.time.duration.DurationValue;
import org.knime.core.data.time.localtime.LocalTimeValue;
import org.knime.python2.typeextension.Serializer;
import org.knime.python2.typeextension.SerializerFactory;

public class DurationSerializerFactory extends SerializerFactory<DurationValue> {
	
	public DurationSerializerFactory() {
		super(DurationValue.class);
	}
	
	@Override
	public Serializer<? extends DurationValue> createSerializer() {
		return new DurationSerializer();
	}
	
	private class DurationSerializer implements Serializer<DurationValue> {

		@Override
		public byte[] serialize(DurationValue value) throws IOException {
			Duration duration = value.getDuration();
			return duration.toString().substring(2).getBytes("UTF-8");
		}
		
	}

}
