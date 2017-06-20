package org.knime.python2.typeextension.builtin.localtime;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.knime.core.data.time.localtime.LocalTimeValue;
import org.knime.python2.typeextension.Serializer;
import org.knime.python2.typeextension.SerializerFactory;

/**
 * Is used to serialitze java8 LocalTime objects to python time objects.
 * 
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 */

public class LocalTimeSerializerFactory extends SerializerFactory<LocalTimeValue> {
	
	static final String FORMAT = "HH:mm:ss.SSS";
	
	public LocalTimeSerializerFactory() {
		super(LocalTimeValue.class);
	}
	
	@Override
	public Serializer<? extends LocalTimeValue> createSerializer() {
		return new LocalTimeSerializer();
	}
	
	private class LocalTimeSerializer implements Serializer<LocalTimeValue> {

		@Override
		public byte[] serialize(LocalTimeValue value) throws IOException {
			LocalTime time = value.getLocalTime();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FORMAT);
			return time.format(formatter).getBytes("UTF-8");
		}
		
	}

}
