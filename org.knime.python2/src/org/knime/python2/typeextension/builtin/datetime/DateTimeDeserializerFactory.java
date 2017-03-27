package org.knime.python2.typeextension.builtin.datetime;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.knime.core.data.DataCell;
import org.knime.core.data.date.DateAndTimeCell;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.python2.typeextension.Deserializer;
import org.knime.python2.typeextension.DeserializerFactory;

public class DateTimeDeserializerFactory extends DeserializerFactory {

	public DateTimeDeserializerFactory() {
		super(DateAndTimeCell.TYPE);
	}

	@Override
	public Deserializer createDeserializer() {
		return new DateTimeDeserializer();
	}

	private class DateTimeDeserializer implements Deserializer {

		@Override
		public DataCell deserialize(byte[] bytes, FileStoreFactory fileStoreFactory) throws IOException {
			try {
				String string = new String(bytes, "UTF-8");
				SimpleDateFormat sdf = new SimpleDateFormat(DateTimeSerializerFactory.FORMAT);
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(sdf.parse(string));
				return new DateAndTimeCell(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH),
						calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY),
						calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND), calendar.get(Calendar.MILLISECOND));
			} catch (ParseException e) {
				throw new IOException(e.getMessage(), e);
			}
		}

	}

}
