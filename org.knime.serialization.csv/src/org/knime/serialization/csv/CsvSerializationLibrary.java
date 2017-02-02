package org.knime.serialization.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.CellImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.RowImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.TableSpecImpl;

public class CsvSerializationLibrary implements SerializationLibrary {

	@Override
	public byte[] tableToBytes(TableIterator tableIterator) {
		try {
			File file = File.createTempFile("java-to-python-", ".csv");
			file.deleteOnExit();
			FileWriter writer = new FileWriter(file);
			String names = "";
			String types = "#";
			TableSpec spec = tableIterator.getTableSpec();
			for (int i = 0; i < spec.getNumberColumns(); i++) {
				names = "," + spec.getColumnNames()[i];
				types = "," + spec.getColumnTypes()[i].getId();
			}
			writer.write(names);
			while (tableIterator.hasNext()) {
				Row row = tableIterator.next();
				String line = row.getRowKey();
				for (Cell cell : row) {
					String value = "";
					switch(cell.getColumnType()) {
					case INTEGER:
						value = cell.getIntegerValue().toString();
						break;
					case DOUBLE:
						value = cell.getDoubleValue().toString();
						break;
					case STRING:
						value = cell.getStringValue();
						break;
						// TODO more...
					default:
						break;
					}
					line += "," + value;
				}
				writer.write(line);
			}
			writer.close();
			return file.getAbsolutePath().getBytes();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void bytesIntoTable(TableCreator tableCreator, byte[] bytes) {
		try {
			File file = new File(new String(bytes));
			FileReader reader = new FileReader(file);
			BufferedReader br = new BufferedReader(reader);
			String names = br.readLine();
			String types = br.readLine();
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				Row row = new RowImpl(values[0], values.length - 1);
				for (int i = 0; i < values.length - 1; i++) {
					String columnName = tableCreator.getTableSpec().getColumnNames()[i];
					Type type = tableCreator.getTableSpec().getColumnTypes()[i];
					Cell cell;
					switch(type) {
					case INTEGER:
						cell = new CellImpl(Integer.parseInt(values[i+1]), columnName);
						break;
					case DOUBLE:
						cell = new CellImpl(Double.parseDouble(values[i+1]), columnName);
						break;
					case STRING:
						cell = new CellImpl(values[i+1], columnName);
						break;
						// TODO more...
					default:
						cell = new CellImpl(columnName);
						break;
					}
					row.setCell(cell, i);
				}
				tableCreator.addRow(row);
			}
			br.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TableSpec tableSpecFromBytes(byte[] bytes) {
		try {
			File file = new File(new String(bytes));
			FileReader reader = new FileReader(file);
			BufferedReader br = new BufferedReader(reader);
			String[] nameValues = br.readLine().split(",");
			String[] typeValues = br.readLine().split(",");
			String[] names = new String[nameValues.length];
			Type[] types = new Type[names.length];
			for (int i = 0; i < names.length; i++) {
				names[i] = nameValues[i];
				types[i] = Type.getTypeForId(Integer.parseInt(typeValues[i]));
			}
			TableSpec spec = new TableSpecImpl(types, names);
			br.close();
			return spec;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
