package org.knime.serialization.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
			String types = "#";
			String names = "";
			TableSpec spec = tableIterator.getTableSpec();
			for (int i = 0; i < spec.getNumberColumns(); i++) {
				types += "," + spec.getColumnTypes()[i].getId();
				names += "," + spec.getColumnNames()[i];
			}
			writer.write(types + "\n");
			writer.write(names + "\n");
			while (tableIterator.hasNext()) {
				Row row = tableIterator.next();
				String line = row.getRowKey();
				for (Cell cell : row) {
					String value = "";
					if (cell.isMissing()) {
						value = "MissingCell";
					} else {
						switch(cell.getColumnType()) {
						case BOOLEAN:
							value = cell.getBooleanValue() ? "True" : "False";
							break;
						case BOOLEAN_LIST:
						case BOOLEAN_SET:
							Boolean[] booleanArray = cell.getBooleanArrayValue();
							StringBuilder booleanBuilder = new StringBuilder();
							booleanBuilder.append(cell.getColumnType()==Type.BOOLEAN_LIST ? "[" : "{");
							for (int i = 0; i < booleanArray.length; i++) {
								booleanBuilder.append(booleanArray[i] ? "True" : "False");
								if (i+1 < booleanArray.length) {
									booleanBuilder.append(",");
								}
							}
							booleanBuilder.append(cell.getColumnType()==Type.BOOLEAN_LIST ? "]" : "}");
							value = booleanBuilder.toString();
							break;
						case INTEGER:
							value = cell.getIntegerValue().toString();
							break;
						case INTEGER_LIST:
						case INTEGER_SET:
							Integer[] integerArray = cell.getIntegerArrayValue();
							StringBuilder integerBuilder = new StringBuilder();
							integerBuilder.append(cell.getColumnType()==Type.INTEGER_LIST ? "[" : "{");
							for (int i = 0; i < integerArray.length; i++) {
								integerBuilder.append(integerArray[i].toString());
								if (i+1 < integerArray.length) {
									integerBuilder.append(",");
								}
							}
							integerBuilder.append(cell.getColumnType()==Type.INTEGER_LIST ? "]" : "}");
							value = integerBuilder.toString();
							break;
						case LONG:
							value = cell.getLongValue().toString();
							break;
						case LONG_LIST:
						case LONG_SET:
							Long[] longArray = cell.getLongArrayValue();
							StringBuilder longBuilder = new StringBuilder();
							longBuilder.append(cell.getColumnType()==Type.LONG_LIST ? "[" : "{");
							for (int i = 0; i < longArray.length; i++) {
								longBuilder.append(longArray[i].toString());
								if (i+1 < longArray.length) {
									longBuilder.append(",");
								}
							}
							longBuilder.append(cell.getColumnType()==Type.LONG_LIST ? "]" : "}");
							value = longBuilder.toString();
							break;
						case DOUBLE:
							Double doubleValue = cell.getDoubleValue();
							if (Double.isInfinite(doubleValue)) {
								if (doubleValue > 0) {
									value = "inf";
								} else {
									value = "-inf";
								}
							} else if (Double.isNaN(doubleValue)) {
								value = "NaN";
							} else {
								value = doubleValue.toString();
							}
							break;
						case DOUBLE_LIST:
						case DOUBLE_SET:
							Double[] doubleArray = cell.getDoubleArrayValue();
							StringBuilder doubleBuilder = new StringBuilder();
							doubleBuilder.append(cell.getColumnType()==Type.DOUBLE_LIST ? "[" : "{");
							for (int i = 0; i < doubleArray.length; i++) {
								doubleBuilder.append(doubleArray[i].toString());
								if (i+1 < doubleArray.length) {
									doubleBuilder.append(",");
								}
							}
							doubleBuilder.append(cell.getColumnType()==Type.DOUBLE_LIST ? "]" : "}");
							value = doubleBuilder.toString();
							break;
						case STRING:
							value = cell.getStringValue();
							break;
						case STRING_LIST:
						case STRING_SET:
							String[] stringArray = cell.getStringArrayValue();
							StringBuilder stringBuilder = new StringBuilder();
							stringBuilder.append(cell.getColumnType()==Type.STRING_LIST ? "[" : "{");
							for (int i = 0; i < stringArray.length; i++) {
								stringBuilder.append(stringArray[i].toString());
								if (i+1 < stringArray.length) {
									stringBuilder.append(",");
								}
							}
							stringBuilder.append(cell.getColumnType()==Type.STRING_LIST ? "]" : "}");
							value = stringBuilder.toString();
							break;
							// TODO more...
						default:
							break;
						}
					}
					value = escapeValue(value);
					line += "," + value;
				}
				writer.write(line + "\n");
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
			file.deleteOnExit();
			FileReader reader = new FileReader(file);
			BufferedReader br = new BufferedReader(reader);
			List<String> types = parseLine(br);
			List<String> names = parseLine(br);
			List<String> values;
			while ((values = parseLine(br)) != null) {
				Row row = new RowImpl(values.get(0), values.size() - 1);
				for (int i = 0; i < values.size() - 1; i++) {
					String columnName = tableCreator.getTableSpec().getColumnNames()[i];
					Type type = tableCreator.getTableSpec().getColumnTypes()[i];
					Cell cell;
					String value = values.get(i+1);
					if (value.equals("MissingCell")) {
						cell = new CellImpl(columnName);
					} else {
						switch(type) {
						case BOOLEAN:
							cell = new CellImpl(value.equals("True") ? true : false);
							break;
						case BOOLEAN_LIST:
						case BOOLEAN_SET:
							String[] booleanValues = value.substring(1, value.length()-1).split(",");
							Boolean[] booleanArray = new Boolean[booleanValues.length];
							for (int j = 0; j < booleanArray.length; j++) {
								booleanArray[j] = booleanValues[j].trim().equals("True");
							}
							cell = new CellImpl(booleanArray, type==Type.BOOLEAN_SET);
							break;
						case INTEGER:
							cell = new CellImpl(Integer.parseInt(value));
							break;
						case INTEGER_LIST:
						case INTEGER_SET:
							String[] integerValues = value.substring(1, value.length()-1).split(",");
							Integer[] integerArray = new Integer[integerValues.length];
							for (int j = 0; j < integerArray.length; j++) {
								integerArray[j] = Integer.parseInt(integerValues[j].trim());
							}
							cell = new CellImpl(integerArray, type==Type.INTEGER_SET);
							break;
						case LONG:
							cell = new CellImpl(Long.parseLong(value));
							break;
						case LONG_LIST:
						case LONG_SET:
							String[] longValues = value.substring(1, value.length()-1).split(",");
							Long[] longArray = new Long[longValues.length];
							for (int j = 0; j < longArray.length; j++) {
								longArray[j] = Long.parseLong(longValues[j].trim());
							}
							cell = new CellImpl(longArray, type==Type.LONG_SET);
							break;
						case DOUBLE:
							Double doubleValue;
							if (value.equals("inf")) {
								doubleValue = Double.POSITIVE_INFINITY;
							} else if (value.equals("-inf")) {
								doubleValue = Double.NEGATIVE_INFINITY;
							} else if (value.equals("NaN")) {
								doubleValue = Double.NaN;
							} else {
								doubleValue = Double.parseDouble(value);
							}
							cell = new CellImpl(doubleValue);
							break;
						case DOUBLE_LIST:
						case DOUBLE_SET:
							String[] doubleValues = value.substring(1, value.length()-1).split(",");
							Double[] doubleArray = new Double[doubleValues.length];
							for (int j = 0; j < doubleArray.length; j++) {
								doubleArray[j] = Double.parseDouble(doubleValues[j].trim());
							}
							cell = new CellImpl(doubleArray, type==Type.DOUBLE_SET);
							break;
						case STRING:
							cell = new CellImpl(value);
							break;
						case STRING_LIST:
						case STRING_SET:
							String[] stringValues = value.substring(1, value.length()-1).split(",");
							String[] stringArray = new String[stringValues.length];
							for (int j = 0; j < stringArray.length; j++) {
								stringArray[j] = stringValues[j].trim();
							}
							cell = new CellImpl(stringArray, type==Type.STRING_SET);
							break;
							// TODO more...
						default:
							cell = new CellImpl(columnName);
							break;
						}
					}
					row.setCell(cell, i);
				}
				tableCreator.addRow(row);
			}
			br.close();
			file.delete();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TableSpec tableSpecFromBytes(byte[] bytes) {
		try {
			File file = new File(new String(bytes));
			file.deleteOnExit();
			FileReader reader = new FileReader(file);
			BufferedReader br = new BufferedReader(reader);
			List<String> typeValues = parseLine(br);
			List<String> nameValues = parseLine(br);
			Type[] types = new Type[typeValues.size() - 1];
			String[] names = new String[types.length];
			for (int i = 0; i < types.length; i++) {
				types[i] = Type.getTypeForId(Integer.parseInt(typeValues.get(i+1)));
				names[i] = nameValues.get(i+1);
			}
			TableSpec spec = new TableSpecImpl(types, names);
			br.close();
			return spec;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static List<String> parseLine(final BufferedReader reader) throws IOException {
		List<String> values = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		boolean end = false;
		boolean escaped = false;
		Character previous = null;
		while (!end) {
			int read = reader.read();
			if (read < 0) {
				if (values.size() > 0 || sb.length()>0) {
					values.add(sb.toString());
					return values;
				} else {
					return null;
				}
			}
			char c = (char)read;
			if (c == '\n' && !escaped) {
				end = true;
			} else if (c == '"') {
				if (previous == '"') {
					sb.append(c);
				}
				escaped = !escaped;
			} else if (c == ',' && !escaped) {
				values.add(sb.toString());
				sb = new StringBuilder();
			} else {
				sb.append(c);
			}
			previous = c;
		}
		values.add(sb.toString());
		return values;
	}
	
	private static String escapeValue(String value) {
		value = value.replace("\"", "\"\"");
		if (value.contains("\"") || value.contains("\n") || value.contains(",") || value.contains("\r")) {
			value = "\"" + value + "\"";
		}
		return value;
	}

}
