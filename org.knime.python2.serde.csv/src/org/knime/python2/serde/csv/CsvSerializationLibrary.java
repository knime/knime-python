/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 */

package org.knime.python2.serde.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
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
	public byte[] tableToBytes(TableIterator tableIterator, SerializationOptions serializationOptions) {
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
			String serializers = "#";
			for (Entry<String,String> entry : spec.getColumnSerializers().entrySet()) {
				serializers += ',' + entry.getKey() + '=' + entry.getValue();
			}
			writer.write(types + "\n");
			writer.write(serializers + "\n");
			writer.write(names + "\n");
			int ctr;
			while (tableIterator.hasNext()) {
				Row row = tableIterator.next();
				String line = row.getRowKey();
				ctr = 0;
				for (Cell cell : row) {
					String value = "";
					if (cell.isMissing()) {
						value = "MissingCell";
						Type type = spec.getColumnTypes()[ctr];
						if(serializationOptions.getConvertMissingToPython() &&
								(type == Type.INTEGER || type == Type.LONG)) {
							value = Long.toString(serializationOptions.getSentinelForType(type));
						}
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
								if (booleanArray[i] == null) {
									booleanBuilder.append("None");
								} else {
									booleanBuilder.append(booleanArray[i] ? "True" : "False");
								}
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
								if (integerArray[i] == null) {
									integerBuilder.append("None");
								} else {
									integerBuilder.append(integerArray[i].toString());
								}
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
								if (longArray[i] == null) {
									longBuilder.append("None");
								} else {
									longBuilder.append(longArray[i].toString());
								}
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
								if (doubleArray[i] == null) {
									doubleBuilder.append("None");
								} else {
									String doubleVal = doubleArray[i].toString();
									if (doubleVal.equals("NaN")) {
										doubleVal = "float('nan')";
									} else if (doubleVal.equals("-Infinity")) {
										doubleVal = "float('-inf')";
									} else if (doubleVal.equals("Infinity")) {
										doubleVal = "float('inf')";
									}
									doubleBuilder.append(doubleVal);
								}
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
								String stringValue = stringArray[i];
								if (stringValue == null) {
									stringBuilder.append("None");
								} else {
									stringValue = stringValue.replace("\\", "\\\\");
									stringValue = stringValue.replace("'", "\\'");
									stringValue = stringValue.replace("\r", "\\r");
									stringValue = stringValue.replace("\n", "\\n");
									stringBuilder.append("'" + stringValue + "'");
								}
								if (i+1 < stringArray.length) {
									stringBuilder.append(",");
								}
							}
							stringBuilder.append(cell.getColumnType()==Type.STRING_LIST ? "]" : "}");
							value = stringBuilder.toString();
							break;
						case BYTES:
							value = bytesToBase64(cell.getBytesValue());
							break;
						case BYTES_LIST:
						case BYTES_SET:
							Byte[][] bytesArray = cell.getBytesArrayValue();
							StringBuilder bytesBuilder = new StringBuilder();
							bytesBuilder.append(cell.getColumnType()==Type.BYTES_LIST ? "[" : "{");
							for (int i = 0; i < bytesArray.length; i++) {
								Byte[] bytesValue = bytesArray[i];
								if (bytesValue == null) {
									bytesBuilder.append("None");
								} else {
									bytesBuilder.append("'" + bytesToBase64(bytesValue) + "'");
								}
								if (i+1 < bytesArray.length) {
									bytesBuilder.append(",");
								}
							}
							bytesBuilder.append(cell.getColumnType()==Type.BYTES_LIST ? "]" : "}");
							value = bytesBuilder.toString();
							break;
						default:
							break;
						}
					}
					value = escapeValue(value);
					line += "," + value;
					ctr++;
				}
				writer.write(line + "\n");
			}
			writer.close();
			return file.getAbsolutePath().getBytes();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unused")
	@Override
	public void bytesIntoTable(TableCreator tableCreator, byte[] bytes, SerializationOptions serializationOptions) {
		try {
			File file = new File(new String(bytes));
			file.deleteOnExit();
			FileReader reader = new FileReader(file);
			BufferedReader br = new BufferedReader(reader);
			List<String> types = parseLine(br);
			List<String> serializers = parseLine(br);
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
						if(type == Type.DOUBLE) {
							cell = new CellImpl(Double.NaN);
						} else {
							cell = new CellImpl();
						}
					} else {
						switch(type) {
						case BOOLEAN:
							cell = new CellImpl(value.equals("True") ? true : false);
							break;
						case BOOLEAN_LIST:
						case BOOLEAN_SET:
							if (value.startsWith("set([")) {
								value = value.substring(5, value.length()-2);
							} else {
								value = value.substring(1, value.length()-1);
							}
							String[] booleanValues = value.split(",");
							Boolean[] booleanArray = new Boolean[booleanValues.length];
							for (int j = 0; j < booleanArray.length; j++) {
								booleanValues[j] = booleanValues[j].trim();
								if (booleanValues[j].equals("None")) {
									booleanArray[j] = null;
								} else {
									booleanArray[j] = booleanValues[j].equals("True");
								}
							}
							cell = new CellImpl(booleanArray, type==Type.BOOLEAN_SET);
							break;
						case INTEGER:
							int intVal = Integer.parseInt(value);
							if(serializationOptions.getConvertMissingFromPython()
									&& serializationOptions.isSentinel(Type.INTEGER, intVal))
							{
								cell = new CellImpl();
							} else {
								cell = new CellImpl(intVal);
							}
							break;
						case INTEGER_LIST:
						case INTEGER_SET:
							if (value.startsWith("set([")) {
								value = value.substring(5, value.length()-2);
							} else {
								value = value.substring(1, value.length()-1);
							}
							String[] integerValues = value.split(",");
							Integer[] integerArray = new Integer[integerValues.length];
							for (int j = 0; j < integerArray.length; j++) {
								integerValues[j] = integerValues[j].trim();
								if (integerValues[j].equals("None")) {
									integerArray[j] = null;
								} else {
									integerArray[j] = Integer.parseInt(integerValues[j]);
								}
							}
							cell = new CellImpl(integerArray, type==Type.INTEGER_SET);
							break;
						case LONG:
							long longVal = Long.parseLong(value);
							if(serializationOptions.getConvertMissingFromPython()
									&& serializationOptions.isSentinel(Type.LONG, longVal))
							{
								cell = new CellImpl();
							} else {
								cell = new CellImpl(longVal);
							}
							break;
						case LONG_LIST:
						case LONG_SET:
							if (value.startsWith("set([")) {
								value = value.substring(5, value.length()-2);
							} else {
								value = value.substring(1, value.length()-1);
							}
							String[] longValues = value.split(",");
							Long[] longArray = new Long[longValues.length];
							for (int j = 0; j < longArray.length; j++) {
								longValues[j] = longValues[j].trim();
								if (longValues[j].equals("None")) {
									longArray[j] = null;
								} else {
									longArray[j] = Long.parseLong(longValues[j]);
								}
							}
							cell = new CellImpl(longArray, type==Type.LONG_SET);
							break;
						case DOUBLE:
							Double doubleValue;
							if (value.equals("inf")) {
								doubleValue = Double.POSITIVE_INFINITY;
							} else if (value.equals("-inf")) {
								doubleValue = Double.NEGATIVE_INFINITY;
							} else if (value.equals("NaN") || value.contentEquals("nan")) {
								doubleValue = Double.NaN;
							} else {
								doubleValue = Double.parseDouble(value);
							}
							cell = new CellImpl(doubleValue);
							break;
						case DOUBLE_LIST:
						case DOUBLE_SET:
							if (value.startsWith("set([")) {
								value = value.substring(5, value.length()-2);
							} else {
								value = value.substring(1, value.length()-1);
							}
							String[] doubleValues = value.split(",");
							Double[] doubleArray = new Double[doubleValues.length];
							for (int j = 0; j < doubleArray.length; j++) {
								doubleValues[j] = doubleValues[j].trim();
								if (doubleValues[j].equals("None")) {
									doubleArray[j] = null;
								} else {
									String doubleVal = doubleValues[j];
									if (doubleVal.equals("nan")) {
										doubleArray[j] = Double.NaN;
									} else if (doubleVal.equals("inf")) {
										doubleArray[j] = Double.POSITIVE_INFINITY;
									} else if (doubleVal.equals("-inf")) {
										doubleArray[j] = Double.NEGATIVE_INFINITY;
									} else {
										doubleArray[j] = Double.parseDouble(doubleVal);
									}
								}
							}
							cell = new CellImpl(doubleArray, type==Type.DOUBLE_SET);
							break;
						case STRING:
							cell = new CellImpl(value);
							break;
						case STRING_LIST:
						case STRING_SET:
							if (value.startsWith("set([")) {
								value = value.substring(5, value.length()-2);
							} else {
								value = value.substring(1, value.length()-1);
							}
							String[] stringValues = value.split("(', '|', \"|\", '|\", \")");
							String[] stringArray = new String[stringValues.length];
							for (int j = 0; j < stringArray.length; j++) {
								stringArray[j] = stringValues[j];
								if(j == 0) {
									stringArray[j] = stringArray[j].substring(1);
								} else if (j == stringArray.length - 1) {
									stringArray[j] = stringArray[j].substring(0,stringArray[j].length() - 1);
								}
								if (stringArray[j].equals("None")) {
									stringArray[j] = null;
								} else {
									//stringArray[j] = stringArray[j].substring(1, stringArray[j].length()-1);
									stringArray[j] = stringArray[j].replace("\\\\", "\\");
									stringArray[j] = stringArray[j].replace("\\'", "'");
									stringArray[j] = stringArray[j].replace("\\r", "\r");
									stringArray[j] = stringArray[j].replace("\\n", "\n");
									stringArray[j] = stringArray[j].replace("\\t", "\t");
								}
							}
							cell = new CellImpl(stringArray, type==Type.STRING_SET);
							break;
						case BYTES:
							cell = new CellImpl(bytesFromBase64(value));
							break;
						case BYTES_LIST:
						case BYTES_SET:
							if (value.startsWith("set([")) {
								value = value.substring(5, value.length()-2);
							} else {
								value = value.substring(1, value.length()-1);
							}
							String[] bytesValues = value.split(",");
							Byte[][] bytesArray = new Byte[bytesValues.length][];
							for (int j = 0; j < bytesArray.length; j++) {
								bytesValues[j] = bytesValues[j].trim();
								if (bytesValues[j].equals("None")) {
									bytesArray[j] = null;
								} else {
									bytesArray[j] = bytesFromBase64(bytesValues[j]);
								}
							}
							cell = new CellImpl(bytesArray, type==Type.BYTES_SET);
							break;
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
			List<String> serializerValues = parseLine(br);
			List<String> nameValues = parseLine(br);
			Type[] types = new Type[typeValues.size() - 1];
			String[] names = new String[types.length];
			for (int i = 0; i < types.length; i++) {
				types[i] = Type.getTypeForId(Integer.parseInt(typeValues.get(i+1)));
				names[i] = nameValues.get(i+1);
			}
			Map<String, String> serializers = new HashMap<String, String>();
			for (int i = 1; i < serializerValues.size(); i++) {
				String[] keyValuePair = serializerValues.get(i).split("=");
				serializers.put(keyValuePair[0], keyValuePair[1]);
			}
			TableSpec spec = new TableSpecImpl(types, names, serializers);
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
				if (previous != null && previous == '"') {
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
	
	private static String bytesToBase64(final Byte[] bytes) {
		return new String(Base64.getEncoder().encode(ArrayUtils.toPrimitive(bytes)));
	}
	
	private static Byte[] bytesFromBase64(String base64) {
		if (base64.startsWith("b'")) {
			base64 = base64.substring(2, base64.length()-1);
		} else if (base64.startsWith("'")) {
			base64 = base64.substring(1, base64.length()-1);
		}
		return ArrayUtils.toObject(Base64.getDecoder().decode(base64.getBytes()));
	}

}
