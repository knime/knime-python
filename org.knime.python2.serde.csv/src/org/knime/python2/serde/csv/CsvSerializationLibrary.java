/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
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
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.util.FileUtil;
import org.knime.core.util.ThreadUtils;
import org.knime.python2.extensions.serializationlibrary.SerializationException;
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
import org.knime.python2.kernel.PythonCancelable;
import org.knime.python2.kernel.PythonCanceledExecutionException;
import org.knime.python2.kernel.PythonExecutionException;
import org.knime.python2.util.BitArray;
import org.knime.python2.util.PythonUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Used for (de)serializing KNIME tables via CSV files.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class CsvSerializationLibrary implements SerializationLibrary {

    /** Used to make (de-)serialization cancelable. */
    private final ExecutorService m_executorService = ThreadUtils.executorServiceWithContext(
        Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("python-csv-serde-%d").build()));

    /**
     * The root directory in which the temporary files used for data transfer are stored. Will be populated during the
     * first call of {@link #tableToBytes(TableIterator, SerializationOptions, PythonCancelable)}.
     */
    private File m_tempDir;

    /**
     * Writes a table to a temporary CSV file and serializes the file path as bytes. The file path is sent to python
     * where the data is read from the CSV file which is deleted afterwards.
     *
     * @param tableIterator Iterator for the table that should be converted.
     * @param serializationOptions All options that control the serialization process.
     * @return The bytes that should be send to python.
     */
    @Override
    public byte[] tableToBytes(final TableIterator tableIterator, final SerializationOptions serializationOptions,
        final PythonCancelable cancelable) throws SerializationException, PythonCanceledExecutionException {
        File file = null;
        try {
            // Temporary files are used for data transfer.
            if (m_tempDir == null || !m_tempDir.exists()) {
                // Deleted upon JVM shutdown (or #close()).
                m_tempDir = FileUtil.createTempDir("knime-python-");
            }
            file = FileUtil.createTempFile("java-to-python-", ".csv", m_tempDir, false);
            final File finalFile = file;
            return PythonUtils.Misc.executeCancelable(
                () -> tableToBytesInternal(tableIterator, serializationOptions, finalFile), m_executorService,
                cancelable);
        } catch (final IOException | PythonExecutionException e) {
            PythonUtils.Misc.invokeSafely(null, File::delete, file);
            throw new SerializationException("An error occurred during serialization. See log for errors.", e);
        } catch (NegativeArraySizeException ex) {
            PythonUtils.Misc.invokeSafely(null, File::delete, file);
            throw new SerializationException(
                "The requested buffer size during serialization exceeds the maximum buffer size."
                    + " Please consider decreasing the 'Rows per chunk' parameter in the 'Options' tab of the"
                    + " configuration dialog.");
        } catch (Exception ex) {
            PythonUtils.Misc.invokeSafely(null, File::delete, file);
            throw ex;
        }
    }

    /**
     * Possibly interrupted by {@link #bytesIntoTable(TableCreator, byte[], SerializationOptions, PythonCancelable)}.
     */
    private static byte[] tableToBytesInternal(final TableIterator tableIterator,
        final SerializationOptions serializationOptions, final File file) throws IOException {
        try (final FileWriter writer = new FileWriter(file)) {
            String types = "#";
            String names = "";
            final TableSpec spec = tableIterator.getTableSpec();
            for (int i = 0; i < spec.getNumberColumns(); i++) {
                types += "," + spec.getColumnTypes()[i].getId();
                names += "," + spec.getColumnNames()[i];
            }
            String serializers = "#";
            for (final Entry<String, String> entry : spec.getColumnSerializers().entrySet()) {
                serializers += ',' + entry.getKey() + '=' + entry.getValue();
            }
            writer.write(types + "\n");
            writer.write(serializers + "\n");
            writer.write(names + "\n");
            int ctr;
            while (tableIterator.hasNext()) {
                if (Thread.interrupted()) {
                    // Stop serialization if canceled by client.
                    throw new CancellationException("Serialization canceled by client.");
                }
                final Row row = tableIterator.next();
                String line = row.getRowKey();
                ctr = 0;
                for (final Cell cell : row) {
                    String value = "";
                    if (cell.isMissing()) {
                        value = "MissingCell";
                        final Type type = spec.getColumnTypes()[ctr];
                        if (serializationOptions.getConvertMissingToPython()
                            && ((type == Type.INTEGER) || (type == Type.LONG))) {
                            value = Long.toString(serializationOptions.getSentinelForType(type));
                        }
                    } else {
                        Type type = cell.getColumnType();
                        switch (type) {
                            case BOOLEAN:
                                value = cell.getBooleanValue() ? "True" : "False";
                                break;
                            case BOOLEAN_LIST:
                            case BOOLEAN_SET:
                                final boolean[] booleanArray = cell.getBooleanArrayValue();
                                final StringBuilder booleanBuilder = new StringBuilder();
                                booleanBuilder.append(cell.getColumnType() == Type.BOOLEAN_LIST ? "[" : "{");
                                for (int i = 0; i < booleanArray.length; i++) {
                                    if (type == Type.BOOLEAN_LIST && cell.isMissing(i)) {
                                        booleanBuilder.append("None");
                                    } else {
                                        booleanBuilder.append(booleanArray[i] ? "True" : "False");
                                    }
                                    if ((i + 1) < booleanArray.length) {
                                        booleanBuilder.append(",");
                                    }
                                }
                                if (type == Type.BOOLEAN_SET && cell.hasMissingInSet()) {
                                    if (booleanArray.length > 0) {
                                        booleanBuilder.append(",");
                                    }
                                    booleanBuilder.append("None");
                                }
                                booleanBuilder.append(cell.getColumnType() == Type.BOOLEAN_LIST ? "]" : "}");
                                value = booleanBuilder.toString();
                                break;
                            case INTEGER:
                                value = Integer.toString(cell.getIntegerValue());
                                break;
                            case INTEGER_LIST:
                            case INTEGER_SET:
                                final int[] integerArray = cell.getIntegerArrayValue();
                                final StringBuilder integerBuilder = new StringBuilder();
                                integerBuilder.append(cell.getColumnType() == Type.INTEGER_LIST ? "[" : "{");
                                for (int i = 0; i < integerArray.length; i++) {
                                    if (type == Type.INTEGER_LIST && cell.isMissing(i)) {
                                        integerBuilder.append("None");
                                    } else {
                                        integerBuilder.append(Integer.toString(integerArray[i]));
                                    }
                                    if ((i + 1) < integerArray.length) {
                                        integerBuilder.append(",");
                                    }
                                }
                                if (type == Type.INTEGER_SET && cell.hasMissingInSet()) {
                                    if (integerArray.length > 0) {
                                        integerBuilder.append(",");
                                    }
                                    integerBuilder.append("None");
                                }
                                integerBuilder.append(cell.getColumnType() == Type.INTEGER_LIST ? "]" : "}");
                                value = integerBuilder.toString();
                                break;
                            case LONG:
                                value = Long.toString(cell.getLongValue());
                                break;
                            case LONG_LIST:
                            case LONG_SET:
                                final long[] longArray = cell.getLongArrayValue();
                                final StringBuilder longBuilder = new StringBuilder();
                                longBuilder.append(cell.getColumnType() == Type.LONG_LIST ? "[" : "{");
                                for (int i = 0; i < longArray.length; i++) {
                                    if (type == Type.LONG_LIST && cell.isMissing(i)) {
                                        longBuilder.append("None");
                                    } else {
                                        longBuilder.append(Long.toString(longArray[i]));
                                    }
                                    if ((i + 1) < longArray.length) {
                                        longBuilder.append(",");
                                    }
                                }
                                if (type == Type.LONG_SET && cell.hasMissingInSet()) {
                                    if (longArray.length > 0) {
                                        longBuilder.append(",");
                                    }
                                    longBuilder.append("None");
                                }
                                longBuilder.append(cell.getColumnType() == Type.LONG_LIST ? "]" : "}");
                                value = longBuilder.toString();
                                break;
                            case DOUBLE:
                                final double doubleValue = cell.getDoubleValue();
                                if (Double.isInfinite(doubleValue)) {
                                    if (doubleValue > 0) {
                                        value = "inf";
                                    } else {
                                        value = "-inf";
                                    }
                                } else if (Double.isNaN(doubleValue)) {
                                    value = "NaN";
                                } else {
                                    value = Double.toString(doubleValue);
                                }
                                break;
                            case DOUBLE_LIST:
                            case DOUBLE_SET:
                                final double[] doubleArray = cell.getDoubleArrayValue();
                                final StringBuilder doubleBuilder = new StringBuilder();
                                doubleBuilder.append(cell.getColumnType() == Type.DOUBLE_LIST ? "[" : "{");
                                for (int i = 0; i < doubleArray.length; i++) {
                                    if (type == Type.DOUBLE_LIST && cell.isMissing(i)) {
                                        doubleBuilder.append("None");
                                    } else {
                                        String doubleVal = Double.toString(doubleArray[i]);
                                        if (doubleVal.equals("NaN")) {
                                            doubleVal = "float('nan')";
                                        } else if (doubleVal.equals("-Infinity")) {
                                            doubleVal = "float('-inf')";
                                        } else if (doubleVal.equals("Infinity")) {
                                            doubleVal = "float('inf')";
                                        }
                                        doubleBuilder.append(doubleVal);
                                    }
                                    if ((i + 1) < doubleArray.length) {
                                        doubleBuilder.append(",");
                                    }
                                }
                                if (type == Type.DOUBLE_SET && cell.hasMissingInSet()) {
                                    if (doubleArray.length > 0) {
                                        doubleBuilder.append(",");
                                    }
                                    doubleBuilder.append("None");
                                }
                                doubleBuilder.append(cell.getColumnType() == Type.DOUBLE_LIST ? "]" : "}");
                                value = doubleBuilder.toString();
                                break;
                            case STRING:
                                value = cell.getStringValue();
                                break;
                            case STRING_LIST:
                            case STRING_SET:
                                final String[] stringArray = cell.getStringArrayValue();
                                final StringBuilder stringBuilder = new StringBuilder();
                                stringBuilder.append(cell.getColumnType() == Type.STRING_LIST ? "[" : "{");
                                for (int i = 0; i < stringArray.length; i++) {
                                    String stringValue = stringArray[i];
                                    if (type == Type.STRING_LIST && cell.isMissing(i)) {
                                        stringBuilder.append("None");
                                    } else {
                                        stringValue = stringValue.replace("\\", "\\\\");
                                        stringValue = stringValue.replace("'", "\\'");
                                        stringValue = stringValue.replace("\r", "\\r");
                                        stringValue = stringValue.replace("\n", "\\n");
                                        stringBuilder.append("'" + stringValue + "'");
                                    }
                                    if ((i + 1) < stringArray.length) {
                                        stringBuilder.append(",");
                                    }
                                }
                                if (type == Type.STRING_SET && cell.hasMissingInSet()) {
                                    if (stringArray.length > 0) {
                                        stringBuilder.append(",");
                                    }
                                    stringBuilder.append("None");
                                }
                                stringBuilder.append(cell.getColumnType() == Type.STRING_LIST ? "]" : "}");
                                value = stringBuilder.toString();
                                break;
                            case BYTES:
                                value = bytesToBase64(cell.getBytesValue());
                                break;
                            case BYTES_LIST:
                            case BYTES_SET:
                                final byte[][] bytesArray = cell.getBytesArrayValue();
                                final StringBuilder bytesBuilder = new StringBuilder();
                                bytesBuilder.append(cell.getColumnType() == Type.BYTES_LIST ? "[" : "{");
                                for (int i = 0; i < bytesArray.length; i++) {
                                    final byte[] bytesValue = bytesArray[i];
                                    if (type == Type.BYTES_LIST && cell.isMissing(i)) {
                                        bytesBuilder.append("None");
                                    } else {
                                        bytesBuilder.append("'" + bytesToBase64(bytesValue) + "'");
                                    }
                                    if ((i + 1) < bytesArray.length) {
                                        bytesBuilder.append(",");
                                    }
                                }
                                if (type == Type.BYTES_SET && cell.hasMissingInSet()) {
                                    if (bytesArray.length > 0) {
                                        bytesBuilder.append(",");
                                    }
                                    bytesBuilder.append("None");
                                }
                                bytesBuilder.append(cell.getColumnType() == Type.BYTES_LIST ? "]" : "}");
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
        }
        return file.getAbsolutePath().getBytes();
    }

    /**
     * Reads a table from a temporary CSV file which is deleted afterwards. The file path is received as bytes from
     * python.
     *
     * @param tableCreator The {@link TableCreator} that the rows should be added to.
     * @param serializationOptions All options that control the serialization process.
     * @param bytes The bytes containing the encoded table.
     */
    @Override
    public void bytesIntoTable(final TableCreator<?> tableCreator, final byte[] bytes,
        final SerializationOptions serializationOptions, final PythonCancelable cancelable)
        throws SerializationException, PythonCanceledExecutionException {
        File file = null;
        try {
            file = new File(new String(bytes, StandardCharsets.UTF_8));
            final File finalFile = file;
            PythonUtils.Misc.executeCancelable(() -> {
                bytesIntoTableInternal(tableCreator, serializationOptions, finalFile);
                return null;
            }, m_executorService, cancelable);
        } catch (final PythonExecutionException e) {
            throw new SerializationException("An error occurred during deserialization. See log for details.", e);
        } finally {
            PythonUtils.Misc.invokeSafely(null, File::delete, file);
        }
    }

    /**
     * Possibly interrupted by {@link #bytesIntoTable(TableCreator, byte[], SerializationOptions, PythonCancelable)}.
     */
    private static void bytesIntoTableInternal(final TableCreator<?> tableCreator,
        final SerializationOptions serializationOptions, final File file) throws IOException {
        try (final BufferedReader br = new BufferedReader(new FileReader(file))) {
            final List<String> types = parseLine(br); // Ignore, just to skip header.
            final List<String> serializers = parseLine(br); // Ignore, just to skip header.
            final List<String> names = parseLine(br); // Ignore, just to skip header.
            List<String> values;
            while ((values = parseLine(br)) != null) {
                if (Thread.interrupted()) {
                    // Stop deserialization if canceled by client.
                    throw new CancellationException("Deserialization canceled by client.");
                }
                final Row row = new RowImpl(values.get(0), values.size() - 1);
                for (int i = 0; i < (values.size() - 1); i++) {
                    final String columnName = tableCreator.getTableSpec().getColumnNames()[i];
                    final Type type = tableCreator.getTableSpec().getColumnTypes()[i];
                    Cell cell;
                    String value = values.get(i + 1);
                    if (value.equals("MissingCell")) {
                        if (type == Type.DOUBLE) {
                            cell = new CellImpl(Double.NaN);
                        } else {
                            cell = new CellImpl();
                        }
                    } else {
                        int idxCtr = 0;
                        switch (type) {
                            case BOOLEAN:
                                cell = new CellImpl(value.equals("True") ? true : false);
                                break;
                            case BOOLEAN_LIST:
                                if (value.startsWith("set([")) {
                                    value = value.substring(5, value.length() - 2);
                                } else {
                                    value = value.substring(1, value.length() - 1);
                                }
                                final String[] booleanValues = value.split(",");
                                final boolean[] booleanArray = new boolean[booleanValues.length];
                                BitArray booleanMissings = new BitArray(booleanValues.length);
                                for (int j = 0; j < booleanArray.length; j++) {
                                    booleanValues[j] = booleanValues[j].trim();
                                    if (!booleanValues[j].equals("None")) {
                                        booleanMissings.setToOne(j);
                                        booleanArray[j] = booleanValues[j].equals("True");
                                    }
                                }
                                cell = new CellImpl(booleanArray, booleanMissings.getEncodedByteArray());
                                break;
                            case BOOLEAN_SET:
                                if (value.startsWith("set([")) {
                                    value = value.substring(5, value.length() - 2);
                                } else {
                                    value = value.substring(1, value.length() - 1);
                                }
                                final String[] booleanSetValues = value.split(",");
                                final boolean[] booleanSetArray = new boolean[booleanSetValues.length];
                                boolean booleanHasMissing = false;
                                for (String bsValue : booleanSetValues) {
                                    bsValue = bsValue.trim();
                                    if (!bsValue.equals("None")) {
                                        booleanSetArray[idxCtr] = bsValue.equals("True");
                                        idxCtr++;
                                    } else {
                                        booleanHasMissing = true;
                                    }
                                }
                                if (!booleanHasMissing) {
                                    cell = new CellImpl(booleanSetArray, false);
                                } else {
                                    cell = new CellImpl(
                                        ArrayUtils.subarray(booleanSetArray, 0, booleanSetArray.length - 1), true);
                                }
                                break;
                            case INTEGER:
                                final int intVal = Integer.parseInt(value);
                                if (serializationOptions.getConvertMissingFromPython()
                                    && serializationOptions.isSentinel(Type.INTEGER, intVal)) {
                                    cell = new CellImpl();
                                } else {
                                    cell = new CellImpl(intVal);
                                }
                                break;
                            case INTEGER_LIST:
                                if (value.startsWith("set([")) {
                                    value = value.substring(5, value.length() - 2);
                                } else {
                                    value = value.substring(1, value.length() - 1);
                                }
                                final String[] integerValues = value.split(",");
                                final int[] integerArray = new int[integerValues.length];
                                BitArray integerMissings = new BitArray(integerValues.length);
                                for (int j = 0; j < integerArray.length; j++) {
                                    integerValues[j] = integerValues[j].trim();
                                    if (!integerValues[j].equals("None")) {
                                        integerMissings.setToOne(j);
                                        integerArray[j] = Integer.parseInt(integerValues[j]);
                                    }
                                }
                                cell = new CellImpl(integerArray, integerMissings.getEncodedByteArray());
                                break;
                            case INTEGER_SET:
                                if (value.startsWith("set([")) {
                                    value = value.substring(5, value.length() - 2);
                                } else {
                                    value = value.substring(1, value.length() - 1);
                                }
                                final String[] integerSetValues = value.split(",");
                                final int[] integerSetArray = new int[integerSetValues.length];
                                boolean integerHasMissing = false;
                                for (String bsValue : integerSetValues) {
                                    bsValue = bsValue.trim();
                                    if (!bsValue.equals("None")) {
                                        integerSetArray[idxCtr] = Integer.parseInt(bsValue);
                                        idxCtr++;
                                    } else {
                                        integerHasMissing = true;
                                    }
                                }
                                if (!integerHasMissing) {
                                    cell = new CellImpl(integerSetArray, false);
                                } else {
                                    cell = new CellImpl(
                                        ArrayUtils.subarray(integerSetArray, 0, integerSetArray.length - 1), true);
                                }
                                break;
                            case LONG:
                                final long longVal = Long.parseLong(value.replace("L", ""));
                                if (serializationOptions.getConvertMissingFromPython()
                                    && serializationOptions.isSentinel(Type.LONG, longVal)) {
                                    cell = new CellImpl();
                                } else {
                                    cell = new CellImpl(longVal);
                                }
                                break;
                            case LONG_LIST:
                                if (value.startsWith("set([")) {
                                    value = value.substring(5, value.length() - 2);
                                } else {
                                    value = value.substring(1, value.length() - 1);
                                }
                                final String[] longValues = value.split(",");
                                final long[] longArray = new long[longValues.length];
                                BitArray longMissings = new BitArray(longValues.length);
                                for (int j = 0; j < longArray.length; j++) {
                                    longValues[j] = longValues[j].trim();
                                    if (!longValues[j].equals("None")) {
                                        longMissings.setToOne(j);
                                        longArray[j] = Long.parseLong(longValues[j].replace("L", ""));
                                    }
                                }
                                cell = new CellImpl(longArray, longMissings.getEncodedByteArray());
                                break;
                            case LONG_SET:
                                if (value.startsWith("set([")) {
                                    value = value.substring(5, value.length() - 2);
                                } else {
                                    value = value.substring(1, value.length() - 1);
                                }
                                final String[] longSetValues = value.split(",");
                                final long[] longSetArray = new long[longSetValues.length];
                                boolean longHasMissing = false;
                                for (String bsValue : longSetValues) {
                                    bsValue = bsValue.trim();
                                    if (!bsValue.equals("None")) {
                                        longSetArray[idxCtr] = Long.parseLong(bsValue.replace("L", ""));
                                        idxCtr++;
                                    } else {
                                        longHasMissing = true;
                                    }
                                }
                                if (!longHasMissing) {
                                    cell = new CellImpl(longSetArray, false);
                                } else {
                                    cell = new CellImpl(ArrayUtils.subarray(longSetArray, 0, longSetArray.length - 1),
                                        true);
                                }
                                break;
                            case DOUBLE:
                                double doubleValue;
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
                                if (value.startsWith("set([")) {
                                    value = value.substring(5, value.length() - 2);
                                } else {
                                    value = value.substring(1, value.length() - 1);
                                }
                                final String[] doubleValues = value.split(",");
                                final double[] doubleArray = new double[doubleValues.length];
                                BitArray doubleMissings = new BitArray(doubleValues.length);
                                for (int j = 0; j < doubleArray.length; j++) {
                                    doubleValues[j] = doubleValues[j].trim();
                                    if (!doubleValues[j].equals("None")) {
                                        doubleMissings.setToOne(j);
                                        final String doubleVal = doubleValues[j];
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
                                cell = new CellImpl(doubleArray, doubleMissings.getEncodedByteArray());
                                break;
                            case DOUBLE_SET:
                                if (value.startsWith("set([")) {
                                    value = value.substring(5, value.length() - 2);
                                } else {
                                    value = value.substring(1, value.length() - 1);
                                }
                                final String[] doubleSetValues = value.split(",");
                                final double[] doubleSetArray = new double[doubleSetValues.length];
                                boolean doubleHasMissing = false;
                                for (String bsValue : doubleSetValues) {
                                    bsValue = bsValue.trim();
                                    if (!bsValue.equals("None")) {
                                        final String doubleVal = bsValue;
                                        if (doubleVal.equals("nan")) {
                                            doubleSetArray[idxCtr] = Double.NaN;
                                        } else if (doubleVal.equals("inf")) {
                                            doubleSetArray[idxCtr] = Double.POSITIVE_INFINITY;
                                        } else if (doubleVal.equals("-inf")) {
                                            doubleSetArray[idxCtr] = Double.NEGATIVE_INFINITY;
                                        } else {
                                            doubleSetArray[idxCtr] = Double.parseDouble(doubleVal);
                                        }
                                        idxCtr++;
                                    } else {
                                        doubleHasMissing = true;
                                    }
                                }
                                if (!doubleHasMissing) {
                                    cell = new CellImpl(doubleSetArray, false);
                                } else {
                                    cell = new CellImpl(
                                        ArrayUtils.subarray(doubleSetArray, 0, doubleSetArray.length - 1), true);
                                }
                                break;
                            case STRING:
                                cell = new CellImpl(value);
                                break;
                            case STRING_LIST:
                                if (value.startsWith("set([")) {
                                    value = value.substring(5, value.length() - 2);
                                } else {
                                    value = value.substring(1, value.length() - 1);
                                }
                                value = value.replaceAll("None", "'None'");
                                final String[] stringValues = value.split("(', '|', \"|\", '|\", \")");
                                final String[] stringArray = new String[stringValues.length];
                                BitArray stringMissings = new BitArray(stringValues.length);
                                for (int j = 0; j < stringArray.length; j++) {
                                    stringArray[j] = stringValues[j];
                                    if (j == 0) {
                                        stringArray[j] = stringArray[j].substring(1);
                                    }
                                    if (j == (stringArray.length - 1)) {
                                        stringArray[j] = stringArray[j].substring(0, stringArray[j].length() - 1);
                                    }
                                    if (!(stringArray[j].equals("None") || stringArray[j].equals("'None'"))) {
                                        // stringArray[j] = stringArray[j].substring(1, stringArray[j].length()-1);
                                        stringArray[j] = stringArray[j].replace("\\\\", "\\");
                                        stringArray[j] = stringArray[j].replace("\\'", "'");
                                        stringArray[j] = stringArray[j].replace("\\r", "\r");
                                        stringArray[j] = stringArray[j].replace("\\n", "\n");
                                        stringArray[j] = stringArray[j].replace("\\t", "\t");
                                        stringMissings.setToOne(j);
                                    }
                                }
                                cell = new CellImpl(stringArray, stringMissings.getEncodedByteArray());
                                break;
                            case STRING_SET:
                                if (value.startsWith("set([")) {
                                    value = value.substring(5, value.length() - 2);
                                } else {
                                    value = value.substring(1, value.length() - 1);
                                }
                                value = value.replaceAll("None", "'None'");
                                final String[] stringSetValues = value.split("(', '|', \"|\", '|\", \")");
                                final String[] stringSetArray = new String[stringSetValues.length];
                                boolean hasStringMissing = false;
                                int posCtr = 0;
                                for (String stringValue : stringSetValues) {
                                    stringSetArray[idxCtr] = stringValue;
                                    if (posCtr == 0) {
                                        stringSetArray[idxCtr] = stringValue.substring(1);
                                    }
                                    if (posCtr == (stringSetArray.length - 1)) {
                                        stringSetArray[idxCtr] = stringValue.substring(0, stringValue.length() - 1);
                                    }
                                    if (!(stringValue.equals("None") || stringValue.contentEquals("'None'"))) {
                                        // stringArray[j] = stringArray[j].substring(1, stringArray[j].length()-1);
                                        stringSetArray[idxCtr] = stringSetArray[idxCtr].replace("\\\\", "\\");
                                        stringSetArray[idxCtr] = stringSetArray[idxCtr].replace("\\'", "'");
                                        stringSetArray[idxCtr] = stringSetArray[idxCtr].replace("\\r", "\r");
                                        stringSetArray[idxCtr] = stringSetArray[idxCtr].replace("\\n", "\n");
                                        stringSetArray[idxCtr] = stringSetArray[idxCtr].replace("\\t", "\t");
                                        idxCtr++;
                                    } else {
                                        hasStringMissing = true;
                                    }
                                    posCtr++;
                                }
                                if (!hasStringMissing) {
                                    cell = new CellImpl(stringSetArray, false);
                                } else {
                                    cell = new CellImpl(
                                        ArrayUtils.subarray(stringSetArray, 0, stringSetArray.length - 1), true);
                                }
                                break;
                            case BYTES:
                                cell = new CellImpl(bytesFromBase64(value));
                                break;
                            case BYTES_LIST:
                                if (value.startsWith("set([")) {
                                    value = value.substring(5, value.length() - 2);
                                } else {
                                    value = value.substring(1, value.length() - 1);
                                }
                                final String[] bytesValues = value.split(",");
                                final byte[][] bytesArray = new byte[bytesValues.length][];
                                BitArray bytesMissings = new BitArray(bytesValues.length);
                                for (int j = 0; j < bytesArray.length; j++) {
                                    bytesValues[j] = bytesValues[j].trim();
                                    if (!bytesValues[j].equals("None")) {
                                        bytesMissings.setToOne(j);
                                        bytesArray[j] = bytesFromBase64(bytesValues[j]);
                                    }
                                }
                                cell = new CellImpl(bytesArray, bytesMissings.getEncodedByteArray());
                                break;
                            case BYTES_SET:
                                if (value.startsWith("set([")) {
                                    value = value.substring(5, value.length() - 2);
                                } else {
                                    value = value.substring(1, value.length() - 1);
                                }
                                final String[] bytesSetValues = value.split(",");
                                final byte[][] bytesSetArray = new byte[bytesSetValues.length][];
                                boolean bytesHasMissing = false;
                                for (String bsValue : bytesSetValues) {
                                    bsValue = bsValue.trim();
                                    if (!bsValue.equals("None")) {
                                        bytesSetArray[idxCtr] = bytesFromBase64(bsValue);
                                        idxCtr++;
                                    } else {
                                        bytesHasMissing = true;
                                    }
                                }
                                if (!bytesHasMissing) {
                                    cell = new CellImpl(bytesSetArray, false);
                                } else {
                                    cell = new CellImpl(ArrayUtils.subarray(bytesSetArray, 0, bytesSetArray.length - 1),
                                        true);
                                }
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
        }
    }

    @Override
    public TableSpec tableSpecFromBytes(final byte[] bytes, final PythonCancelable cancelable)
        throws SerializationException {
        // Note: We don't implement cancellation here, because reading the spec should be cancelable in a timely manner
        // anyway.
        File file = null;
        try {
            file = new File(new String(bytes, StandardCharsets.UTF_8));
            file.deleteOnExit();
            try (final BufferedReader br = new BufferedReader(new FileReader(file))) {
                final List<String> typeValues = parseLine(br);
                final List<String> serializerValues = parseLine(br);
                final List<String> nameValues = parseLine(br);
                final Type[] types = new Type[typeValues.size() - 1];
                final String[] names = new String[types.length];
                for (int i = 0; i < types.length; i++) {
                    types[i] = Type.getTypeForId(Integer.parseInt(typeValues.get(i + 1)));
                    names[i] = nameValues.get(i + 1);
                }
                final Map<String, String> serializers = new HashMap<>();
                for (int i = 1; i < serializerValues.size(); i++) {
                    final String[] keyValuePair = serializerValues.get(i).split("=");
                    serializers.put(keyValuePair[0], keyValuePair[1]);
                }
                return new TableSpecImpl(types, names, serializers);
            }
        } catch (final IOException e) {
            PythonUtils.Misc.invokeSafely(null, File::delete, file);
            throw new SerializationException("An error occurred during deserialization. See log for details.", e);
        } catch (Exception ex) {
            PythonUtils.Misc.invokeSafely(null, File::delete, file);
            throw ex;
        }
    }

    private static List<String> parseLine(final BufferedReader reader) throws IOException {
        final List<String> values = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean end = false;
        boolean escaped = false;
        Character previous = null;
        while (!end) {
            final int read = reader.read();
            if (read < 0) {
                if ((!values.isEmpty()) || (sb.length() > 0)) {
                    values.add(sb.toString());
                    return values;
                } else {
                    return null;
                }
            }
            final char c = (char)read;
            if ((c == '\n') && !escaped) {
                end = true;
            } else if (c == '"') {
                if ((previous != null) && (previous == '"')) {
                    sb.append(c);
                }
                escaped = !escaped;
            } else if ((c == ',') && !escaped) {
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

    private static String bytesToBase64(final byte[] bytes) {
        return new String(Base64.getEncoder().encode(bytes));
    }

    private static byte[] bytesFromBase64(String base64) {
        if (base64.startsWith("b'")) {
            base64 = base64.substring(2, base64.length() - 1);
        } else if (base64.startsWith("'")) {
            base64 = base64.substring(1, base64.length() - 1);
        }
        return Base64.getDecoder().decode(base64.getBytes());
    }

    @Override
    public void close() throws Exception {
        PythonUtils.Misc.invokeSafely(null, ExecutorService::shutdownNow, m_executorService);
        if (m_tempDir != null) {
            PythonUtils.Misc.invokeSafely(null, FileUtil::deleteRecursively, m_tempDir);
        }
    }
}
