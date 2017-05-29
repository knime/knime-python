package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.StringValue;
import org.knime.core.data.collection.CollectionDataValue;
import org.knime.core.data.collection.SetDataValue;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableChunker;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.typeextension.KnimeToPythonExtension;
import org.knime.python2.typeextension.KnimeToPythonExtensions;
import org.knime.python2.typeextension.Serializer;

public class BufferedDataTableChunker implements TableChunker {

private static final NodeLogger LOGGER = NodeLogger.getLogger(BufferedDataTableIterator.class);
	
	private final int m_numberRows;
	private final IterationProperties m_iterationProperties;
	private CloseableRowIterator m_iterator;
	private TableSpec m_spec;
	
	private BufferedDataTableIterator m_currentTableIterator;
	
	public BufferedDataTableChunker(final DataTableSpec spec, final CloseableRowIterator rowIterator, final int numberRows) {
		this(BufferedDataTableIterator.dataTableSpecToTableSpec(spec), rowIterator, numberRows);
	}
	
	public BufferedDataTableChunker(final TableSpec spec, final CloseableRowIterator rowIterator, final int numberRows) {
		m_numberRows = numberRows;
		m_spec = spec;
		m_iterationProperties = new IterationProperties(numberRows);
		m_iterator = rowIterator;
		m_currentTableIterator = null;
	}
	
	@Override
	public boolean hasNextChunk() {
		return m_iterationProperties.m_remainingRows > 0;
	}

	@Override
	public TableIterator nextChunk(int numRows) {
		return nextChunk(numRows, new ExecutionMonitor());
	}
	
	public TableIterator nextChunk(int numRows, ExecutionMonitor executionMonitor)
	{
		if(numRows > m_iterationProperties.m_remainingRows)
			numRows = m_iterationProperties.m_remainingRows;
		if(m_currentTableIterator != null)
			m_currentTableIterator.close();
		m_currentTableIterator = new BufferedDataTableIterator(m_spec, m_iterator, numRows, executionMonitor, m_iterationProperties);
		return m_currentTableIterator;
	}


	@Override
	public int getNumberRemainingRows() {
		return m_iterationProperties.m_remainingRows;
	}

	@Override
	public TableSpec getTableSpec() {
		return m_spec;
	}
	
	class IterationProperties {
		public int m_remainingRows;
		private IterationProperties(int numRows) {
			m_remainingRows = numRows;
		}
	}

}
