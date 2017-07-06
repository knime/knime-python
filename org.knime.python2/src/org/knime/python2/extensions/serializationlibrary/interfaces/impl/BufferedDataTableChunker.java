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

package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionMonitor;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableChunker;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;

/**
 * Used for splitting a {@link BufferedDataTable} into chunks. Assures that only one {@link TableIterator} on the underlying
 * {@link BufferedDataTable} is open at a time. If new {@link TableIterator} is requested the current one is closed and the
 * new one starts at the position of the current iterator in the table.
 *
 * @author Clemens von Schwerin, KNIME.com, Konstanz, Germany
 *
 */

public class BufferedDataTableChunker implements TableChunker {

	private final IterationProperties m_iterationProperties;
	private CloseableRowIterator m_iterator;
	private TableSpec m_spec;

	private BufferedDataTableIterator m_currentTableIterator;

	/**
	 * Constructor.
	 * @param spec         the spec of the table to chunk in the standard KNIME format
	 * @param rowIterator  an iterator for the table to chunk
	 * @param numberRows   the number of rows of the table to chunk
	 */
	public BufferedDataTableChunker(final DataTableSpec spec, final CloseableRowIterator rowIterator, final int numberRows) {
		this(BufferedDataTableIterator.dataTableSpecToTableSpec(spec), rowIterator, numberRows);
	}

	/**
     * Constructor.
     * @param spec         the spec of the table to chunk in the python table representation format
     * @param rowIterator  an iterator for the table to chunk
     * @param numberRows   the number of rows of the table to chunk
     */
	public BufferedDataTableChunker(final TableSpec spec, final CloseableRowIterator rowIterator, final int numberRows) {
		m_spec = spec;
		m_iterationProperties = new IterationProperties(numberRows);
		m_iterator = rowIterator;
		m_currentTableIterator = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasNextChunk() {
		return m_iterationProperties.m_remainingRows > 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TableIterator nextChunk(final int numRows) {
		return nextChunk(numRows, new ExecutionMonitor());
	}

	/**
	 * Returns a TableIterator that updates the passed executionMonitor.
	 *
	 * @param numRows - the number of rows in the next chunk
	 * @param executionMonitor - an {@link ExecutionMonitor} to update while iterating over the table chunk
	 * @return a {@TableIterator} for iterating over the next chunk
	 */
	public TableIterator nextChunk(int numRows, final ExecutionMonitor executionMonitor)
	{
		if(numRows > m_iterationProperties.m_remainingRows) {
            numRows = m_iterationProperties.m_remainingRows;
        }
		if(m_currentTableIterator != null) {
            m_currentTableIterator.close();
        }
		m_currentTableIterator = new BufferedDataTableIterator(m_spec, m_iterator, numRows, executionMonitor, m_iterationProperties);
		return m_currentTableIterator;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberRemainingRows() {
		return m_iterationProperties.m_remainingRows;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TableSpec getTableSpec() {
		return m_spec;
	}

	/**
	 * Internal class shared with the {@link BufferedDataTableIterator} for ensuring the correct row pointer on both ends.
	 */
	class IterationProperties {
		public int m_remainingRows;
		private IterationProperties(final int numRows) {
			m_remainingRows = numRows;
		}
	}

}
