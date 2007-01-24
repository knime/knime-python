/* @(#)$RCSfile$
 * $Revision$ $Date$ $Author$
 */
package org.knime.ext.jython;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;

/**
 * This is the implementation of the "JPython Script" node
 *
 * @author Tripos
 */
public class PythonFunctionNodeModel extends PythonScriptNodeModel
{

	// our logger instance
	private static NodeLogger logger = NodeLogger.getLogger(PythonFunctionNodeModel.class);

	
	protected PythonFunctionNodeModel() {
		super(1,1);

		// define the common imports string
		StringBuffer header = new StringBuffer();
		header.append("from org.knime.core.data import *\n");
		header.append("from org.knime.core.data.def import *\n");
		header.append("from org.knime.base.data.append.column import *\n");
		header.append("from org.knime.base import *\n");
		header.append("\n");
		header.append("__dts = inData0.getDataTableSpec()\n");
		header.append("row = 0\n");
		header.append("outColumnType = outColumnTypes[0]\n");
		header.append("print \"outcoltype = \" + outColumnType\n");
		header.append("\n");
		header.append("def val(colname) :\n");
		header.append("    index = __dts.findColumnIndex(colname)\n");
		header.append("    cellType = __dts.getColumnSpec(colname).getType().toString()\n");
		header.append("    cellValue = row.getCell(index)\n");
	    header.append("    stringCellValue = str(cellValue)\n");
	    header.append("\n");
		header.append("    if cellType == \"DoubleCell\":\n");
		header.append("        return float(stringCellValue)\n");
		header.append("    elif cellType == \"IntCell\":\n");
		header.append("        return int(stringCellValue)\n");
		header.append("    else:\n");
		header.append("        return stringCellValue\n");
		header.append("\n");
		header.append("def getDataCell(value) :\n");
		header.append("    if outColumnType == \"Double\":\n");
		header.append("        return DoubleCell(value)\n");
		header.append("    elif outColumnType == \"Integer\":\n");
		header.append("        return IntCell(value)\n");
		header.append("    else:\n");
		header.append("        return StringCell(value)\n");
		header.append("\n");
		header.append("iterator = inData0.iterator()\n");
		header.append("while iterator.hasNext():\n");
		header.append("    row = iterator.next()\n");
		header.append("    newCell = getDataCell(");
		scriptHeader = header.toString();
		
		
		StringBuffer footer = new StringBuffer();
		footer.append(")\n");
		footer.append("    newRow = AppendedColumnRow(row, [newCell])\n");
		footer.append("    outContainer.addRowToTable(newRow)");
		scriptFooter = footer.toString();
		
		// initialize the script/function contents
		script = "";
	}
	
	/**
	 * @see org.knime.core.node.NodeModel
	 *      #configure(org.knime.core.data.DataTableSpec[])
	 */
	protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
			throws InvalidSettingsException
	{
		if (script == null || "".equals(script)) {
			throw new InvalidSettingsException("Please specify a JPython function.");
		}
		
		if (columnNames == null || columnNames.length == 0) {
			throw new InvalidSettingsException("Please specify an output column name.");
		}
		
        return super.configure(inSpecs);
	}
}
