/* @(#)$RCSfile$
 * $Revision$ $Date$ $Author$
 */
package org.knime.ext.jython;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.knime.base.data.append.column.AppendedColumnTable;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.osgi.framework.Bundle;
import org.python.core.Options;
import org.python.core.Py;
import org.python.core.PyCode;
import org.python.core.PyException;
import org.python.core.PySystemState;
import org.python.core.__builtin__;
import org.python.util.PythonInterpreter;

/**
 * This is the base implementation of the "JPython Script" node
 *
 * @author Tripos
 */
public class PythonScriptNodeModel extends NodeModel
{
	
	public static final String SCRIPT = "script";
	public static final String APPEND_COLS = "append_columns";
	public static final String COLUMN_NAMES = "new_column_names";
	public static final String COLUMN_TYPES = "new_column_types";
	protected int numInputs = 0;
	protected int numOutputs = 0;

	// our logger instance
	private static NodeLogger logger = NodeLogger.getLogger(PythonScriptNodeModel.class);
	protected String scriptHeader = "";
	protected String scriptFooter = "";
	protected String script = "";
	protected boolean appendCols = true;
	protected String[] columnNames;
	protected String[] columnTypes;
	private static String pluginsRootPath;
	private static String javaExtDirsExtensionsPath;
	private static String javaClasspathExtensionsPath;
	private static String pythoncacheDir;
	
	protected PythonScriptNodeModel(int numInputs, int numOutputs) {
		super(numInputs, numOutputs);
		
		this.numInputs = numInputs;
		this.numOutputs = numOutputs;
		
		// define the common imports string
		StringBuffer buffer = new StringBuffer();
		buffer.append("from org.knime.core.data import *\n");
		buffer.append("from org.knime.core.data.def import *\n");
		buffer.append("from org.knime.base.data.append.column import *\n");
		buffer.append("from org.knime.base import *\n");
		buffer.append("\n");
		buffer.append("def val(colname, rowinstance, inport=0) :\n");
		buffer.append("    __dts = 0\n");
		buffer.append("    if inport == 0:\n");
		buffer.append("        __dts = inData0.getDataTableSpec()\n");
		buffer.append("    else:\n");
		buffer.append("        __dts = inData1.getDataTableSpec()\n");
		buffer.append("\n");
		buffer.append("    index = __dts.findColumnIndex(colname)\n");
		buffer.append("    cellType = __dts.getColumnSpec(colname).getType().toString()\n");
		buffer.append("    cellValue = rowinstance.getCell(index)\n");
	    buffer.append("    stringCellValue = str(cellValue)\n");
	    buffer.append("\n");
		buffer.append("    if cellType == \"DoubleCell\":\n");
		buffer.append("        return float(stringCellValue)\n");
		buffer.append("    elif cellType == \"IntCell\":\n");
		buffer.append("        return int(stringCellValue)\n");
		buffer.append("    else:\n");
		buffer.append("        return stringCellValue\n");
		buffer.append("\n");
		
		scriptHeader = buffer.toString();
		
		buffer = new StringBuffer();
		buffer.append("## Available scripting variables:\n");
		buffer.append("##     inData0 - input DataTable 0\n");
		if (numInputs == 2) {
			buffer.append("##     inData1 - input DataTable 1\n");
		}
		buffer.append("##     outContainer - container housing output DataTable\n");
		buffer.append("##\n");
		buffer.append("## Example starter script:\n");
		buffer.append("##\n");
		buffer.append("## dts = inData0.getDataTableSpec()\n");
		buffer.append("## inputColumnIndex= dts.findColumnIndex(\"input_column_name\")\n");
		buffer.append("##\n");
		buffer.append("## iterator = inData0.iterator()\n");
		buffer.append("## while iterator.hasNext():\n");
		buffer.append("##     row = iterator.next()\n");
		buffer.append("##     cell = row.getCell(inputColumnIndex)\n");
		buffer.append("##\n");
		buffer.append("##     newStringCell = StringCell(\"some string\")\n");
		buffer.append("##     newIntCell = IntCell(100)\n");
		buffer.append("##     newDoubleCell = DoubleCell(99.9)\n");
		buffer.append("##\n");
		buffer.append("##     newRow = AppendedColumnRow(row, [newStringCell, newIntCell, newDoubleCell])\n");
		buffer.append("##     outContainer.addRowToTable(newRow)\n");
		buffer.append("##\n");
		buffer.append("## Default script:\n");
		buffer.append("##\n");
		buffer.append("iterator = inData0.iterator()\n");
		buffer.append("while iterator.hasNext():\n");
		buffer.append("    row = iterator.next()\n");
		buffer.append("    outContainer.addRowToTable(row)\n");

		script = buffer.toString();
	}

	
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
			final ExecutionContext exec) throws CanceledExecutionException,
			Exception
	{		
		BufferedDataTable in = inData[0];
		BufferedDataTable in2 = null;
		if (numInputs == 2) {
			in2 = inData[1];
		}

		// construct the output data table specs and the output containers
		DataTableSpec[] outSpecs = configure(new DataTableSpec[] {in.getDataTableSpec()});
		DataContainer outContainer = new DataContainer(outSpecs[0]);
		DataContainer outContainer2 = null;
		if (numOutputs == 2) {
			outContainer2 = new DataContainer(outSpecs[1]);
		}
	 
		String pathSep = System.getProperty("path.separator");
	    String fileSep = System.getProperty("file.separator");
		
        // construct all necessary paths
        Bundle core = Platform.getBundle("org.knime.core");
        String coreClassPath =
                core.getHeaders().get("Bundle-Classpath").toString();
        String corePluginPath =
                FileLocator
                        .resolve(FileLocator.find(core, new Path("."), null))
                        .getPath();

        Bundle base = Platform.getBundle("org.knime.base");
        String baseClassPath =
                base.getHeaders().get("Bundle-Classpath").toString();
        String basePluginPath =
                FileLocator
                        .resolve(FileLocator.find(base, new Path("."), null))
                        .getPath();

        Bundle python = Platform.getBundle("org.python.plugin");
        String pythonPluginPath =
                FileLocator.resolve(
                        FileLocator.find(python, new Path("."), null))
                        .getPath();

        // set up ext dirs
        StringBuffer ext = new StringBuffer();
        ext.append(basePluginPath + fileSep + "lib");
        ext.append(pathSep);
        ext.append(corePluginPath + fileSep + "lib");
        ext.append(pathSep);
        ext.append(getJavaExtDirsExtensionPath());

        // set up the classpath
        StringBuilder classpath = new StringBuilder();
        for (String s : coreClassPath.split(",")) {
            URL u = FileLocator.find(core, new Path(s), null);
            if (u != null) {
                classpath.append(FileLocator.resolve(u).getFile());
                classpath.append(pathSep);
            }
        }
        // this entry is necessary if KNIME is started from Eclipse SDK
        classpath.append(corePluginPath + fileSep + "bin");
        classpath.append(pathSep);

        for (String s : baseClassPath.split(",")) {
            URL u = FileLocator.find(base, new Path(s), null);
            if (u != null) {
                classpath.append(FileLocator.resolve(u).getFile());
                classpath.append(pathSep);
            }
        }
        // this entry is necessary if KNIME is started from Eclipse SDK
        classpath.append(basePluginPath + fileSep + "bin");
        classpath.append(pathSep);

        classpath.append(getJavaClasspathExtensionPath());
		
		Options.verbose = Py.WARNING;
		// set necessary properties
		Properties props = new Properties();
		props.setProperty("python.home", pythonPluginPath);
		props.setProperty("python.cachedir", pythoncacheDir);
		props.setProperty("java.ext.dirs", ext.toString());
		props.setProperty("java.class.path", classpath.toString());
		props.setProperty("python.packages.path", "java.class.path, sun.boot.class.path");
		props.setProperty("python.packages.directories", "java.ext.dirs, python.path");

		// initialize and invoke the interpreter
		PySystemState.initialize(System.getProperties(), props, new String[] {""}, getClass().getClassLoader());
		PySystemState state = Py.getSystemState();
		PythonInterpreter interpreter = new PythonInterpreter(null, state);
		interpreter.setOut(new LoggerOutputStream(logger, NodeLogger.LEVEL.INFO));
		interpreter.setErr(new LoggerOutputStream(logger, NodeLogger.LEVEL.ERROR));
		
		interpreter.set("inData0", in);
		if (numInputs == 2) {
			interpreter.set("inData1", in2);
		}
		interpreter.set("outContainer", outContainer);
		interpreter.set("outColumnNames", columnNames);
		interpreter.set("outColumnTypes", columnTypes);
		
		exec.setMessage("Executing user python script...");
        try {
            exec.checkCanceled();
        } catch (CanceledExecutionException cee) {
            outContainer.close();
            throw cee;
        }
		try {
			PyCode code = __builtin__.compile(scriptHeader + script + scriptFooter, "<>", "exec");
			interpreter.exec(code);
		} catch (PyException pe) {
//			pe.printStackTrace();
			logger.error(pe.getMessage()); // + "\nFULL SCRIPT:\n" + scriptHeader + script + scriptFooter, pe);
//            setWarningMessage("Jython execution failed: " + pe.value.safeRepr());
			throw new Exception("Jython error (see console for error log).", (Throwable)pe);
		}
        interpreter.cleanup();	
		
		outContainer.close();
		if (outContainer2 != null) {
			outContainer2.close();
		}
		if (numOutputs == 2) {
			return new BufferedDataTable[] {exec.createBufferedDataTable(outContainer.getTable(), exec),
					exec.createBufferedDataTable(outContainer.getTable(), exec)};
		}
		return new BufferedDataTable[] {exec.createBufferedDataTable(outContainer.getTable(), exec) };
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
			throws InvalidSettingsException
	{
		//	append the property columns to the data table spec
        DataTableSpec newSpec = appendCols ? inSpecs[0] : new DataTableSpec();
        
        if (columnNames == null) {
        	return new DataTableSpec[]{newSpec};
        }
        
        for (int i=0; i < columnNames.length; i++) {
        	DataType type = StringCell.TYPE;
        	String columnType = columnTypes[i];
        	
        	if ("String".equals(columnType)) {
        		type = StringCell.TYPE; 
        	} else if ("Integer".equals(columnType)) {
        		type = IntCell.TYPE;
        	} else if ("Double".equals(columnType)) {
        		type = DoubleCell.TYPE;
        	}
        	DataColumnSpec newColumn = 
        		new DataColumnSpecCreator(columnNames[i], type).createSpec();
        	
        	newSpec = AppendedColumnTable.getTableSpec(newSpec, newColumn);
        }
        
        if (script == null) {
        	script = "";
        }
        
        if (numOutputs == 2) {
        	return new DataTableSpec[] {newSpec, newSpec};
        }
        return new DataTableSpec[]{newSpec};
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	protected void reset()
	{
	}

    /**
     * {@inheritDoc}
     */
	@Override
    protected void loadInternals(final File nodeInternDir,
            final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to load.
    }
	
	
    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
        // no internals to save
    }
	
	
	/**
	 * {@inheritDoc}
	 */
	protected void saveSettingsTo(final NodeSettingsWO settings)
	{
        settings.addString(SCRIPT, script);
        settings.addBoolean(APPEND_COLS, appendCols);
        settings.addStringArray(COLUMN_NAMES, columnNames);
        settings.addStringArray(COLUMN_TYPES, columnTypes);
	}

	/**
	 * {@inheritDoc}
	 */
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
			throws InvalidSettingsException
	{
        script = settings.getString(SCRIPT);
        // since 1.3
        appendCols = settings.getBoolean(APPEND_COLS, true);
        columnNames = settings.getStringArray(COLUMN_NAMES);
        columnTypes = settings.getStringArray(COLUMN_TYPES);
	}

	/**
	 * {@inheritDoc}
	 */
	protected void validateSettings(final NodeSettingsRO settings)
			throws InvalidSettingsException
	{
        settings.getString(SCRIPT);
        settings.getBoolean(APPEND_COLS);
        settings.getStringArray(COLUMN_NAMES);
        settings.getStringArray(COLUMN_TYPES);
	}
	
	
	public static void setJavaExtDirsExtensionPath(String path) {
		javaExtDirsExtensionsPath = path;
	}
	
	public static String getJavaExtDirsExtensionPath() {
		return javaExtDirsExtensionsPath;
	}
	
	public static void setJavaClasspathExtensionPath(String path) {
		javaClasspathExtensionsPath = path;
	}
	
	public static void setPythonCacheDir(String path) {
		pythoncacheDir = path;
	}
	
	public static String getJavaClasspathExtensionPath() {
		return javaClasspathExtensionsPath;
	}	
}
