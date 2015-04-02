package org.knime.python.kernel;

public interface EditorObjectWriter {

	public String getInputVariableName();

	public byte[] getMessage() throws Exception;

}
