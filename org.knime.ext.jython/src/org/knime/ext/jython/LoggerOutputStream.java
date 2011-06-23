package org.knime.ext.jython;

import java.io.ByteArrayOutputStream;
import org.knime.core.node.NodeLogger;

public class LoggerOutputStream extends ByteArrayOutputStream {

	private NodeLogger logger;
	private NodeLogger.LEVEL level;
	
	public LoggerOutputStream(NodeLogger inLogger, NodeLogger.LEVEL inLevel) {
		this.logger = inLogger;
		this.level = inLevel;
	}
	
	public void write(byte[] b, int off, int len) {
		String str = new String(b, off, len);
		if (level == NodeLogger.LEVEL.INFO) {
			logger.info(str);
			return;
		} else if (level == NodeLogger.LEVEL.ERROR) {
			logger.error(str);
			return;
		}
		
		logger.debug(str);
	}
}
