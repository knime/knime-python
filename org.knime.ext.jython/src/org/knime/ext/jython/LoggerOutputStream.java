package org.knime.ext.jython;

import java.io.ByteArrayOutputStream;
import org.knime.core.node.NodeLogger;

public class LoggerOutputStream extends ByteArrayOutputStream {

	private NodeLogger logger;
	private NodeLogger.LEVEL level;
	
	public LoggerOutputStream(NodeLogger logger, NodeLogger.LEVEL level) {
		this.logger = logger;
		this.level = level;
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
