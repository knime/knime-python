package org.knime.code2.python;

import org.knime.python2.kernel.PythonKernel;

public interface WorkspacePreparer {
	
	void prepareWorkspace(PythonKernel kernel);

}
