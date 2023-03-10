#!/bin/bash

# Create the Python environment if required
if [[ -n $KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT ]]; then
	envPath="${WORKSPACE}\\\\python_test_environment"
	prefPath="${WORKSPACE}/workflow-tests/preferences-Windows.epf"
	echo "Creating Conda environment for: ${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT} at ${envPath}"

	cmd /c micromamba.exe create \
		-p ${envPath} \
		-f ${WORKSPACE}\\workflow-tests\\${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT}
	cmd /c micromamba.exe list -p ${envPath}

	sedi "s|<placeholder_for_env_path>|${envPath//\\/\\\\\\\\}|g" "${prefPath}"
	cat "${prefPath}"
fi
