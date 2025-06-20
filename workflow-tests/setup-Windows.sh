#!/bin/bash

# Create the Python environment if required
if [[ -n $KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT ]]; then
	prefPath="${WORKSPACE}/workflow-tests/preferences-Windows.epf"

	if [[ $KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT = "bundled" ]]; then
		# Use the generic preferences file for bundled environment
		cp "${WORKSPACE}/workflow-tests/preferences-bundled-env.epf" "${WORKSPACE}/workflow-tests/preferences-Windows.epf"
		cat "${WORKSPACE}/workflow-tests/preferences-Windows.epf"
	else
		envPath="${WORKSPACE}\\\\python_test_environment"
		echo "Creating Conda environment for: ${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT} at ${envPath}"

		cmd /c C:/tools/micromamba.exe create \
			-p ${envPath} \
			-f ${WORKSPACE}\\workflow-tests\\${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT}
		cmd /c C:/tools/micromamba.exe list -p ${envPath}

		sedi "s|<placeholder_for_env_path>|${envPath//\\/\\\\\\\\}|g" "${prefPath}"
		cat "${prefPath}"
	fi
fi
