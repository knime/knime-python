#!/bin/bash

# Create the Python environment if required
if [[ -n $KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT ]]; then
	envPath="${WORKSPACE}/python_test_environment"
	prefPath="${WORKSPACE}/workflow-tests/preferences-MacOSX.epf"
	echo "Creating Conda environment for: ${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT} at ${envPath}"

	# TODO(DEVOPS-1649) use micromamba
	conda env create \
		-p ${envPath} \
		-f ${WORKSPACE}/workflow-tests/${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT}
	conda list -p ${envPath}

	sedi "s|<placeholder_for_env_path>|${envPath}|g" "${prefPath}"
	cat "${prefPath}"
fi
