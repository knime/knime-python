#!/bin/bash

# Create the Python environment if required
if [[ -n $KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT ]]; then
	prefPath="${WORKSPACE}/workflow-tests/preferences-Linux.epf"

	if [[ $KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT = "bundled" ]]; then
		# Use the generic preferences file for bundled environment
		cp "${WORKSPACE}/workflow-tests/preferences-bundled-env.epf" "${WORKSPACE}/workflow-tests/preferences-Linux.epf"
		cat "${WORKSPACE}/workflow-tests/preferences-Linux.epf"
	else
		envPath="${WORKSPACE}/python_test_environment"
		echo "Creating Conda environment for: ${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT} at ${envPath}"

		micromamba create \
			-p ${envPath} \
			-f ${WORKSPACE}/workflow-tests/${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT}
		micromamba list -p ${envPath}

		sedi "s|<placeholder_for_env_path>|${envPath}|g" "${prefPath}"
		cat "${prefPath}"

		# Configure Python extension environment for testing extension
		ext_config="${WORKSPACE}/workflow-tests/python-test-ext-config.yaml"
		sedi "s|<placeholder_for_env_path>|${envPath}|g" "${ext_config}"
		echo "-Dknime.python.extension.config=${ext_config}" >> "${WORKSPACE}/workflow-tests/vmargs"
	fi
fi