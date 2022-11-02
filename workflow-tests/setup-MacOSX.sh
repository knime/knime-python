#!/bin/bash

# set python version if required
if [[ -n $KNIME_WORKFLOWTEST_PYTHON_VERSION ]]; then
	echo "Setting python version to: ${KNIME_WORKFLOWTEST_PYTHON_VERSION}"

	preferencesPath="${WORKSPACE}/workflow-tests/preferences-MacOSX.epf"
	envPath="/Users/jenkins/miniconda3/envs/knime_py${KNIME_WORKFLOWTEST_PYTHON_VERSION}"
	py3EnvPrefKey='python3CondaEnvironmentDirectoryPath='

	sedi "s|${py3EnvPrefKey}.*|${py3EnvPrefKey}${envPath}|g" "${preferencesPath}"

	cat "${preferencesPath}"
fi
