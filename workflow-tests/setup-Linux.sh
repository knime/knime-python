#!/bin/bash

# Create the Python environment if required
if [[ -n $KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT ]]; then
	envPath="${WORKSPACE}/python_test_environment"
	prefPath="${WORKSPACE}/workflow-tests/preferences-Linux.epf"
	echo "Creating Conda environment for: ${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT} at ${envPath}"

	# TODO(DEVOPS-1649) use pre-installed micromamba
	conda install -c conda-forge micromamba
	/home/jenkins/miniconda3/bin/micromamba create \
		-p ${envPath} \
		-f ${WORKSPACE}/workflow-tests/${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT}
	/home/jenkins/miniconda3/bin/micromamba list -p ${envPath}

	sedi "s|<placeholder_for_env_path>|${envPath}|g" "${prefPath}"
	cat "${prefPath}"
fi