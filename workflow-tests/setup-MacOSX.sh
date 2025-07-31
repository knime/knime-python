#!/bin/bash

# Create the Python environment if required
if [[ -n $KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT ]]; then
	prefPath="${WORKSPACE}/workflow-tests/preferences-MacOSX.epf"

	if [[ $KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT = "bundled" ]]; then
		# Use the generic preferences file for bundled environment
		cp "${WORKSPACE}/workflow-tests/preferences-bundled-env.epf" "${WORKSPACE}/workflow-tests/preferences-MacOSX.epf"
		cat "${WORKSPACE}/workflow-tests/preferences-MacOSX.epf"
	else
		envPath="${WORKSPACE}/python_test_environment"
		echo "Creating Conda environment for: ${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT} at ${envPath}"

		# Disable code signing so that micromamba can run without issues on macOS-aarch64
		export LIBMAMBA_DISABLE_CODE_SIGNING=1
		export MAMBA_NO_CODE_SIGNING=1

		micromamba create \
			-r ${WORKSPACE}/micromamba-root \
			-p ${envPath} \
			-f ${WORKSPACE}/workflow-tests/${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT} \
			--no-code-signing
		micromamba list -p ${envPath}

		sedi "s|<placeholder_for_env_path>|${envPath}|g" "${prefPath}"
		cat "${prefPath}"
	fi
fi
