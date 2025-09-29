#!/bin/bash

# Create the Python environment if required
if [[ -n $KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT ]]; then
	prefPath="${WORKSPACE}/workflow-tests/preferences-Windows.epf"

	# Create temporary directory
	export KNIME_WORKFLOWTEST_TMP_DIR="$TEMP/knime_temp"
	mkdir -p "${KNIME_WORKFLOWTEST_TMP_DIR}"
	echo "-Dknime.tmpdir=$(path "$KNIME_WORKFLOWTEST_TMP_DIR")" >> "$KNIME_INI"

	if [[ $KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT = "bundled" ]]; then
		# Use the generic preferences file for bundled environment
		cp "${WORKSPACE}/workflow-tests/preferences-bundled-env.epf" "${WORKSPACE}/workflow-tests/preferences-Windows.epf"
		cat "${WORKSPACE}/workflow-tests/preferences-Windows.epf"
	else
	  # remove extension substring from name
	  envName=${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT%".yml"}

		envPath="${WORKSPACE}\\\\${envName}"
		echo "Creating Conda environment for: ${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT} at ${envPath}"

		cmd /c C:/tools/micromamba.exe create \
			-p ${envPath} \
			-f ${WORKSPACE}\\workflow-tests\\${KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT}
		cmd /c C:/tools/micromamba.exe list -p ${envPath}

		sedi "s|<placeholder_for_env_path>|${envPath//\\/\\\\\\\\}|g" "${prefPath}"
		cat "${prefPath}"

		# Configure environment for Python-based nodes testing extension (py36 is not supported)
		if [[ $KNIME_WORKFLOWTEST_PYTHON_ENVIRONMENT != env_py36* ]]; then
			ext_config="${WORKSPACE}/workflow-tests/python-test-ext-config.yaml"
			sedi "s|<placeholder_for_env_path>|${envPath}|g" "${ext_config}"
			echo "-Dknime.python.extension.config=${ext_config}" >> "${WORKSPACE}/workflow-tests/vmargs"
		else
			echo "Python 3.6 is not supported to run workflow-tests"
			exit 1
		fi
	fi

	# Test debug_knime_yaml_list argument with test extension
	test_extension="${WORKSPACE}/workflow-tests/test-extension/knime.yml"

	# Run pixi install in the test extension directory
	echo "Setting up pixi environment for test extension..."
	cd "${WORKSPACE}/workflow-tests/test-extension"
	pixi install --frozen
	cd "${WORKSPACE}"
	echo "-Dknime.python.extension.debug_knime_yaml_list=${test_extension}" >> "${WORKSPACE}/workflow-tests/vmargs"
fi
