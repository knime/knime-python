#!/bin/bash

# Create the Python environment for JUnit tests
envYml="${WORKSPACE}/junit-tests/env_py39.yml"
envPath="${WORKSPACE}/env_py39"
echo "Creating Conda environment for: ${envYml} at ${envPath}"

micromamba create \
	-p ${envPath} \
	-f ${envYml}
micromamba list -p ${envPath}

export PYTHON3_EXEC_PATH_LINUX="${envPath}/bin/python"
