#!/bin/bash

# Create the Python environment for JUnit tests
envYml="${WORKSPACE}/junit-tests/env_py39.yml"
envPath="${WORKSPACE}/python_test_environment"
echo "Creating Conda environment for: ${envYml} at ${envPath}"

# TODO(DEVOPS-1649) use pre-installed micromamba
conda install -c conda-forge micromamba
/home/jenkins/miniconda3/bin/micromamba create \
	-p ${envPath} \
	-f ${envYml}
/home/jenkins/miniconda3/bin/micromamba list -p ${envPath}

export PYTHON3_EXEC_PATH_LINUX="${envPath}/bin/python"
