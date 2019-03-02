#! /usr/bin/env bash

# $1: The conda directory.
# $2: The environment name.
# All subsequent arguments are passed to the Python process.

knime_conda_init_script=""$1"/etc/profile.d/conda.sh"
knime_conda_environment="$2"

shift
shift

. $knime_conda_init_script
conda activate "$knime_conda_environment"
python "$@" 1>&1 2>&2
