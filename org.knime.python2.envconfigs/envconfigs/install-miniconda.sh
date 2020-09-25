#!/bin/bash

set -x

if [[ ! -d ~/miniconda3 ]]; then
    if [[ "$(uname -s)" == "Darwin" ]]; then
        INST_FILE=Miniconda3-latest-MacOSX-x86_64.sh
		ENV_PATH=macosx/test
    else
        INST_FILE=Miniconda3-latest-Linux-x86_64.sh
		ENV_PATH=macosx/linux
    fi

    wget -qN https://repo.anaconda.com/miniconda/$INST_FILE
    bash $INST_FILE -b -p ~/miniconda3
    rm $INST_FILE
fi

export PATH=~/miniconda3/bin:$PATH

conda env remove -n py3_knime -q -y || true
conda env remove -n py3_knime43 -q -y || true
conda env remove -n py2_knime -q -y || true
conda env remove -n py2_knime43 -q -y || true

conda env create -f $ENV_PATH/py3_knime.yml -q
conda env create -f $ENV_PATH/py3_knime43.yml -q
conda env create -f $ENV_PATH/py2_knime.yml -q
conda env create -f $ENV_PATH/py2_knime43.yml -q
