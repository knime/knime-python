#!/bin/bash

set -x
cd ~/python
if [[ ! -d ~/python/anaconda ]]; then
        if [[ "$(uname -s)" == "Darwin" ]]; then
                INST_FILE=Anaconda3-4.4.0-MacOSX-x86_64.sh
        else
                INST_FILE=Anaconda3-4.4.0-Linux-x86_64.sh
        fi

        wget -qN https://repo.continuum.io/archive/$INST_FILE
        bash $INST_FILE -b -p ~/python/anaconda
        rm $INST_FILE
fi

export PATH=~/python/anaconda/bin:$PATH

conda env remove -n py36_knime -q -y || true
conda env remove -n py27_knime -q -y || true

conda env remove -n py3_knime -q -y || true
conda env remove -n py2_knime -q -y || true

conda env create -f py3_knime.yml -q
conda env create -f py2_knime.yml -q
