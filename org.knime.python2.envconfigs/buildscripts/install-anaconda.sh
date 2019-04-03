#!/bin/bash

cd python
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

conda env remove -n py35_knime -q -y
conda env remove -n py27_knime -q -y

conda env create -f py35_knime.yml -q
conda env create -f py27_knime.yml -q

if [[ "$(uname -s)" == "Linux" ]]; then
	source activate py35_knime
	pip install https://cntk.ai/PythonWheel/CPU-Only/cntk-2.2-cp35-cp35m-linux_x86_64.whl
fi

