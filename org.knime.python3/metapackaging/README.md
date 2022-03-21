# ![Image](https://www.knime.com/files/knime_logo_github_40x40_4layers.png) KNIME Analytics Platform

### Content
This folder contains recipes for Python metapackages for the [KNIME Analytics Platform](http://www.knime.org).

Metapackages are used to have necessary packages for certain aspects available at hand.
For example, `knime-python-base` is used to get the basic packages, such as pyarrow, numpy and pandas.
`knime-python-scripting` gives possibilities to use notebook, autocompletion via jedi and others.

The metapackages are built by the `conda build` command. The corresponding conda environment is created via ´conda create`.


### Usage
The `buildAllMetapackages.sh` gives an example on how to create the metapackages from the recipes shipped in this folder.
**Example:**
1. Go in the terminal to this folder
2. `conda index` 
2. `conda build knime-python-base -c conda-forge --override-channels --output-folder metapackages --yes` (omit the the output param to have the metapackage stored in the local conda path (accessibla via *-c local*); with the output param it is stored in a subfolder (accessible via *-c $PWD/metapackages*))
  a. `conda build knime-python-base --python=3.7 ...` if you want a package only for a specified of the available Python versions
3. `conda create -n baseTestEnv knime-python-base -c local / $PWD/metapackages -c conda-forge --override-channels`
  a. `conda create -n baseTestEnv knime-python-base python=3.7 ...` if you want to specify the environment by yourself instead of letting conda giving you the most recent possibility

If you try to use metapackages from a folder which is not indexed as a local conda channel yet, you might want to use `conda index` in that folder. If the folder has a *channeldata.json* and a *index.html*, it should already be indexed.

### Join the Community!
* [KNIME Forum](https://tech.knime.org/forum)
