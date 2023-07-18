## Sharing for autocompletion

This manual shall help in sharing and testing the most user-facing functionalities of Python extension development to get information about them and have some autocompletion.

### Share

1. Run the `collect_files.py` script with this folder as the working directory
2. `conda build recipe -c conda-forge --output-folder output`
3. Login to some anaconda account which is associated with our channel `knime`
4. `anaconda upload --user KNIME --label nightly {path/to/knime-extension.tar.bz2}`


### Test
1. Create some test environment, e.g.  
   `conda create -n my_autocompletion_test_env knime-python-base knime-extension packaging -c knime -c conda-forge`
2. Select that environment in your editor (VS Code,...)
3. Open some Pytho extension file
4. See whether the information of the classes and methods and attributes are shown and whether you can autocomplete `knext` thingies