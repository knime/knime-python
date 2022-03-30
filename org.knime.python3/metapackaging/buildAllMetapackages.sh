#!/bin/bash
echo "Starting to build meta packages."
conda build purge-all

for i in $(find . -maxdepth 1 -type d)
do
  if [[ "$i" == "." ]] # this root folder should not contain any meta.yaml and thus do not need to be looked at.
  then
    continue
  fi

  conda build ${i##*/} -c conda-forge --override-channels --output-folder metapackages
done

echo "Metapackages created. Cleaning up temporary files."
conda build purge-all
echo "Finished building meta packages!"
