#!/bin/bash
# install required lib python3.7 and gcc
yum -y install gcc python3-devel python3
# create the virtual environment
python3.7 -m venv wheel-env
# activate the virtual environment
source wheel-env/bin/activate
# install wheel package for creating wheel files
pip install wheel
# create folder for package and cache
mkdir wheelhouse cache
# run pip command on cache location
cd cache
for f in $(cat ../requirements.txt); do pip wheel $f -w ../wheelhouse; done
cd ..
# create the index.html file
cd wheelhouse
INDEXFILE="<html><head><title>Links</title></head><body><h1>Links</h1>"
for f in *.whl; do INDEXFILE+="<a href='$f'>$f</a><br>"; done
INDEXFILE+="</body></html>"
echo "$INDEXFILE" > index.html
cd ..
# cleanup environment
deactivate
rm -rf cache wheel-env
# exit the docker container
exit

