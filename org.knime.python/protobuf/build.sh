#!/bin/bash 
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd $SCRIPTPATH
protoc --java_out=../src *.proto
protoc --python_out=../py *.proto

