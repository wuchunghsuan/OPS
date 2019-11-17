#!/bin/bash
cd ../
mvn package
cd ./scripts
./ops.sh worker start
