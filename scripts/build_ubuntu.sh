#!/bin/bash

# Build linkbenchX on Ubuntu 14.x 

##################
# installation
##################

# install dependencies

sudo apt-get -y update
sudo apt-get install -y wget git openjdk-7-jdk maven

# make sure we have everything we need

MISSING=0
function check_if_missing {
  if ! hash $1 2>/dev/null; then
    echo "ERROR: $1 command required for this script."
    MISSING=$((MISSING+1))
  fi
}
for CMD in "sudo" "wget" "git" "java" "javac" "mvn"
do
  check_if_missing $CMD
done
if [ $MISSING \> 0 ]; then
  exit 1;
fi

# download and build linkbenchX

cd ~
git clone https://github.com/Percona-Lab/linkbenchX.git
cd linkbenchX
mvn clean package -DskipTests
