#!/bin/bash
HDR_HISTOGRAM="HdrHistogram"
HDR_HISTOGRAM_JAR="HdrHistogram/target/hdrhistogram-1.0-SNAPSHOT.jar"
HDR_HISTOGRAM_VER="1.0-SNAPSHOT"
IN_PROJECT_MVN_REPO="core/lib"

git submodule update --init
rm -rf $IN_PROJECT_MVN_REPO
cd $HDR_HISTOGRAM
mvn clean package
cd ..
mvn install:install-file -DlocalRepositoryPath=$IN_PROJECT_MVN_REPO -DcreateChecksum=true -Dpackaging=jar -Dfile=$HDR_HISTOGRAM_JAR -DgroupId=org.hdrhistogram -DartifactId=hdrhistogram -Dversion=$HDR_HISTOGRAM_VER
mvn clean package
