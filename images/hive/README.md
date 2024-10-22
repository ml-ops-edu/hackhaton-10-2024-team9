# HIVE IMAGE

## Description

Dockerfile and build shell scripts for Hive 4.0.1 metastore, with mysql jdbc drivers 9.1, Hadoop 3.4, and java 21 LTS from eclipse-temurin..
Alternatively, Dockerfile.slim provides a smaller image based on apache/hive:4.0.1, with Hadoop 3.3.6, and java 8 LTS.

## Build

./build.sh [amd64] [arm64] [-r image-repository]

## Notes

- Hadoop and Hive depends on different versions of guava. Online guides recommend replacing them all with the newest version. However, preliminary tests suggest that this is not required (yet).
- The build copies the hadoop-aws-3.4.0.jar and the aws bundle jar from hadoop to hive (tests fail without them)

## Dependencies check

