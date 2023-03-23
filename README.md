Avrocount
========================
[![release](https://badge.fury.io/gh/jwoschitz%2Favrocount.svg)](https://github.com/jwoschitz/avrocount/releases/latest)
[![Build Status](https://github.com/jwoschitz/avrocount/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/jwoschitz/avrocount/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

This tool provides a way of efficiently counting records in Apache Avro data files. (https://avro.apache.org/docs/current/)

It works with single files or whole folders, with local filesystem or HDFS.

- [Usage](#usage)
- [Build from source](#build-from-source)
- [Motivation](#motivation)

--------------


| :warning: This repository is partially deprecated   |
|-----------------------------------------|

Since the release of Avro 1.10.0 (released on June 29, 2020), avro-tools provides an integrated way of counting Avro record files.

```
java -jar avro-tools.jar count example.avro
```

The stable version of avro-tools can be downloaded from the [Apache Avro project page](https://avro.apache.org/project/download/).

### Why still use this project?

While avro-tools provides the general ability to count avro files nowadays, it is still lacking the performance improvements which have been implemented in this project.

If you are working with large avro files (above 1 GB per file), using avrocount might provide a substantial performance gain.

See [Motivation](#motivation) and [Benchmark](#benchmark) for more information.

--------------


## Usage

### Quickstart

Get the [latest released version](https://github.com/jwoschitz/avrocount/releases/latest) or [build it from source](#build-from-source).

Then simply invoke the tool via

```
java -jar avrocount.jar /path/to/myfile.avro
```

And it will print the amount of records found within the file to stdout.

### Folders containing avro files

You can either provide a path to a file or a folder containing avro files.

```
java -jar avrocount.jar /path/to/folder
```

The tool will consider only files ending with `.avro` and ignore other files. Currently only files within the directory will be processed, sub-directories will not be considered.

The total amount of records found in all avro files within the folder will be printed to stdout.

### HDFS integration

The tool is using the Hadoop Filesystem API to resolve paths, as long as the proper Hadoop configuration is provided via PATH it should be able to connect to HDFS file paths.

You can execute the tool directly with the yarn binary and it should pick up the necessary configurations automatically.

```
yarn jar avrocount.jar /path/to/myfile.avro
```

In older Hadoop distributions, you need to replace `yarn` with `hadoop`.

Alternatively you can explicitly point to a HDFS instance by specifying the protocol

```
jar -jar avrocount.jar hdfs://<namenode>/path/to/myfile.avro
```


## Build from source

You can also get the already compiled dependencies from the
[latest release](https://github.com/jwoschitz/avrocount/releases/latest).

This project relies on gradle for dependency management and build automation.

In order to build the project execute:


```
gradle build
```

This will generate an uber-jar (contains all relevant dependencies) in `./build/libs/`

## Motivation

In 2015, an initial proposal was submitted as a patch to the Apache Avro project (https://issues.apache.org/jira/browse/AVRO-1720) to enhance the existing avro-tools.

The project emerged when the patch remained unmerged for an extended period, leaving no convenient or efficient method for counting records in Avro data files using avro-tools via the command line.

This project sought to bridge that gap until avro-tools incorporated similar functionality.

The original patch was eventually merged in May 2020, rendering this project partially obsolete since counting records in Avro files became possible.

However, several performance enhancements were introduced to this project, which were not included in the original patch.

These performance improvements have not yet been integrated into avro-tools. As a result, if you work with large Avro files (>1 GB or a significant number of records), you might still benefit from a considerable performance boost using this project.

In the meantime, while working to incorporate the performance enhancements into the Apache Avro project, you can continue using this project as an alternative when handling large Avro files.

## Benchmark

This is a naive benchmark to show the difference in counting performance between `avro-tools count` and `avrocount`.

The benchmark file was created via `avro-tools`. A file with 1000000000 records was created (approximates to a file size of ~4.2 GB).

```
java -jar avro-tools-1.11.1.jar random --count 1000000000 --schema-file src/test/resources/intRecord.avsc --seed 1 test_large.avro
```

Counting via `avro-tools-1.11.1.jar` takes about 32s wall time.
```
time java -jar avro-tools-1.11.1.jar count test_large.avro
1000000000
32.57s user 1.07s system 99% cpu 33.889 total
```

Counting via `avrocount-0.5.0-all.jar` takes only about 1s. This is about 30x faster than with the current implementation in Apache Avro.
```
time java -jar build/libs/avrocount-0.5.0-all.jar test_large.avro
1000000000
1.69s user 1.18s system 46% cpu 6.199 total
```