Avrocount
========================
[![release](https://badge.fury.io/gh/jwoschitz%2Favrocount.svg)](https://github.com/jwoschitz/avrocount/releases/latest)
[![Build Status](https://travis-ci.org/jwoschitz/avrocount.svg?branch=master)](https://travis-ci.org/jwoschitz/avrocount)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

This tool provides a way of efficiently counting records in Apache Avro data files. (https://avro.apache.org/docs/current/)

It works with single files or whole folders, with local filesystem or HDFS.

- [Usage](#usage)
- [Build from source](#build%20from%20source)
- [Motivation](#motivation)

Usage
------------

### Quickstart

Simply invoke the tool via

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


Build from source
------------

This project relies on gradle for dependency management and build automation.

In order to build the project execute:


```
gradle build
```

This will generate an uber-jar (contains all relevant dependencies) in `./build/libs/`

Motivation
----------

The initial idea was submitted as a patch in 2015 to the Apache Avro project (https://issues.apache.org/jira/browse/AVRO-1720) as an addition to the already existing avro-tools.

Though due to several reasons this patch has not been merged yet.

Unfortunately up to this date there is no convenient and efficient way to count records in an Avro data file by using avro-tools from the command line.

This project tries to fill this gap (at least) until a similar functionality is provided by avro-tools.

Over time there were also several improvements to this project in comparison to the original patch.

It would be great if these improvements would also find a way back into the Apache Avro project in the longterm. Until then this project can be used in addition to the currently existing avro-tools.

