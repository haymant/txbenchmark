# benchmark

## Table of content

* [What is it?](#what-is-it)
* [Why?](#why)
* [How to build](#how-to-build)
* [Maven Profiles](#maven-profiles)
    * [Integration tests](#integration-tests)
* [How to run it](#how-to-run-it)
* [How to check the results](#how-to-check-the-results)

## What is it?

This tool will perform a benchmark (see below for more details) against a PostgreSQL or MongoDB database.


## Why?

MongoDB announced as one of the main features for version 4.0, if not the main one, 
support for multi-document ACID transactions. The goal of this benchmark is to 
compare an ACID transactional system by default, PostgreSQL, with MongoDB 4.0 
using also transactions.

Given that MongoDB’s support for transactions is quite recent, there are no 
benchmarks ready to exercise this capability. The OnGres team exercised patching the 
sysbench benchmark using for the OLTP benchmark adding support with transactions. 
But the effort was not successful, probably due to limitations in the driver used by this 
benchmark.

To support this analysis, a new benchmark was created from scratch, and its 
published as open source on this repository. It has been developed in Java with the 
idea to elaborate on a test/benchmark already proposed by MongoDB. In particular, it 
was modeled a similar scenario to the one proposed in [Introduction to MongoDB 
Transactions in Python](https://www.mongodb.com/blog/post/introduction-to-mongodb-transactions-in-python),
which lead to the creation of the  this software [pymongo-transactions](https://github.com/jdrumgoole/pymongo-transactions).

This benchmark simulates users buying airline tickets, and generating the appropriate 
records. Instead of fully synthetic data, some real data (see 1) was used based on the one 
available on the [LSV](http://www.lsv.fr/~sirangel/teaching/dataset/index.html) site. 
This makes the benchmark more likely to represent real-world scenarios.
It uses the most popular Java drivers for MongoDB and PostgreSQL - [mongo-java-driver][1]
 and [PgJDBC](https://github.com/pgjdbc/pgjdbc), respectively. The code for the actual
 tests lives in two files, [MongoFlightBenchmark.java][2] and [PostgresFlightBenchmark.java][3].
 Both databases are generated using custom scripts and the static data (flight schedules 
 and airplane information) is preloaded automatically, before tests are run.

1. The original benchmark generated very simple data. In particular, the flight number was [hard-coded
to a constant value][4] and the seats assigned were purely random. For the benchmark that was developed,
a separate table (or collection in MongoDB) was used to load real data from the LSV site containing 
flight data, and another one with plane data. Data is still very small (15K rows for the flight schedules,
and 200 rows for the planes data).

[1]: https://mongodb.github.io/mongo-java-driver
[2]: https://gitlab.com/ongresinc/devel/benchmark/blob/master/cli/src/main/java/com/ongres/benchmark/MongoFlightBenchmark.java
[3]: https://gitlab.com/ongresinc/devel/benchmark/blob/master/cli/src/main/java/com/ongres/benchmark/PostgresFlightBenchmark.java
[4]: https://github.com/jdrumgoole/pymongo-transactions/blob/f73a1b366ff78aed13c870ee2e15ec87be6307ef/transaction_main.py#L70

## How to build

Java 8 JDK and Maven are required to build this project (you can replace all `mvn` commands with `./mvnw` that will launch a provided wrapper that will download and install Maven for ease of use).

Run following command:

```
mvn clean package
```

The command will compile source code and generate an uber-JAR archive in ```cli/target/benchmark-<version>.jar``

## Maven Profiles

- Safer: Slower but safer profile used to look for errors before pushing to SCM 

```
mvn verify -P safer
```

### Integration tests

The integration test suite requires that Docker is installed on the system and available to the user. 
To launch the integrations tests run the following command:

```
mvn verify -P integration
```

To run integration tests with Java debugging enabled on port 8000:

```
mvn verify -P integration -Dmaven.failsafe.debug="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000"
```

## How to run it

Go to the root folder of the project and run the following commands:

```
java -jar cli/target/benchmark-<version>.jar -h
```
The main options are:  
- --benchmark-target: Can be `mongo` or `postgres`
- --target-database-host hostname (or ip address) of the database host
- --min-connections: Minimum amount of connections to keep 
- --max-connections: Maximum amount of connections available
- --duration: Length (in seconds) of the test.
- --metrics: Interval to show accumulated metrics
- --day-range: Integer. When running with high parallellism, a lower number of `day-range` will make _collisions_ of request more likely

Use with `--help` to get a list of all the available options.

## How to check the results
Once execution is over, three files emerges as a result:
- iterations.csv
- response-time.csv
- retries.csv

_Iterations.csv_ shows the number of movements of each interval  
_Response-time.csv_ show some statistic data about execution times  
_Retries.csv_ shows the total transaction retries for each interval (in the case of PostgreSQL, this only shows when used with `--sql-isolation-level=SERIALIZABLE`)