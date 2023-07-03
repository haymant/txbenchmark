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

# Postgres setup on Ubuntu

```bash
sudo apt update && sudo apt upgrade
sudo apt install postgresql postgresql-contrib

sudo systemctl start postgresql
sudo systemctl status postgresql
sudo vim /etc/postgresql/15/main/postgresql.conf
#Add row
#listen_addresses = '*'
sudo vim /etc/postgresql/15/main/pg_hba.conf
# update host row
host    all             all             0.0.0.0/0            scram-sha-256
sudo systemctl restart postgresql
sudo systemctl status postgresql
sudo -i -u postgres
#create user bench
createuser --interactive
#create DB for benchmark
createdb -O bench benchmark
psql
#update pwd
alter role bench with password 'bench';
#verify
psql -U bench -h localhost -d benchmark
#Benchmark
java -jar cli/target/benchmark-1.3.jar \
  --benchmark-target postgres --parallelism 20 \
  --target-database-host localhost --target-database-port 5432 \
  --target-database-user bench --target-database-name benchmark \
  --target-database-password bench \
  --duration PT120S --metrics "PT10S" --metrics-reporter csv 
```

# MongoDB setup on Ubuntu

```bash
sudo apt-get install gnupg
curl -fsSL https://pgp.mongodb.com/server-6.0.asc | \
   sudo gpg -o /usr/share/keyrings/mongodb-server-6.0.gpg \
   --dearmor
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-6.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/6.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list
sudo apt update
sudo apt-get install -y mongodb-org
vim /etc/mongod.conf
# configure as replicate set, add
replication:
  replSetName: "rs0"
# start
sudo systemctl start mongod
sudo systemctl status mongod
mongosh
> rs.initiate();
> use benchmark;
> db.createUser( {user: 'bench', pwd: passwordPrompt(), roles: [{role: 'readWrite', db:'benchmark'}]});
# verify
mongosh localhost:27017/benchmark -u bench
java -jar cli/target/benchmark-1.3.jar \
  --benchmark-target mongo --parallelism 20 \
  --target-database-host localhost --target-database-port 27017 \
  --target-database-user bench  --target-database-name benchmark \
  --target-database-password bench \
  --duration PT120S --metrics "PT10S" --metrics-reporter csv
```

# Default Results

## Postgres
```csv
t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit
1688279352,161970,44.826623,1.225440,0.614400,0.722449,1.138687,1.294335,1.769471,2.162687,2.555903,10.485759,16025.673308,14764.948266,14576.823668,14544.232091,calls/second,milliseconds
1688279362,341513,44.826623,1.166361,0.614400,0.521964,1.114111,1.261567,1.572863,1.892351,2.195455,7.176191,16979.038995,15252.301944,14687.081129,14581.754676,calls/second,milliseconds
1688279387,158668,45.875199,1.250408,0.647168,0.734829,1.163263,1.310719,1.892351,2.293759,2.654207,9.764863,15627.208292,14042.055100,13800.854755,13759.068090,calls/second,milliseconds
1688279397,334419,45.875199,1.191024,0.638976,0.531723,1.146879,1.277951,1.646591,2.007039,2.310143,6.750207,16588.006089,14577.248720,13922.944381,13800.673275,calls/second,milliseconds
1688279407,510619,45.875199,1.171350,0.630784,0.450188,1.130495,1.269759,1.564671,1.875967,2.162687,5.439487,16928.041906,15042.734258,14043.809105,13842.762476,calls/second,milliseconds
1688279417,689934,45.875199,1.156516,0.610304,0.439065,1.122303,1.261567,1.515519,1.777663,2.072575,5.111807,17176.445983,15484.546124,14170.857891,13887.801285,calls/second,milliseconds
1688279427,869162,45.875199,1.147923,0.610304,0.432105,1.114111,1.253375,1.490943,1.720319,2.007039,4.947967,17324.266303,15857.981899,14293.689806,13932.329628,calls/second,milliseconds
1688279437,1047600,45.875199,1.143131,0.610304,0.427443,1.105919,1.245183,1.474559,1.687551,1.966079,4.882431,17409.735306,16164.521002,14410.434563,13975.670401,calls/second,milliseconds
1688279447,1224405,45.875199,1.141261,0.610304,0.434036,1.105919,1.245183,1.466367,1.662975,1.933311,4.882431,17447.532441,16395.438848,14517.108304,14016.420145,calls/second,milliseconds
1688279457,1401545,45.875199,1.139577,0.610304,0.430220,1.105919,1.245183,1.458175,1.638399,1.900543,4.882431,17480.247317,16597.693798,14621.863054,14057.258618,calls/second,milliseconds
1688279467,1579388,45.875199,1.137768,0.610304,0.410693,1.105919,1.245183,1.449983,1.630207,1.867775,4.751359,17513.481692,16780.075135,14725.596485,14098.460572,calls/second,milliseconds
1688279477,1757558,45.875199,1.136109,0.610304,0.395202,1.105919,1.245183,1.449983,1.613823,1.843199,4.653055,17543.362421,16937.927029,14826.643818,14139.446669,calls/second,milliseconds
1688279487,1935746,45.875199,1.134747,0.610304,0.381283,1.105919,1.245183,1.441791,1.605631,1.818623,4.587519,17568.000518,17072.109279,14924.489044,14180.016622,calls/second,milliseconds
1688279497,2113219,45.875199,1.133993,0.610304,0.369471,1.105919,1.245183,1.441791,1.597439,1.802239,4.489215,17582.627104,17175.856215,15017.072474,14219.448598,calls/second,milliseconds
1688279497,2113752,45.875199,1.134115,0.000480,0.371508,1.105919,1.245183,1.441791,1.597439,1.802239,4.489215,17580.538051,17175.856215,15017.072474,14219.448598,calls/second,milliseconds

```

## MongoDB
```csv
t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit
1688028152,83564,93.847551,8.605054,1.507328,4.237073,7.962623,11.272191,16.383999,18.350079,19.660799,25.165823,1118.948139,798.056598,232.890275,83.536558,calls/second,milliseconds
1688028162,98312,93.847551,8.534340,1.507328,4.122899,7.995391,11.141119,16.056319,18.087935,19.267583,25.034751,1160.999950,899.060776,272.917686,98.673891,calls/second,milliseconds
1688028172,111554,93.847551,8.596526,1.507328,4.106671,8.028159,11.272191,15.990783,17.956863,19.136511,24.903679,1178.240018,974.693221,309.632588,112.975779,calls/second,milliseconds
1688028182,123884,93.847551,8.709046,1.507328,4.145169,8.126463,11.468799,16.056319,17.956863,19.136511,25.034751,1183.460967,1018.935716,340.897415,125.687138,calls/second,milliseconds
1688028192,135211,93.847551,8.866580,1.507328,4.239047,8.290303,11.796479,16.383999,18.219007,19.398655,25.296895,1179.036216,1043.708846,368.453051,137.354641,calls/second,milliseconds
1688028202,146335,93.847551,9.012754,1.507328,4.332239,8.454143,11.993087,16.908287,18.612223,19.791871,25.296895,1173.690199,1055.373572,393.073932,148.206075,calls/second,milliseconds
1688028212,156916,93.847551,9.169330,1.507328,4.450139,8.650751,12.124159,17.170431,19.005439,20.185087,25.821183,1165.109385,1057.710512,415.298965,158.403322,calls/second,milliseconds
1688039392,18082,50.855935,6.599511,1.703936,2.479909,6.127615,8.126463,10.485759,11.403263,12.255231,23.986175,755.545084,188.253636,39.429738,13.248182,calls/second,milliseconds
1688039402,31464,50.855935,7.604742,1.703936,3.165932,7.077887,9.764863,13.107199,14.352383,15.335423,23.855103,927.155648,381.268618,85.586090,29.095474,calls/second,milliseconds
1688039412,43500,76.021759,8.256439,1.703936,3.654203,7.733247,10.747903,14.811135,16.252927,17.170431,23.986175,990.168960,511.202522,123.078487,42.359139,calls/second,milliseconds
1688039422,54437,76.021759,8.800824,1.703936,4.048456,8.159231,11.534335,16.056319,17.825791,19.005439,25.427967,1009.325509,608.115221,156.556663,54.537941,calls/second,milliseconds
1688039432,65211,76.021759,9.186566,1.703936,4.314469,8.585215,12.058623,17.039359,18.874367,20.054015,25.821183,1019.979069,680.547210,186.806667,65.859564,calls/second,milliseconds
1688039442,75431,76.021759,9.531852,1.703936,4.550610,8.912895,12.713983,17.956863,19.660799,20.971519,26.345471,1020.249002,737.703838,215.218670,76.773164,calls/second,milliseconds
1688039452,89240,144.703487,9.402423,1.703936,4.667189,8.847359,12.386303,17.432575,19.398655,20.709375,26.345471,1063.196967,815.788352,248.819860,89.617710,calls/second,milliseconds
1688039462,101943,144.703487,9.407135,1.703936,4.593573,8.912895,12.386303,17.301503,19.136511,20.447231,26.083327,1085.274400,893.277322,283.988082,103.230815,calls/second,milliseconds
1688039472,114077,144.703487,9.458275,1.703936,4.562750,8.978431,12.582911,17.170431,19.005439,20.316159,25.690111,1097.585675,943.491553,314.697589,115.579201,calls/second,milliseconds
1688039482,125547,144.703487,9.549466,1.662976,4.576249,9.043967,12.713983,17.301503,19.005439,20.316159,25.559039,1101.958667,980.011100,343.137537,127.366400,calls/second,milliseconds
1688039492,136503,144.703487,9.661528,1.662976,4.622237,9.109503,12.976127,17.563647,19.136511,20.316159,25.690111,1101.445275,999.680264,368.234567,138.210368,calls/second,milliseconds
1688039502,146755,144.703487,9.804337,1.662976,4.708542,9.306111,13.041663,17.956863,19.660799,20.840447,25.952255,1095.751983,1007.795549,390.696580,148.324288,calls/second,milliseconds
1688273498,131606,37.224447,0.903519,0.585728,0.508420,0.839679,0.868351,1.212415,1.433599,1.703935,6.258687,13004.416121,12109.018445,11975.415730,11952.269776,calls/second,milliseconds
1688273508,268662,37.224447,0.888751,0.581632,0.420196,0.839679,0.868351,1.122303,1.310719,1.548287,5.406719,13348.822759,12352.999393,12031.822426,11971.532811,calls/second,milliseconds
1688273518,406601,37.224447,0.882035,0.581632,0.391606,0.839679,0.868351,1.097727,1.261567,1.466367,4.849663,13494.819781,12573.704995,12089.475156,11991.630916,calls/second,milliseconds
1688273528,546261,37.224447,0.875935,0.581632,0.379397,0.839679,0.864255,1.064959,1.204223,1.376255,4.554751,13611.138086,12787.047119,12150.853960,12013.395361,calls/second,milliseconds
1688273538,684139,37.224447,0.874591,0.581632,0.361637,0.839679,0.868351,1.064959,1.187839,1.343487,4.489215,13645.470867,12939.837246,12204.333904,12032.939009,calls/second,milliseconds
1688273548,823542,37.224447,0.872086,0.581632,0.351005,0.839679,0.864255,1.044479,1.163263,1.302527,4.358143,13693.846952,13093.841443,12261.261433,12054.015643,calls/second,milliseconds

```

# Tunning

## Postgres

```bash
sudo sysctl vm.swappiness=10
sudo sysctl vm.dirty_expire_centisecs=500
sudo sysctl vm.dirty_writeback_centisecs=250
sudo sysctl vm.dirty_ratio=10
sudo sysctl vm.dirty_background_ratio=3
sudo sysctl vm.overcommit_memory=0
sudo sysctl net.ipv4.tcp_timestamps=0
sudo systemctl restart postgresql
```

## MongoDB

```bash
sudo sysctl -w kernel.pid_max=64000
sudo sysctl -w kernel.threads-max=64000
sudo sysctl -w vm.max_map_count=128000
sudo sysctl -w net.core.somaxconn=65535
sudo systemctl restart mongod
```

# Queries

## Simple

### Postgres

```sql
select * from ord inner join customer on (customer.customer_id = ord.customer_id)
  inner join sales on (sales.sales_id = ord.sales_id) 
  inner join product on (product.product_id = ord.product_id) limit 100;
```

### Mongo

```json
[
  {
    "$lookup":{
      "from":"product",
      "let":{
        "pId":"$product_id",
        "cId":"$customer_id",
        "sId":"$sales_id"
      },
      "pipeline":[
        {
          "$match":{
            "$expr":{
              "$and":[
                {
                  "$eq":[
                    "$product_id",
                    "$$pId"
                  ]
                }
              ]
            }
          }
        },
        {
          "$project":{
            "product_name":1,
            "_id":0
          }
        }
      ],
      "as":"prod"
    }
  },
  {
    "$lookup":{
      "from":"customer",
      "let":{
        "pId":"$product_id",
        "cId":"$customer_id",
        "sId":"$sales_id"
      },
      "pipeline":[
        {
          "$match":{
            "$expr":{
              "$and":[
                {
                  "$eq":[
                    "$customer_id",
                    "$$cId"
                  ]
                }
              ]
            }
          }
        },
        {
          "$project":{
            "first_name":1,
            "_id":0
          }
        }
      ],
      "as":"cust"
    }
  },
  {
    "$lookup":{
      "from":"sales",
      "let":{
        "pId":"$product_id",
        "cId":"$customer_id",
        "sId":"$sales_id"
      },
      "pipeline":[
        {
          "$match":{
            "$expr":{
              "$and":[
                {
                  "$eq":[
                    "$sales_id",
                    "$$sId"
                  ]
                }
              ]
            }
          }
        },
        {
          "$project":{
            "first_name":1,
            "_id":0
          }
        }
      ],
      "as":"sale"
    }
  },
  {
    "$limit":100
  }
]
```

## Group By

### Postgres

```sql
select customer.first_name as customer, product.product_name as product, sales.first_name as sales, 
  sum(ord.price) as px, sum(ord.quantity) as amount, sum(ord.beta) as beta, 
  sum(ord.gamma) as gamma, sum(ord.theta) as theta, sum(ord.vega) as vega, sum(ord.vanna) as vanna
  from ord    inner join customer on (customer.customer_id = ord.customer_id) 
  inner join sales on (sales.sales_id = ord.sales_id)             
  inner join product on (product.product_id = ord.product_id)     
  group by customer.first_name, product.product_name, sales.first_name
  order by customer, product, sales;
```

### Mongo

```json
[
  {
    "$lookup":{
      "from":"product",
      "localField":"product_id",
      "foreignField":"product_id",
      "as":"prod"
    }
  },
  {
    "$unwind":"$prod"
  },
  {
    "$lookup":{
      "from":"customer",
      "localField":"customer_id",
      "foreignField":"customer_id",
      "as":"cust"
    }
  },
  {
    "$unwind":"$cust"
  },
  {
    "$lookup":{
      "from":"sales",
      "localField":"sales_id",
      "foreignField":"sales_id",
      "as":"sale"
    }
  },
  {
    "$unwind":"$sale"
  },
  {
    "$project":{
      "_id":1,
      "customer":"$cust.first_name",
      "product":"$prod.product_name",
      "sales":"$sale.first_name",
      "price":1,
      "amount":1,
      "beta":1,
      "gamma":1,
      "theta":1,
      "vega":1,
      "vanna":1
    }
  },
  {
    "$group":{
      "_id":{
        "custo":"$customer",
        "produ":"$product",
        "sal":"$sales"
      },
      "beta":{
        "$sum":"$beta"
      }
    }
  },
  {
    "$sort":{
      "_id.custo":1,
      "_id.produ":1,
      "_id.sal":1
    }
  }
]

```

## GroupSet

### Postgres

```sql
select customer.first_name as customer, product.product_name as product, sales.first_name as sales, 
  sum(ord.price) as px, sum(ord.quantity) as amount, sum(ord.beta) as beta, 
  sum(ord.gamma) as gamma, sum(ord.theta) as theta, sum(ord.vega) as vega, sum(ord.vanna) as vanna
  from ord    inner join customer on (customer.customer_id = ord.customer_id) 
  inner join sales on (sales.sales_id = ord.sales_id)             
  inner join product on (product.product_id = ord.product_id)     
  group by grouping sets (customer.first_name, product.product_name, sales.first_name);
```

### Mongo

```json
[{"$lookup":{"from":"product","localField":"product_id","foreignField":"product_id","as":"prod"}},{"$unwind":"$prod"},{"$lookup":{"from":"customer","localField":"customer_id","foreignField":"customer_id","as":"cust"}},{"$unwind":"$cust"},{"$lookup":{"from":"sales","localField":"sales_id","foreignField":"sales_id","as":"sale"}},{"$unwind":"$sale"},{"$project":{"_id":1,"customer":"$cust.first_name","product":"$prod.product_name","sales":"$sale.first_name","price":1,"amount":1,"beta":1,"gamma":1,"theta":1,"vega":1,"vanna":1}},{"$group":{"_id":{"custo":"$customer","produ":"$product","sal":"$sales"}}}]);

```

## Rollup

### Postgres
```sql
select customer.first_name as customer, product.product_name as product, sales.first_name as sales, 
  sum(ord.price) as px, sum(ord.quantity) as amount, sum(ord.beta) as beta, 
  sum(ord.gamma) as gamma, sum(ord.theta) as theta, sum(ord.vega) as vega, sum(ord.vanna) as vanna
  from ord    inner join customer on (customer.customer_id = ord.customer_id) 
  inner join sales on (sales.sales_id = ord.sales_id)             
  inner join product on (product.product_id = ord.product_id)     
  group by rollup (customer.first_name, product.product_name, sales.first_name);
```

### Mongo
```json
```

## Cube

### Postgres
```sql
select customer.first_name as customer, product.product_name as product, sales.first_name as sales, 
  sum(ord.price) as px, sum(ord.quantity) as amount, sum(ord.beta) as beta, 
  sum(ord.gamma) as gamma, sum(ord.theta) as theta, sum(ord.vega) as vega, sum(ord.vanna) as vanna
  from ord    inner join customer on (customer.customer_id = ord.customer_id) 
  inner join sales on (sales.sales_id = ord.sales_id)             
  inner join product on (product.product_id = ord.product_id)     
  group by cube (customer.first_name, product.product_name, sales.first_name);
```

### Mongo
```json
```

## Pivot

### Postgres
```sql
select * from crosstab('select product.product_name as product, sales.first_name as sales, 
 ord.quantity as amount from ord inner join sales on (sales.sales_id = ord.sales_id) 
 inner join product on (product.product_id = ord.product_id) order by 1, 2') as 
 sales (product text, sales1 real, sales2 real, sales3 real);
```

### Mongo
```json
```
