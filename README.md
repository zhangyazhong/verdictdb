# Verdict

Verdict makes database users able to get fast, approximate results for their aggregate queries on big data. It is designed as a middleware that stands between user's application and DBMS.
Verdict gets the original query from user, transforms it to run on samples and calculate error estimate as well as approximate query answer. Then it sends the new query to DBMS and gets back the approximate answer and error estimate and sends them to the user. Sometimes, Verdict needs to post-process the result got from DBMS before sending it to user. 

Verdict is designed in a way that be as decoupled as possible from the underlying DBMS. The main parts of Verdict are independent from DBMS and just small amount of code needs to be added to support a new DBMS. This design lets us (or other developers in the future) to easily create a diver for any SQL DBMS and run Verdict on top of that DBMS. Currently we have developed drivers for **Impala** and **Hive**. We plan to add drivers for some other popular DBMS's soon.

Verdict supports both uniform and stratified samples. It uses the bootstrap method for error estimation which makes it able to support fairly complex queries.


## How to Install

To install verdict you need to first clone the repository and build the project:

```
git clone https://github.com/mozafari/verdict.git
cd verdict
build/sbt assembly
```

Now you need to configure Verdict. In the Configuration section, please read the part related to the DBMS you plan to use Verdict with.
 
## Verdict Configurations

A template for Verdict's configurations for each supported DBMS can be found in `configs` folder. You can find the config file for the DBMS you want to use and edit it based on description provided in the following subsections. 

### Global Configurations

The following configurations are related to approximate query processing and you may need to customized them regardless of what DBMS you use.
The value of this options can also be changed during running verdict using command [`SET`](#set-and-get-commands).
 
|Config         |Default Value  |Description                                        |
|------         |-------------  |-----------                                        |
|`bootstrap`    |`on`           |A boolean value `on/off` that switches approximate query processing on and off. Verdict doesn't do anything and just submits the original queries to the DBMS if this option is set to `off`.|
|`bootstrap.method`    |`uda`   |This option can have one of the values `uda`,`udf` or `stored`. It determines the method Verdict uses to perform bootstrap trials for calculating estimated error. Usually the `uda` method is the fastest, but other two options are useful for the DBMSs that don't support UDA (user defined aggregate function).|
|`bootstrap.trials`    |`100`   |An integer specifying the number of bootstrap trials being run for calculating error estimation. Usually `100` or smaller number works well. Choosing a very small number reduces the accuracy of error estimations, while a very large number of bootstrap trials makes the query slow.|
|`bootstrap.confidence`|`95%`   |A percentage that determines the confidence level for reporting the confidence interval (error estimation). For example when it is set to 95%, it means that Verdict is 95% confident that the true answer for the query is in the provided bound.|
|`bootstrap.sample_size`|`1%`   |A percentage that determines the preferred size for the sample (relative to the actual table) used for running approximate query. Choosing a small sample makes your queries faster but with higher error. When multiple samples are present for a table, Verdict tries to use the sample which size is closest to this value.|
|`bootstrap.sample_type`|`uniform`   |This option tells Verdict what kind of sample (`uniform` or `stratified`) do you prefer to run your query on. If both kind of samples are present for a table, Verdict tries to chose the one that is the kind specified in this option.|


### Configurations for Impala 

Please correct the values of the following configs in `configs/impala.conf`, if needed.

|Config         |Default Value  |Description                                        |
|------         |-------------  |-----------                                        |
|`dbms`         |None           |Tells Verdict what DBMS it should connect to. Use value `impala`.|
|`impala.host`  |`127.0.0.1`    |Impala's host address|
|`impala.port`  |`21050`        |Impala's JDBC port address|
|`impala.user`  |`""`           |Username to login with, if Impala's authentication is enabled|
|`impala.password`  |`""`       |Password to login with, if Impala's authentication is enabled|
|`udf_bin_hdfs` |None           |Verdict needs to install some UDF and UDAs on Impala. You need to copy the `udf_bin` folder to a location accessible by Impala in HDFS. Put the full HDFS path of `udf_bin` as the value for this config.|

Verdict for Impala also needs to connect to Hive. Because Impala hasn't necessary features for creating samples yet, Verdict uses Hive to create samples. Since Impala needs Hive to be running anyway and it uses Hive's metadata, Verdict's dependency to Hive is not a problem at all.

For using Verdict on Impala, you also need to set the values for Hive's configs. Please correct the Hive's config in `configs/impala.conf` based on the next section. Not that you do not need to edit `configs/hive.conf` for running verdict on Impala.

### Configurations for Hive 

Please correct the values of the following configs in `configs/hive.conf`, if needed.

|Config         |Default Value  |Description                                        |
|------         |-------------  |-----------                                        |
|`dbms`         |None           |Tells Verdict what DBMS it should connect to. Use value `hive` if you want use Verdict on Hive. Don't set this to `hive` if you want use Verdict on Impala.|
|`hive.host`  |`127.0.0.1`    |Hive's host address|
|`hive.port`  |`10000`        |Hive's JDBC port address|
|`hive.user`  |`hive`           |Username to login with|
|`hive.password`  |`""`       |Password to login with|
|`udf_bin` |None           |Verdict needs to deploy some UDF and UDAs into Hive. You need to copy the `udf_bin` folder to a place accessible by Hive (If Hive is running in another server you may need to copy `udf_bin` folder to that server).|


## Running Verdict

After building and configuring Verdict, you can run the its command line interface (CLI) using the following command:

`bin/verdict-cli -conf <config_file>`

Replace `<config_file>` with the config file you edited in the configuration step (i.e. `configs/impala.conf`, `config/hive.conf`, etc.)

You should be able to see the message `Successfully connected to <DBMS>`.


## Samples

Before you can submit any queries, you need to tell Verdict to create the samples you need. For doing that, you can use the `CREATE SAMPLE` command:
 
```
CREATE SAMPLE <sample_name> FROM <table_name> WITH SIZE <size_percentage>% 
    [STORE <number_of_poisson_columns> POISSON COLUMNS] 
    [STRATIFIED BY <column(s)>];
```

|Argument                 |Description                                        |
------------------------|------------                                       |
|`<sample_name>`        |The name of the sample to be created|
|`<table_name>`         |The name of the original table|
|`<size_percentage>%`   |The size of sample relative to the original sample, e.g. `5%`|
|`<number_of_poisson_columns>`   |This part is optional. This option specifies the number of Poisson random number columns to be generated and stored in the sample. These random numbers are needed only when you are using the `stored` bootstrap method.|
|`<column(s)>`   |If you want to create a uniform sample just ignore this part, otherwise, if you want to create a stratified sample, with this option you can specify the column(s) based on which Verdict should construct strata. The resulting sample will have a stratum for each distinct value of the specified column(s).|

To list the existing samples use the following command:

```
SHOW [<type>] SAMPLES [FOR <table_name>];
```

|Argument                 |Description                                        |
------------------------|------------                                       |
|`<type>`        |This is an optional argument that can be one of `ALL`, `STRATIFIED` or `UNIFORM` to specify the type of samples to be listed.|
|`<table_name>`         |An optional argument, can be used to list just samples of an specific table. If not specified, Verdict will list the samples for all tables.|

To delete a sample use the following command:

```
DROP <sample_name>;
```


## Submitting Query

After creating the proper samples, you can submit your queries. You should write your query as you would for an exact answer, that is, you should use the original table in your query, not a sample.

For example, one query can be the following, where the `sales` is the name of the original table:

```
SELECT department, SUM(price) from sales GROUP BY department;
```

On a exact DBMS you'll get a result like below:

```
department  |sum(price) 
-----------------------
foo         |4893
bar         |34509
```

However Verdict will output an answer like:

```
department  |sum(price) |ci_2   
---------------------------------------
foo         |4848       |[4753, 5023]
bar         |34613      |[34332, 34690]
```

`ci_2` means the confidence interval for the column number 2. If there are more aggregate function in the query, there will be a column for the confidence interval of each aggregation at the end.
 


## Supported Queries

Currently, Verdict supports the queries that have the following criteria:
- Query should have at least one of the supported aggregate functions `COUNT`, `SUM` and `AVG`
- Query can have sub-queries, but the aggregate functions cannot be in sub-queries.
- Query can have JOINs, but Verdict will replace one of the tables with a sample.

If Verdict identify a query as unsupported query, it will try running the query without modification. 


## `SET` and `GET` commands

You can use `SET` and `GET` commands to set or get the value of a query processing parameters [global configurations](#global-configurations) while verdict is running.

```
SET <parameter> = <value>;
GET <parameter>;
```

#### Example
```
> SET bootstrap.sample_size = 1%;
> GET bootstrap.sample_type;
```
