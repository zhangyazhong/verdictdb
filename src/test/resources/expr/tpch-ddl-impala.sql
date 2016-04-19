CREATE EXTERNAL TABLE IF NOT EXISTS lineitem (l_orderkey bigint , l_partkey bigint ,        l_suppkey bigint ,        l_linenumber bigint ,        l_quantity FLOAT,        l_extendedprice FLOAT,        l_discount FLOAT,        l_tax FLOAT,        l_returnflag STRING ,        l_linestatus STRING ,        l_shipdate string ,        l_commitdate string ,        l_receiptdate string ,        l_shipinstruct STRING,        l_shipmode STRING,        l_comment STRING ) ROW FORMAT DELIMITED FIELDS TERMINATED by '|' STORED AS TEXTFILE LOCATION '/user/hive/tpch50/lineitem';

CREATE EXTERNAL TABLE IF NOT EXISTS customer ( c_custkey BIGINT, c_name STRING, c_address STRING, c_nationkey INT, c_phone STRING, c_acctbal FLOAT, c_mktsegment STRING, c_comment STRING) ROW FORMAT DELIMITED FIELDS TERMINATED by '|' STORED AS TEXTFILE LOCATION '/user/hive/tpch50/customer';

CREATE EXTERNAL TABLE IF NOT EXISTS nation ( n_nationkey INT, n_name STRING, n_regionkey INT, n_comment STRING) ROW FORMAT DELIMITED FIELDS TERMINATED by '|' STORED AS TEXTFILE LOCATION '/user/hive/tpch50/nation';

CREATE EXTERNAL TABLE IF NOT EXISTS orders ( o_orderkey BIGINT, o_custkey BIGINT, o_orderstatus STRING, o_totalprice FLOAT, o_orderdate string, o_orderpriority STRING, o_clerk STRING, o_shippriority INT, o_comment STRING) ROW FORMAT DELIMITED FIELDS TERMINATED by '|' STORED AS TEXTFILE LOCATION '/user/hive/tpch50/orders';

CREATE EXTERNAL TABLE IF NOT EXISTS part ( p_partkey BIGINT, p_name STRING, p_mfgr STRING, p_brand STRING, p_type STRING, p_size INT, p_container STRING, p_retailprice FLOAT, p_comment STRING ) ROW FORMAT DELIMITED FIELDS TERMINATED by '|' STORED AS TEXTFILE LOCATION '/user/hive/tpch50/part';

CREATE EXTERNAL TABLE IF NOT EXISTS partsupp ( ps_partkey BIGINT, ps_suppkey BIGINT, ps_availqty INT, ps_supplycost FLOAT, ps_comment STRING ) ROW FORMAT DELIMITED FIELDS TERMINATED by '|' STORED AS TEXTFILE LOCATION '/user/hive/tpch50/partsupp';

CREATE EXTERNAL TABLE IF NOT EXISTS region ( r_regionkey INT, r_name STRING, r_comment STRING ) ROW FORMAT DELIMITED FIELDS TERMINATED by '|' STORED AS TEXTFILE LOCATION '/user/hive/tpch50/region';

CREATE EXTERNAL TABLE IF NOT EXISTS supplier ( s_suppkey BIGINT, s_name STRING, s_address STRING, s_nationkey INT, s_phone STRING, s_acctbal FLOAT, s_comment STRING ) ROW FORMAT DELIMITED FIELDS TERMINATED by '|' STORED AS TEXTFILE