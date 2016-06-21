select
sum(quantity) as sum_qty,
sum(extendedprice) as sum_base_price,
avg(extendedprice) as avg_price,
count(*) as count_order
from
lineitem40
where
shipdate <= '1998-09-01'
and returnflag = 'A'
and linestatus = 'F'