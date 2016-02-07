create table lineitem(
   l_orderkey integer,
   l_partkey integer,
   l_suppkey integer,
   l_linenumber integer,
   l_quantity real,
   l_extendedprice real,
   l_discount real,
   l_tax real,
   l_returnflag char,
   l_linestatus char,
   l_shipdate date,
   l_commitdate date,
   l_receiptdate date,
   l_shipinstruct char(25),
   l_shipmode char(10),
   l_comment char(44),
   l_dummy char(1));

create index lineitem_order_fk on lineitem(l_orderkey);

copy lineitem from 'lineitem.tbl' delimiter '|' csv;

create view lineitems as select 
	l_orderkey,
	l_returnflag,
	l_linestatus,
	l_quantity,
	l_extendedprice,
	l_discount,
	l_tax,
	l_shipdate 
from 
	lineitem;

select cs_create('lineitems','l_orderkey');

\timing

select 
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
    sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= cast('1998-12-01' as date)
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

select g1,sum_qty,sum_base_price,sum_disc_price,sum_charge,sum_qty/count_order as avg_qty,sum_base_price/count_order as avg_price,avg_disc,count_order
from 
(select (agg).* 
 from
  lineitems_get() as l,
  cs_project(row(
    (cs_hash_sum(cs_filter(l.l_shipdate <= cast('1998-12-01' as date), l.l_quantity),
	             cs_filter(l.l_shipdate <= cast('1998-12-01' as date), l.l_returnflag||l.l_linestatus))).*,
    (cs_hash_sum(cs_filter(l.l_shipdate <= cast('1998-12-01' as date), l.l_extendedprice),
	             cs_filter(l.l_shipdate <= cast('1998-12-01' as date), l.l_returnflag||l.l_linestatus))).*, 
    (cs_hash_sum(cs_filter(l.l_shipdate <= cast('1998-12-01' as date), l.l_extendedprice*(-l.l_discount+1)),
	             cs_filter(l.l_shipdate <= cast('1998-12-01' as date), l.l_returnflag||l.l_linestatus))).*, 
    (cs_hash_sum(cs_filter(l.l_shipdate <= cast('1998-12-01' as date), l.l_extendedprice*(-l.l_discount+1)*(l.l_tax+1)),
                 cs_filter(l.l_shipdate <= cast('1998-12-01' as date), l.l_returnflag||l.l_linestatus))).*, 
    (cs_hash_avg(cs_filter(l.l_shipdate <= cast('1998-12-01' as date), l.l_discount),
                 cs_filter(l.l_shipdate <= cast('1998-12-01' as date), l.l_returnflag||l.l_linestatus))).*,  
	(cs_hash_count(cs_filter(l.l_shipdate <= cast('1998-12-01' as date), l.l_returnflag||l.l_linestatus))).*))
	 as agg(sum_qty float8,g1 char,sum_base_price float8,g2 char,sum_disc_price float8,g3 char,sum_charge float8,g4 char,avg_disc float8,g5 char,count_order bigint,g6 char)) s;


select g1,sum_qty,sum_base_price,sum_disc_price,sum_charge,sum_qty/count_order as avg_qty,sum_base_price/count_order as avg_price,avg_disc,count_order
from 
(select (agg).* 
 from
  lineitems_get() as ll,
  lineitems_filter(ll, ll.l_shipdate <= cast('1998-12-01' as date)) as l,
  cs_project(row(
    (cs_hash_sum(l.l_quantity, l.l_returnflag||l.l_linestatus)).*,
    (cs_hash_sum(l.l_extendedprice, l.l_returnflag||l.l_linestatus)).*, 
    (cs_hash_sum(l.l_extendedprice*(-l.l_discount+1), l.l_returnflag||l.l_linestatus)).*, 
    (cs_hash_sum(l.l_extendedprice*(-l.l_discount+1)*(l.l_tax+1), l.l_returnflag||l.l_linestatus)).*, 
    (cs_hash_avg(l.l_discount, l.l_returnflag||l.l_linestatus)).*,  
	(cs_hash_count(l.l_returnflag||l.l_linestatus)).*))
	 as agg(sum_qty float8,g1 char,sum_base_price float8,g2 char,sum_disc_price float8,g3 char,sum_charge float8,g4 char,avg_disc float8,g5 char,count_order bigint,g6 char)) s;

select g1,sum_qty
from 
(select (agg).* 
 from
  lineitems_get() as ll,
  lineitems_filter(ll, ll.l_shipdate <= cast('1998-12-01' as date)) as l,
  cs_project(cs_hash_sum(l.l_quantity+l.l_quantity, l.l_returnflag||l.l_linestatus))
	 as agg(sum_qty float8,g1 char)) s;






select cs_cut,sum_qty,sum_base_price,sum_disc_price,sum_charge,sum_qty/count_order as avg_qty,sum_base_price/count_order as avg_price,avg_disc,count_order
 from 
(select cs_cut(group_by,'i1i1'),agg_val as sum_qty from
 (select (cs_project_agg(cs_hash_sum(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_quantity),
                                                    cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag||l_linestatus)))).*  
  from lineitems_get()) agg) q1
natural join
(select cs_cut(group_by,'i1i1'),agg_val as sum_base_price from
 (select (cs_project_agg(cs_hash_sum(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_extendedprice),
                                                    cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag||l_linestatus)))).* 
  from lineitems_get()) agg) q2
natural join
(select cs_cut(group_by,'i1i1'),agg_val as sum_disc_price from
 (select (cs_project_agg(cs_hash_sum(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_extendedprice*(-l_discount+1)),
                                                    cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag||l_linestatus)))).* 
  from lineitems_get()) agg) q3
natural join
(select cs_cut(group_by,'i1i1'),agg_val as sum_charge from
 (select (cs_project_agg(cs_hash_sum(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_extendedprice*(-l_discount+1)*(l_tax+1)),
                                                    cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag||l_linestatus)))).* 
  from lineitems_get()) agg) q4
natural join
(select cs_cut(group_by,'i1i1'),agg_val as avg_disc from
 (select (cs_project_agg(cs_hash_avg(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_discount),
                                                    cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag||l_linestatus)))).*  
  from lineitems_get()) agg) q5
natural join
(select cs_cut(group_by,'i1i1'),agg_val as count_order from
 (select (cs_project_agg(cs_hash_count(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag||l_linestatus)))).*
  from lineitems_get()) agg) q6;
 









select cs_cut(group_by,'i1i1'),agg_val as sum_charge from
 (select (cs_project_agg(cs_hash_sum(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_extendedprice*(-l_discount+1)*(l_tax+1)),
	   			                     cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag||l_linestatus)))).* 
  from lineitems_get()) agg;


select 
    l_returnflag,
    l_linestatus,
    sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge
from
    lineitem
where
    l_shipdate <= cast('1998-12-01' as date)
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;


select cs_cut,sum_qty,sum_base_price,sum_disc_price,sum_charge,sum_qty/count_order as avg_qty,sum_base_price/count_order as avg_price,count_order
from 
(select cs_cut(group_by,'i1i1'),agg_val as sum_qty from
 (select (cs_project_agg(cs_hash_sum(l_quantity, l_returnflag||l_linestatus))).*  
  from lineitems_get()) agg) q1
natural join
(select cs_cut(group_by,'i1i1'),agg_val as sum_base_price from
 (select (cs_project_agg(cs_hash_sum(l_extendedprice, l_returnflag||l_linestatus))).* 
  from lineitems_get()) agg) q2
natural join
(select cs_cut(group_by,'i1i1'),agg_val as sum_disc_price from
 (select (cs_project_agg(cs_hash_sum(l_extendedprice*(-l_discount+1)),
	   			                     l_returnflag||l_linestatus)))).* 
  from lineitems_get()) agg) q3
natural join
(select cs_cut(group_by,'i1i1'),agg_val as sum_charge from
 (select (cs_project_agg(cs_hash_sum(l_extendedprice*(-l_discount+1)*(l_tax+1)),
	   			                     l_returnflag||l_linestatus)))).* 
  from lineitems_get()) agg) q4
natural join
(select cs_cut(group_by,'i1i1'),agg_val as avg_disc from
 (select (cs_project_agg(cs_hash_avg(l_discount),
	   			                     l_returnflag||l_linestatus)))).*  
  from lineitems_get()) agg) q5
natural join
(select cs_cut(group_by,'i1i1'),agg_val as count_order from
 (select (cs_project_agg(cs_hash_count(l_returnflag||l_linestatus)))).*
  from lineitems_get()) agg) q6;


select cs_cut(group_by,'i1i1'),agg_val as sum_charge from
 (select (cs_project_agg(cs_hash_sum(l_extendedprice*(-l_discount+1)*(l_tax+1)),
	   			                     l_returnflag||l_linestatus)))).* 
  from lineitems_get()) agg;
.* agg;


select cs_hash_sum(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_quantity),
	   			   cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag*256 + l_linestatus)) as sum_qty,
       cs_hash_sum(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_extendedprice),
	   			   cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag*256 + l_linestatus)) as sum_base_price,
       cs_hash_sum(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_extendedprice*(1-l_discount)),
	   			   cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag*256 + l_linestatus)) as sum_disc_price,
       cs_hash_sum(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_extendedprice*(1-l_discount)*(1+l_tax)),
	   			   cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag*256 + l_linestatus)) as sum_charge,
       cs_hash_avg(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_quantity),
	   			   cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag*256 + l_linestatus)) as avg_qty,
       cs_hash_avg(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_price),
	   			   cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag*256 + l_linestatus)) as avg_price,
       cs_hash_avg(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_discount),
	   			   cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag*256 + l_linestatus)) as avg_discount,


		  lineitems_hash_sum(cs_filter(l_shipdate <= cast('1998-12-01' as date), l_quantity),	
	   	   	              cs_filter(l_shipdate <= cast('1998-12-01' as date), l_returnflag*256 + l_linestatus)) as sum_qty,
						  
	       sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
    sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
f