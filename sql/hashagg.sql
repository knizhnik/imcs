select cs_hash_max(Close,Day % 2) from Quote_get('IBM');
select cs_hash_min(Close,Day % 2) from Quote_get('IBM');
select cs_hash_sum(Close,Day % 2) from Quote_get('IBM');
select cs_hash_avg(Close,Day % 2) from Quote_get('IBM');
select (q.p).agg_val,cs_cut((q.p).group_by,'i4i4') from (select cs_project_agg(cs_hash_sum(Close,(Day % 2)||(Volume%10))) p from Quote_get('IBM')) q;
select p.agg_val,cs_cut(p.group_by,'i4i4') from (select (cs_project_agg(cs_hash_sum(Close,(Day % 2)||(Volume%10)))).* from Quote_get('IBM')) p;
create type PairOfInt as (first integer, second integer);
select p.agg_val,cs_as(p.group_by,'PairOfInt') from (select (cs_project_agg(cs_hash_sum(Close,(Day % 2)||(Volume%10)))).* from Quote_get('IBM')) p;

select cs_hash_count(cs_floor((High-Low)*10)) from Quote_get('IBM');
select cs_hash_dup_count(cs_ceil((High-Low)*10), Day%3) from Quote_get('IBM');
select cs_hash_all('int4:{3,1,6,7,0,3,6,5,2,3,7}','int8:{1,1,1,2,2,3,3,4,5,5,5}');
select cs_hash_any('char:{3,1,6,7,0,3,6,5,2,3,7}','int2:{1,1,1,2,2,3,3,4,5,5,5}');