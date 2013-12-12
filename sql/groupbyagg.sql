select cs_group_max(Close,Day/3) from Quote_get('IBM');
select cs_group_min(Close,Day/3) from Quote_get('IBM');
select cs_group_sum(Close,Day/3) from Quote_get('IBM');
select cs_group_avg(Close,Day/3) from Quote_get('IBM');
select cs_group_var(Close,Day/3) from Quote_get('IBM');
select cs_group_dev(Close,Day/3) from Quote_get('IBM');
select cs_group_first(Close,Day/3) from Quote_get('IBM');
select cs_group_last(Close,Day/3) from Quote_get('IBM');
