select cs_group_max(Close,Day/3) from Quote_get('IBM');
select cs_group_min(Close,Day/3) from Quote_get('IBM');
select cs_group_sum(Close,Day/3) from Quote_get('IBM');
select cs_group_avg(Close,Day/3) from Quote_get('IBM');
select cs_group_var(Close,Day/3) from Quote_get('IBM');
select cs_group_dev(Close,Day/3) from Quote_get('IBM');
select cs_group_first(Close,Day/3) from Quote_get('IBM');
select cs_group_last(Close,Day/3) from Quote_get('IBM');
select cs_group_all('int8:{3,1,6,7,0,3,6,5,2,3,7}','int4:{1,1,1,2,2,3,3,4,5,5,5}');
select cs_group_any('int2:{3,1,6,7,0,3,6,5,2,3,7}','char:{1,1,1,2,2,3,3,4,5,5,5}');
select cs_win_group_max(Close,cs_week(Day)) from Quote_get('IBM');
select cs_win_group_min(Close,cs_week(Day)) from Quote_get('IBM');
select cs_win_group_sum(Close,cs_week(Day)) from Quote_get('IBM');
select cs_win_group_avg(Close,cs_week(Day)) from Quote_get('IBM');
select cs_win_group_var(Close,cs_week(Day)) from Quote_get('IBM');
select cs_win_group_dev(Close,cs_week(Day)) from Quote_get('IBM');
select cs_win_group_first(Close,cs_week(Day)) from Quote_get('IBM');
select cs_win_group_last(Close,cs_week(Day)) from Quote_get('IBM');
select cs_win_group_sum('int4:{1,2,3,4,5,6,7,8,9,10}','int4:{1,1,1,2,2,3,3,3,3,4}');
