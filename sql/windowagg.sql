select cs_window_max(Close,3) from Quote_get('IBM');
select cs_window_min(Close,3) from Quote_get('IBM');
select cs_window_sum(Close,3) from Quote_get('IBM');
select cs_window_avg(Close,3) from Quote_get('IBM');
select cs_window_var(Close,3) from Quote_get('IBM');
select cs_window_dev(Close,3) from Quote_get('IBM');
--- Leave only elements corresponding to entire window
select cs_limit(cs_window_max(Close,3), 2) from Quote_get('IBM');
select cs_window_min(Close,3) << 2 from Quote_get('IBM');
select cs_window_sum(Close,3) << 2 from Quote_get('IBM');
select cs_window_avg(Close,3) << 2 from Quote_get('IBM');
select cs_window_var(Close,3) << 2 from Quote_get('IBM');
select cs_window_dev(Close,3) << 2 from Quote_get('IBM');
