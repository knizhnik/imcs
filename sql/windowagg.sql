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
--- 2-days EMA (Exponential Moving Average)
select cs_window_ema(Close,3) from Quote_get('IBM');
--- 2-days ATR (Average True Range)
select cs_window_atr(cs_maxof(High-Low,cs_concat('float4:{0}',cs_maxof(cs_abs((High<<1) - Close), cs_abs((Low<<1) - Close)))), 3) << 2 from Quote_get('IBM');
