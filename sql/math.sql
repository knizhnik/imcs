select cs_sqrt((Open - Close) ^ 2.0) from Quote_get('IBM');
select cs_sin(Open)*cs_sin(Open) + cs_cos(Open)*cs_cos(Open) from Quote_get('IBM');
select cs_atan(cs_tan(Close)) from Quote_get('IBM');
select cs_asin(cs_sin(Close)) from Quote_get('IBM');
select cs_acos(cs_cos(Close)) from Quote_get('IBM');
select cs_log(cs_exp(Close/Open)) - Open/Close from Quote_get('IBM');
select cs_isnan(cs_parse('{-1,0,1}','float8')/0.0);