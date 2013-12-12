select cs_wsum(Volume,Close) from Quote_get('IBM');
select cs_wavg(Volume,Close) from Quote_get('IBM');
select cs_corr(High,Low) from Quote_get('IBM');
select cs_cov(High,Low) from Quote_get('IBM');
