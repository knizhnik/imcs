\timing

-- Table with quotes. Do not use index to make inserts faster
create table Quote (Symbol char(10), Day date, Open real, High real, Low real, Close real, Volume integer);

-- Load NYSE data for ten year (can be obtained from http://www.garret.ru/NYSE_2003_2013.csv.gz)
\copy Quote from 'NYSE_2003_2013.csv' with csv header;
-- 2m31.435s
-- with triggers: 6m30.939s

-- This table should actually contain more information about companies, but for this example we need just symbol name
create table Securities (Symbol char(10));

-- It is certainly not efficient way of populating Securities table, usually information about all used symbols is available
insert into Securities select distinct Symbol from Quote;

-- Generate timeseries functions 
select cs_create('Quote', 'Day', 'Symbol');

select Quote_load(); 
--- Time: 10222.079 ms

-- We will use this view to perform queries for all quotes for symbols
--create view SecurityQuotes as select * from Quote_get(array(select Symbol from Securities));
create view SecurityQuotes as select (Quote_get(Symbol)).* from Securities;


-- Calculate VWAP (volume-weighted average price) for each symbol
select Symbol,cs_sum(Close*Volume) / cs_sum(Volume) as VWAP from SecurityQuotes;
--- Time: 386.528 ms

-- Show growth days for symbol ABB during first quoter of 2010 
select (Quote_project(abb.*,cs_top_max_pos(Close, 10))).* from Quote_get('ABB', date('01-Jan-2010'), date('31-Mar-2010')) abb;

select (Quote_project(abb.*,cs_filter_pos(Close>Open*1.01))).* from Quote_get('ABB', date('01-Jan-2010'), date('31-Mar-2010')) abb;

select cs_count(cs_filter_pos(Close>Open*1.01))  from Quote_get('ABB');

-- Now calculate VWAP using standard Postgress aggregates.
select Symbol,sum(Close*Volume)/sum(Volume) as VWAP from Quote group by Symbol;
--- Time: 2184.646 ms

select Symbol,cs_sum(Close*Volume) / cs_sum(Volume) as VWAP from Quote_get('ABB');
--- Time: 0.506 ms

select Symbol,sum(Close*Volume)/sum(Volume) as VWAP from Quote group by Symbol having Symbol='ABB';
--- Time: 2.818 ms

select cs_sum(Close) from Quote_concat(array(select Symbol from Securities));
--- Time: 76.167 ms


--- Average True Range (ATR) indicator with 14 days period for last quarter of ABB
select cs_window_atr(cs_maxof(High-Low,0|||cs_maxof(cs_abs((High<<1) - Close), cs_abs((Low<<1) - Close))), 14) << 13 from Quote_get('ABB', date('01-Jan-2010'), date('31-Mar-2010'));

--- Relative Strength Index (RSI) indicator with 14 days period for last quarter of ABB
select 100-(100/(1+cs_window_ema(cs_maxof(cs_diff(Close), 0), 14)/cs_window_ema(cs_maxof(-cs_diff(Close), 0), 14))) from Quote_get('ABB', date('01-Jan-2010'), date('31-Mar-2010'));


--- Now place all quotes in single timeseries (no symbol)
select Quote_drop();
select cs_create('Quote', 'Day');

select Quote_load();
--- Time: 7658.043 ms

--- Calculate VWAP for the whole timeseries with ~6 millions elments
select cs_sum(Close*Volume) / cs_sum(Volume) as VWAP from Quote_get();
--- Time: 7.616 ms

--- Yet another way of calculating VWAP using cs_wavg
select Volume//Close as VWAP from Quote_get();
--- Time: 6.501 ms

--- The same query using standard Postgress aggregates
select sum(Close*Volume)/sum(Volume) as VWAP from Quote;
--- Time: 1078.843 ms

--- Select top 5 symbols with largest average prices
select cs_project(q, cs_top_max_pos((q).avg, 5)) from (select cs_hash_avg(Close, Symbol) q from Quote_get() offset 0) s;
--- Time: 76.214 ms

--- The same using standard SQL
select avg(Close) ac,Symbol from Quote group by Symbol order by ac desc limit 5;
--- Time: 1621.042 ms

--- Find longest periods when NYSE is not working
select cs_map(Day, cs_top_max_pos(cs_diff(Day), 5)),cs_top_max(cs_diff(Day), 5) from Quote_get();
--- Time: 38.448 ms

--- Number of unique values of close prices for 10 years
select cs_count(cs_unique(cs_sort(Close))) from Quote_get();
--- Time: 866.869 ms

select cs_quantile(Close,5) from Quote_get();
--- Time: 810.362 ms

--- Delete all records
select StackOptions_delete();
select sum(Quote_delete(Symbol)) from Securities;