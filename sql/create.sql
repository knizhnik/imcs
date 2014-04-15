create table Quote (Symbol char(10), Day date, Open real, High real, Low real, Close real, Volume integer);
insert into Quote (Symbol,Day,Open,High,Low,Close,Volume) values ('IBM', date('01-Nov-2013'), 10.2, 11.0, 10.0, 10.5, 100);
insert into Quote (Symbol,Day,Open,High,Low,Close,Volume) values ('IBM', date('02-Nov-2013'), 20.2, 20.2, 20.2, 20.2, 200);
insert into Quote (Symbol,Day,Open,High,Low,Close,Volume) values ('IBM', date('04-Nov-2013'), 30.5, 31.0, 30.0, 30.2, 300);
insert into Quote (Symbol,Day,Open,High,Low,Close,Volume) values ('IBM', date('05-Nov-2013'), 40.5, 41.0, 40.0, 40.2, 400);
insert into Quote (Symbol,Day,Open,High,Low,Close,Volume) values ('IBM', date('06-Nov-2013'), 50.2, 51.0, 50.0, 50.5, 500);
insert into Quote (Symbol,Day,Open,High,Low,Close,Volume) values ('ABB', date('03-Nov-2013'), 60.5, 61.0, 70.0, 60.2, 600);
insert into Quote (Symbol,Day,Open,High,Low,Close,Volume) values ('ABB', date('06-Nov-2013'), 70.2, 71.0, 70.0, 70.5, 700);
create extension imcs;
select cs_create('Quote', 'Day', 'Symbol');
select Quote_load();

-- Table with varying size strings which are mapped to integers using dictionary
create table CrashLog(
log_time timestamp, ---when this data is produce,selective
uid bigint, ---user id
country varchar,----USA,Chine,Japan etc...cardinal number is ~ 100,has hot spot
city varchar, ---cardinal number~10000,has hot spot
device varchar, ---iphone4,nokiaX etc, cardinal number is 1000,has hot spot
net varchar, -----wifi,Vodafone 3G... cardinal number is 10,hash hot spot
os varchar); ----iOS6.x,Android 2.3 cardinal number is 100,hash hot spot

insert into CrashLog values ('2014-04-14 11:54', 10000001, 'USA', 'New York', 'iPhone4', 'wifi', 'iOS6.x');
insert into CrashLog values ('2014-04-14 11:55', 10000002, 'Japan', 'Tokio', 'Sony Xperia Z1', 'Vodafone 3G', 'Android 4.4');
insert into CrashLog values ('2014-04-14 11:56', 10000003, 'China', 'Beijing', 'iPhone5', 'wifi', 'iOS7.x');
select cs_create('CrashLog', 'log_time');
select CrashLog_load();

select cs_used_memory();
