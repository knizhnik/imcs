select Quote_delete('IBM', date('02-Nov-2013'));
select Quote_count('IBM');
select Day from Quote_get('IBM');

select Quote_delete('ABB', date('03-Nov-2013'), date('06-Nov-2013'));
select Quote_truncate();

select Quote_count('IBM');
select Quote_count('ABB');
select * from Quote_get('IBM');
select * from Quote_get('ABB');

select Quote_drop();