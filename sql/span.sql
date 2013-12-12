select Close from Quote_get('IBM', '2-Nov-2013', '5-Nov-2013');
select Close from Quote_get('IBM', '2-Nov-2013');
select Close from Quote_get('IBM', from_ts:='2-Nov-2013');
select Close from Quote_get('IBM', till_ts:='5-Nov-2013');
select Close from Quote_get('IBM');

select Close from Quote_span('IBM', 1, 3);
select Close from Quote_span('IBM', 1);
select Close from Quote_span('IBM', from_pos:=1);
select Close from Quote_span('IBM', till_pos:=3);
select Close from Quote_span('IBM');

select Close from Quote_get(array['ABB','IBM'], '2-Nov-2013', '5-Nov-2013');
select Close from Quote_get(array['ABB','IBM'], '2-Nov-2013');
select Close from Quote_get(array['ABB','IBM'], from_ts:='2-Nov-2013');
select Close from Quote_get(array['ABB','IBM'], till_ts:='5-Nov-2013');
select Close from Quote_get(array['ABB','IBM']);

select Close from Quote_span(array['ABB','IBM'], 1, 3);
select Close from Quote_span(array['ABB','IBM'], 1);
select Close from Quote_span(array['ABB','IBM'], from_pos:=1);
select Close from Quote_span(array['ABB','IBM'], till_pos:=3);
select Close from Quote_span(array['ABB','IBM']);