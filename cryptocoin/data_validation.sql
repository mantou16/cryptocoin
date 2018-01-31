# Data validation
select count(*) from coin_profile;
select count(*) from coin_price_multifull;
select count(*) from coin_price_hour;
select count(*) from coin_price_day;

select count(distinct symbol) from coin_profile;
select count(distinct fsym, tsym) from coin_price_multifull;
select count(distinct fsym, tsym) from coin_price_hour;
select count(distinct fsym, tsym) from coin_price_day;

select fsym, tsym, count(*) from coin_price_hour group by fsym, tsym;
select fsym, tsym, count(*) from coin_price_day group by fsym, tsym;

select exchange, tsym, count(distinct fsym) from coin_exchange_price_day group by exchange, tsym;

select distinct fsym from cc.coin_price_multifull 
where tsym = 'BTC' and totalvolume24hto > 2 and fsym not in
(select distinct fsym from coin_price_day);

