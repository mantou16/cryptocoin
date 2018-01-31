# Coin Analysis

# Hit or close to Hit New High

select c.*, a.sale_days, b.* from 
(select exchange, fsym, tsym, count(*) as sale_days from coin_exchange_price_day group by exchange, fsym, tsym having sale_days > 10) a
left join
(select exchange, fsym, tsym, max(close) as max_close, max(high) as max_high from coin_exchange_price_day group by exchange, fsym, tsym) b
on a.fsym = b.fsym
and a.tsym = b.tsym
and a.exchange = b.exchange
left join
coin_exchange_price_day c
on a.fsym = c.fsym
and a.tsym = c.tsym
and a.exchange = c.exchange 
#c.close <= b.max_close*1.1 and 
where
(c.high >= b.max_high)# or c.close >= b.max_close*0.9)
 and
c.volumeto > 1 and 
c.time = '2018-01-13'
order by c.exchange;

select c.*, a.sale_days, b.* from 
(select exchange, fsym, tsym, count(*) as sale_days from coin_exchange_price_day group by exchange, fsym, tsym having sale_days > 10) a
left join
(select exchange, fsym, tsym, min(close) as min_close, min(low) as min_low from coin_exchange_price_day group by exchange, fsym, tsym) b
on a.fsym = b.fsym
and a.tsym = b.tsym
and a.exchange = b.exchange
left join
coin_exchange_price_day c
on a.fsym = c.fsym
and a.tsym = c.tsym
and a.exchange = c.exchange 
#c.close <= b.max_close*1.1 and 
where
(c.low <= b.min_low*1.5)# or c.close >= b.max_close*0.9)
 and
c.volumeto > 1 and 
c.time = '2018-01-13'
order by c.exchange;


select a.*,b.* from 
coin_exchange_price_day a
join
(select exchange, fsym, tsym, max(close) as max_close, max(high) as max_high from coin_exchange_price_day group by exchange, fsym, tsym) b
on 
a.exchange = b.exchange and
a.fsym = b.fsym and 
a.tsym = b.tsym and
a.high = b.max_high;


# 每天大涨的个数
select c.exchange, c.time, sum(c.jump) from
(
select a.time, a.exchange, a.fsym#, a.open, a.close, b.open, b.close, a.open*b.open usd_price_open, a.close*b.close usd_price_close, ((a.close*b.close)/(a.open*b.open)-1)*100
, if( (((a.close*b.close)/(a.open*b.open)-1)*100 > 30),  1, 0 ) as jump
from
coin_exchange_price_day a
join
coin_exchange_price_day b
on a.time = b.time
and a.exchange = b.exchange
and b.fsym = 'BTC'
and b.tsym = 'USD'
and a.tsym = 'BTC'
#and a.time >= '2016-01-01'
#and a.volumeto >= 2
) c
group by c.exchange, c.time
order by c.exchange, c.time
limit 5000

# 每天大跌的个数
select c.exchange, c.time, sum(c.jump) from
(
select a.time, a.exchange, a.fsym#, a.open, a.close, b.open, b.close, a.open*b.open usd_price_open, a.close*b.close usd_price_close, ((a.close*b.close)/(a.open*b.open)-1)*100
, if( (((a.close*b.close)/(a.open*b.open)-1)*100 < -30 ),  1, 0 ) as jump
from
coin_exchange_price_day a
join
coin_exchange_price_day b
on a.time = b.time
and a.exchange = b.exchange
and b.fsym = 'BTC'
and b.tsym = 'USD'
and a.tsym = 'BTC'
#and a.time >= '2016-01-01'
#and a.volumeto >= 2
) c
group by c.exchange, c.time
order by c.exchange, c.time
limit 5000