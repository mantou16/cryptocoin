DELIMITER //

DROP PROCEDURE IF EXISTS  coin_price_day_update;
CREATE PROCEDURE coin_price_day_update()
BEGIN
	DECLARE fsym_v text;
    DECLARE close_start double;
	DECLARE has_more int default 1;
    DECLARE _cur CURSOR FOR 
    select a.fsym, a.close from coin_price_day_raw a 
    join 
    (select fsym, min(time) as min_time from coin_price_day_raw group by fsym) b 
    on a.fsym = b.fsym 
    and a.time = b.min_time;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET has_more = 0;
	OPEN _cur;
		WHILE has_more DO
			FETCH _cur INTO fsym_v, close_start;
            IF has_more = 1 THEN
                insert into coin_price_day (fsym, date, volumefrom, volumeto, high, low, open, close, EMA_12, EMA_26, DIFF, DEA, HIST)
				select c.fsym, from_unixtime(c.time) as date, c.volumefrom, c.volumeto, c.high, c.low, c.open, c.close, 
							#@ema_5 := 2/(5+1)*c.close + (5-1)/(5+1)*@ema_5 AS EMA_5,
                            #@ema_12 := 2/(12+1)*c.close + (12-1)/(12+1)*@ema_12 AS EMA_12,
                            #@ema_26 := 2/(26+1)*c.close + (26-1)/(26+1)*@ema_26 AS EMA_26,
                            @ema_12 := func_ema(12, @ema_12, c.close) AS EMA_12,
                            @ema_26 := func_ema(26, @ema_26, c.close) AS EMA_26,
                            @diff := @ema_12 - @ema_26 AS DIFF,
                            @dea := func_ema(9, @dea, @diff) AS DEA,
                            @hist := @diff - @dea AS HIST
                            #@dea := @
				from (select @ema_12 :=  close_start, @ema_26 :=  close_start, @diff := 0, @dea := 0, @hist := 0) as dummy 
				cross join coin_price_day_raw as c 
				where fsym = fsym_v order by time asc;
			END IF;
		END WHILE;
    CLOSE _cur;
END //

DELIMITER ;