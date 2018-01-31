-- Exponential moving average (EMA) calculation 

-- first set the delimiter to $$ 
-- otherwise, it will treat the normal ';' as end statement
DELIMITER $$

-- execute a drop if the function exists
-- in the example, our db is stock
-- we call the function cal_ema
DROP function IF EXISTS func_ema $$

-- in the () Identifies the in parameters
-- for ema calculation, 3 parameters are needed
CREATE function func_ema(
  in_period INT,
  in_prior_ema DOUBLE,
  in_current_price DOUBLE
)
-- specify the return data type as 
-- float with 2 decimal points
RETURNS DOUBLE
DETERMINISTIC
-- to calculate ema, 3 values are needed
-- var in_period: is the ema period 
-- for example, to get a 10 day ema, pass in 10 as the parameter
-- var in_prior_ema: is the prior day ema
-- var in_current_price: is the current price

BEGIN
	-- declare the ema variable
	-- to store the calculated value
	DECLARE ema DOUBLE; 
	
	-- perform the calculation
	SET ema = ((in_current_price * ( 2 / ( in_period + 1 )))
		+ (in_prior_ema * (1 - (2 / (in_period + 1)))));
	-- round the result to 2 decimal points
#	SET ema = ROUND(ema, 8);
	
	-- return the result
	return ema;
-- end the stored function code block
END $$

DELIMITER ;