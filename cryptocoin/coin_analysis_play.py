'''
Created on Jan 2, 2018

@author: zmei
'''

import pandas
import talib
from talib.abstract import *
import numpy as np
import statsmodels
import stockstats
import MySQLdb as mysql
import matplotlib.pyplot as plt
import time

def get_macd():
	pass
get_macd()

def get_sma():
	talib.SMA()
	pass

def read_coin_close(coin):
	conn = init_connection()
	cursor = conn.cursor()
	sql = 'select close from coin_price_day'
	rows = cursor.execute(sql)
	close = [item[0] for item in cursor.fetchall()]
	cursor.close()
	conn.commit()
	conn.close()
	
	return close

def init_connection():
	conn = mysql.connect(user = 'root', passwd = 'root', host = '127.0.0.1', db='cc')
	return conn

close = np.random.random(100000000)
print len(close)

start = time.clock()
talib.SMA(close, timeperiod = 20)
end = time.clock()
print 'talib time: ' , start - end

df = pandas.DataFrame(close, columns = ['close price'])
start = time.clock()
df.rolling(20).mean()
end = time.clock()
print 'pandas time: ', start - end

# note that all ndarrays must be the same length!
inputs = {
    'open': np.random.random(100),
    'high': np.random.random(100),
    'low': np.random.random(100),
    'close': np.random.random(100),
    'volume': np.random.random(100)
}
#print inputs
#upper, middle, lower = BBANDS(inputs, 20, 2, 2)
#print upper, middle, lower
close = np.random.random(100)
SMA = talib.MA(close,30,matype=0)[-1]
EMA = talib.MA(close,30,matype=1)[-1]
WMA = talib.MA(close,30,matype=2)[-1]
DEMA = talib.MA(close,30,matype=3)[-1]
TEMA = talib.MA(close,30,matype=4)[-1]




