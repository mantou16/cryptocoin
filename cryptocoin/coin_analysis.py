'''
Created on Jan 2, 2018

@author: zmei
'''

import pandas as pd
import talib
#from talib.abstract import *
import numpy as np
import statsmodels
import stockstats
import MySQLdb as mysql
#from matplotlib.pyplot import *
import itertools
import time
import pylab as pl
from _mysql import result
from operator import itemgetter
from time import ctime
import threading
from threading import current_thread
import Queue



def init_connection():
	conn = mysql.connect(user = 'root', passwd = 'root', host = '127.0.0.1', db='cc')
	return conn


def read_coin_pair(granularity, exchange):
	conn = init_connection()
	cursor = conn.cursor()
	sql = 'select distinct fsym, tsym from coin_exchange_price_' + granularity + ' where exchange = %s limit 10'  #limit 60 offset 0 = limit 0, 60 (60 records from first one)
	cursor.execute(sql, (exchange,))
	result = cursor.fetchall()
	result = np.array(result)
	#result = [item for item in cursor.fetchall()]
	cursor.close()
	conn.commit()
	conn.close()
	return result

def read_coin_close(fsym, tsym, granularity, exchange):
	conn = init_connection()
	cursor = conn.cursor()
	sql = 'select time, close from coin_exchange_price_' + granularity +' where fsym = %s and tsym = %s and exchange = %s order by time desc'  #limit 60 offset 0 = limit 0, 60 (60 records from first one)
	cursor.execute(sql, (fsym, tsym, exchange))
	result = cursor.fetchall()
	result = np.array(result)
	#result = [item for item in cursor.fetchall()]
	cursor.close()
	conn.commit()
	conn.close()
	return result

def find_macd_gold_cross(granularity, exchange):
	pairs = read_coin_pair(granularity, exchange)
	gold_gold = list()
	gold_silver = list()
	for fsym, tsym in pairs:
		result = read_coin_close(fsym, tsym, granularity, exchange)
		close = np.array([float(i) for i in result[:,1]])
		macd,macdsignal,macdhist = talib.MACD(close, fastperiod=14, slowperiod=27, signalperiod=2)

		if len(macdhist) > 28:   #need at least 34 data points to mature MACD
			if (macdhist[-2] <= 0) & (macdhist[-1] > 0): #& (macdsignal[-2]<0):
				cross_point = macd[-1] - abs(macd[-1]-macd[-2])*abs(macd[-1]-macdsignal[-1])/(abs(macd[-1]-macdsignal[-1])+abs(macdsignal[-2]-macd[-2]))
				if cross_point < 0:
					gold_gold.append([fsym, tsym, macdhist[-2], macdhist[-1], cross_point])
				elif cross_point >= 0:
					gold_silver.append([fsym, tsym, macdhist[-2], macdhist[-1], cross_point])

	print 'list of gold cross under 0-----------------------------:'
	for item in gold_gold:
		print item[0], ' : ', item[1] 
	print 'list of gold cross above 0+++++++++++++++++++++:'
	for item in gold_silver:
		print item[0], ' : ', item[1]

def get_X(fsym, tsym, granularity, exchange):
	conn = init_connection()
	cursor = conn.cursor()
	sql = 'select fsym, tsym, time, open, high, low, close, volumefrom from coin_exchange_price_' + granularity + ' where fsym = %s and tsym = %s and exchange = %s'  #limit 60 offset 0 = limit 0, 60 (60 records from first one)
	cursor.execute(sql, (fsym, tsym, exchange))
	result = cursor.fetchall()
	#print fsym, ' : ', tsym, ' X size: ', len(result)
	result = np.array(result)
	#result = [item for item in cursor.fetchall()]
	cursor.close()
	conn.commit()
	conn.close()
	return result

def get_open(X):
	return X[:,3]
def get_high(X):
	return X[:,4]
def get_low(X):
	return X[:,5]
def get_close(X):
	return X[:,6]
def get_volume(X):
	return X[:,7]
def get_ohlcv(X):
	return X[:,3:7]
def REF(X, N):
	return X[:N]   # N means number of items, return including index 0 item
def HHV(X, N):
	return np.max(REF(X, N))
def HHVBARS(X, N):
	return np.argmax(REF(X, N))
def LLV(X, N):
	return np.min(REF(X, N))
def LLVBARS(X, N):
	return np.argmin(REF(X, N))

def find_break_tank(X, bars, range):
	hh = HHV(get_high(X), bars)
	ll = LLV(get_low(X), bars)
	c = REF(get_close(X),0)
	v = REF(get_volume(X),0)
	(hh/ll-1)*100<=range and c>hh and v>HHV(REF(get_volume(X),1),bars)



def find_new_high(granularity, range_down, range_up, bars,  correction_bars, exchange):
	pairs = read_coin_pair(granularity, exchange)
	result = list()
	for fsym, tsym in pairs:
		X = get_X(fsym, tsym, granularity, exchange)
		highs = get_high(X)
		h = REF(highs, 1)
		hh = HHV(highs[1:], bars)
		hhb = HHVBARS(highs[1:], bars)
		if (h>=hh*range_down) & (h<=hh*range_up) & (hhb>correction_bars):
			result.append([fsym, tsym, hh, h])
	return np.array(result)


def find_price_boundary(fsym, tsym, high_low, granularity, range_down, range_up, exchange):
	X = get_X(fsym, tsym, granularity, exchange)
	if high_low == 'high':
		prices = get_high(X)
	else:  #low
		prices = get_low(X)
	
	price_list = list()
	cnt_list = list()
	for p in prices:
		price_list.append(p)
		mask = np.ma.masked_outside(prices, p*range_down, p*range_up)
		cnt = mask.count()
		cnt_list.append(cnt)
	price_array = np.array(price_list)
	cnt_max = max(cnt_list)
	result = price_array[cnt_list==cnt_max]
	return result
	'''
	temp = list()
	for h in prices:
		mask = np.ma.masked_outside(prices, h*range_down, h*range_up)
		cnt = mask.count()
		temp.append((h, cnt))
	'''
	'''
	#use np structured array
	sa = np.array(temp,dtype=[('fsym','S10'),('tsym','S10'),('high',float),('count',int)])  #create a structured array
	sa_sort =  np.sort(sa, order = 'count')
	result.append(sa_sort[:5])
	result.append(sa_sort[-5:])
	'''
	'''
	# use list sorted function
	temp_sort = sorted(temp, key=itemgetter(1))
	result = temp_sort[:5]
	#result.extend(temp_sort[-5:])   #should not include the least ones, support prices should also appears many times, instead of least times
	return np.array(result)[:,0]
	'''

def join_result(X1, X2):  #intersection
	if (X1.size > 0) & (X1.size > 0):
		set1 = set([tuple(i) for i in X1])
		set2 = set([tuple(i) for i in X2])
		return np.array([x for x in set1 & set2])
	elif X1.size > 0:
		return X1
	else:
		return X2



def eval_predict(prices, checkpoints, bars_later, expect):
	result = list()
	for N in checkpoints:
		if prices[N+bars_later] >= prices[N]*expect:
			result.append(1)
		else:
			result.append(0)
	return result


def macd_crossover(close, fast, slow, signal):
	result = list()
	close = np.array([float(i) for i in close])
	macd,macdsignal,macdhist = talib.MACD(close, fastperiod=fast, slowperiod=slow, signalperiod=signal)
	if (len(macdhist))>=(slow + signal - 1):  #ensure macdhist is mature enough
		for N in range(len(macdhist)-1):
			if (macdhist[N+1] >= 0) & (macdhist[N] < 0):
				result.append(N)
#				result.append((N,'gold'))
#			elif (macdhist[-(N+2)] >= 0) & (macdhist[-(N+1)] < 0):
#				result.append((N,'dead'))

	return result

'''

def find_best_macd_combo(coin_pairs, granularity, exchange, bars_later, expect):
	coin_pair_closes = [[coin_pair, get_close(get_X(coin_pair[0], coin_pair[1], granularity, exchange))] for coin_pair in coin_pairs]
	fast_ar = np.arange(2,30,1)
	slow_ar = np.arange(10,60,1)
	sig_ar = np.arange(2,30,1)
	combo_accuracy = dict()
	for combo in list(itertools.product(fast_ar, slow_ar, sig_ar)):
		print combo
		coinpair_N_correct = dict()
		for coin_pair_close in coin_pair_closes:
			coin_pair = coin_pair_close[0]
			close = coin_pair_close[1]
			crossover = macd_crossover(close, combo[0], combo[1], combo[2])
			coinpair_N_correct[tuple(coin_pair)] = eval_predict(close, crossover, bars_later, expect)
			#np.concatenate((np.repeat(coin_pair, N_correct.shape[0]).T, N_correct), axis=1)
			#np.c_[(np.repeat(coin_pair, N_correct.shape[0])).T, N_correct]   #same as concatenate, or column_stack

		combo_accuracy[combo] = coinpair_N_correct
	#combo_accuracy = np.array(combo_accuracy)
	#max_index = np.argmax(combo_accuracy, axis=0)[1]
	
	df = pd.DataFrame(combo_accuracy)
	result = df.unstack(level=[0,1]).apply(pd.Series).stack().groupby(level=[0,1,2]).mean().sort_values()

	print result[:10]
	print result[-10:]
	#df = pd.DataFrame(combo_accuracy, columns = ['fast_slow_sig', 'accuracy'])
	#print df.groupby(['fast_slow_sig'])['correct'].mean().sort_values(ascending=False)[:3]
	#print df.groupby(['fast_slow_sig'])['correct'].mean().sort_values(ascending=True)[:3]
	return 0

'''
def find_best_macd_combo(coin_pairs, granularity, exchange, bars_later, expect):
	df = pd.DataFrame()
	#closes = [get_close(get_X(coin_pair[0], coin_pair[1], granularity, exchange)) for coin_pair in coin_pairs]
	closes = list()
	for coin_pair in coin_pairs:
		X = get_X(coin_pair[0], coin_pair[1], granularity, exchange)
		closes.append(get_close(X))

	fast_ar = np.arange(2,60,1)
	slow_ar = np.arange(2,100,1)
	sig_ar = np.arange(2,60,1)
	combo_accuracy = list()
	for combo in list(itertools.product(fast_ar, slow_ar, sig_ar)):
		print combo
		coinpair_N_correct = list()
		for close in closes:
			crossover = macd_crossover(close, combo[0], combo[1], combo[2])
			N_correct = eval_predict(close, crossover, bars_later, expect)
			coinpair_N_correct = coinpair_N_correct + N_correct
			#np.concatenate((np.repeat(coin_pair, N_correct.shape[0]).T, N_correct), axis=1)
			#np.c_[(np.repeat(coin_pair, N_correct.shape[0])).T, N_correct]   #same as concatenate, or column_stack
		if (len(coinpair_N_correct)>0):
			combo_correct_avg = float(sum(coinpair_N_correct))/len(coinpair_N_correct)
			combo_accuracy.append([combo,combo_correct_avg])
	combo_accuracy = np.array(combo_accuracy)
	max_index = np.argmax(combo_accuracy, axis=0)[1]

	print combo_accuracy[max_index]
	#df = pd.DataFrame(combo_accuracy, columns = ['fast_slow_sig', 'accuracy'])
	#print df.groupby(['fast_slow_sig'])['correct'].mean().sort_values(ascending=False)[:3]
	#print df.groupby(['fast_slow_sig'])['correct'].mean().sort_values(ascending=True)[:3]
	return combo_accuracy[max_index]

#find_break_tank(get_X('ETH', 'BTC', 'day', 'bittrex'), 10, 2)

#print HHV(get_X('ETH', 'BTC', 'day', 'bittrex'), 10)
#print LLV(get_X('ETH', 'BTC', 'day', 'bittrex'), 10)

#result = read_coin_close('BTC', 'USD')

def find_close_to_boundary(granularity, exchange):
	pairs = read_coin_pair(granularity, exchange)
	result_set = set()
	for fsym, tsym in pairs:
		X = get_X(fsym, tsym, granularity, exchange)
		price_resistance =  find_price_boundary(fsym, tsym, 'high', granularity,0.95,1.05,exchange)
		high = REF(get_high(X),1)
		for r in price_resistance:
			if (high >= r*0.95) & (high <= r*1.05):
				result_set.add(('resistance', fsym, tsym))
		price_support =  find_price_boundary(fsym, tsym, 'low', granularity,0.95,1.05,exchange)
		low = REF(get_low(X),1)
		for r in price_support:
			if (low >= r*0.95) & (low <= r*1.05):
				result_set.add(('support', fsym, tsym))
	result = list(result_set).sort(key=itemgetter(0,1))
	return result


timer = ctime()




find_best_macd_combo(read_coin_pair('day', 'bittrex'), 'day', 'bittrex', 1, 1.1)

'''
print 'this is bittrex close to boundary.................'
print find_close_to_boundary('day', 'bittrex')
print 'this is yobit.................'
print find_close_to_boundary('day', 'yobit')
'''

'''
print 'find new high on bittrex....................'
result1 = find_new_high('day', 0.9, 1, 3650, 0, 'bittrex')
if len(result1) > 0:
	print result1
else:
	print 'find nothing........'

print 'find new high on yobit....................'
result2 = find_new_high('day', 0.9, 1, 3650, 0, 'yobit')
if len(result2) > 0:
	print result2
else:
	print 'find nothing........'

print 'print intersection result........'
print join_result(result1[:,:2], result2[:,:2])
'''


print 'bittrex day macd gold cross: '
find_macd_gold_cross('day', 'bittrex')
print 'yobit day macd gold cross: '
find_macd_gold_cross('day', 'yobit')

print 'bittrex hour macd gold cross: '
find_macd_gold_cross('hour', 'bittrex')
print 'yobit hour macd gold cross: '
find_macd_gold_cross('hour', 'yobit')

#REF(HHV(H,20)/LLV(L,30)<=2,1)




'''
plot(time, macd)
plot(time, macdsignal)
plot(time, macdhist)

pl.show()
'''


timer = timer + ' to: ' + ctime()
print timer
