'''
Created on Jan 3, 2018

@author: zmei
'''
from os.path import os
import MySQLdb as mysql
import requests
from collections import Counter

def read_coins_from_file(coin_file):
	if (os.path.isfile(coin_file)):
		with open(coin_file, 'r') as inf:
			coin_list = inf.read().splitlines()
			return coin_list[::3]   #pick all 1, 4, 7, 10... x[startAt:endBefore:skip], pick items from list x, within range start~end, and skip every 'skip' item.

def read_coins_from_bittrex_api(api):
	json = requests.get(api).json()
	coin_pair_list = json['result']
	print 'bittrex coin pair size: ', len(coin_pair_list)
	coin_pair = list()
	for item in coin_pair_list:
		if item['BaseCurrency'] == 'USDT':
			item['BaseCurrency'] = 'USD'
		coin_pair.append([item['MarketCurrency'], item['BaseCurrency']])
	return coin_pair

def read_coins_from_yobit_api(api):
	json = requests.get(api).json()
	data = json['pairs']
	coin_pair_list = data.keys()
	print 'yobit coin pair size: ', len(coin_pair_list)
	#fsym = list()
	#tsym = list()
	coin_pair = list()
	for item in coin_pair_list:
		coin_pair.append(item.split('_'))
		#fsym.append(coin_pair.split('_')[0].upper())
		#tsym.append(coin_pair.split('_')[1].upper())
	#print Counter(fsym)  # like SQL, count(*) group by
	#print Counter(tsym)
	return coin_pair
	#return set(fsym,)  #remove duplicate fsym coin to avoid duplicate primary key when inserting to DB.

def clear_exchange_coins(exchange):
	conn = mysql.connect(user = 'root', passwd = 'root', host = '127.0.0.1', db='cc')
	cursor = conn.cursor()
	sql = 'delete from coin_exchange where exchange = %s'
	cursor.execute(sql, (exchange,))
	cursor.close()
	conn.commit()
	conn.close()

def write_coin_exchange(exchange, coin_pair_list):
	conn = mysql.connect(user = 'root', passwd = 'root', host = '127.0.0.1', db='cc')
	cursor = conn.cursor()
	values = [{'fsym':coin_pair[0], 'tsym':coin_pair[1], 'exchange': exchange} for coin_pair in coin_pair_list]
	sql = 'insert into coin_exchange (fsym, tsym, exchange) values (%(fsym)s, %(tsym)s, %(exchange)s)'
	rows = cursor.executemany(sql, values)
	print 'inserted row count: ', rows
	cursor.close()
	conn.commit()
	conn.close()

clear_exchange_coins('bittrex')
#write_coin_exchange('bittrex', read_coins_from_file('bittrex.txt'))
write_coin_exchange('bittrex', read_coins_from_bittrex_api('https://bittrex.com/api/v1.1/public/getmarkets'))

clear_exchange_coins('yobit')
write_coin_exchange('yobit', read_coins_from_yobit_api('https://yobit.net/api/3/info'))



