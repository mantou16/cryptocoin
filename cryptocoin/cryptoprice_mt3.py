'''
Created on Dec 30, 2017

@author: zmei
'''

import  threading
from threading import current_thread
import Queue
import time
import datetime
import requests
import os
from time import ctime
import MySQLdb as mysql

cold_coin_queue = Queue.Queue()
num_thread = 10
coin_short_list_queue = Queue.Queue()

def db_init():
	conn = mysql.connect(user = 'root', passwd = 'root', host = '127.0.0.1', db='cc')
	cursor = conn.cursor()
	sql = "truncate table coin_price_day_new"
	cursor.execute(sql)
	cursor.close()
	conn.commit()
	conn.close()

def get_full_coin_list():
	api = 'https://min-api.cryptocompare.com/data/all/coinlist'
	json = requests.get(api).json()
	coin_data = json['Data']
	
	coin_keys = coin_data.keys()
	coin_symbols = list()
	for coin in coin_keys:
		Symbol = coin_data.get(coin).get('Symbol')
		coin_symbols.append(Symbol)
	
	return coin_symbols

def find_cold_coins(fsym_list):
	cold_coin_list = list()
	fsym = ','.join(fsym_list)
	api = 'https://min-api.cryptocompare.com/data/pricemultifull?fsyms='+fsym+'&tsyms=USD,BTC'
	json = requests.get(api).json().get('RAW')
	for coin in fsym_list:
		coin_price = json.get(coin,'None')
		if coin_price != 'None':
			coin_usd_result = coin_price.get('USD', dict())
			coin_btc_result = coin_price.get('BTC', dict())
			usd_volume = coin_usd_result.get('TOTALVOLUME24HTO', 0)
			btc_volume = coin_btc_result.get('TOTALVOLUME24HTO', 0)
			
			if usd_volume < 5000 and btc_volume < 0.5:
				cold_coin_list.append(coin)
				print 'cold coin: ' + coin
		else:
			cold_coin_list.append(coin)
			print 'cold coin: ' + coin
	cold_coin_queue.put(cold_coin_list)
	
def get_short_coin_list():
	short_list_file = 'coin_short_list_test.csv'
	if (os.path.isfile(short_list_file)):
		with open(short_list_file, 'r') as inf:
			coin_short_list = inf.read().splitlines()
	else:
		coin_full_list = get_full_coin_list()
		coin_short_list = coin_full_list
		bucket_num = 40
		coin_list_size = len(coin_full_list)
		bucket_size = coin_list_size/bucket_num+1
		fsym_list_buckets = list()
		for i in range(bucket_num):
			fsym_list = coin_full_list[i*bucket_size:(i+1)*bucket_size]
			fsym_list_buckets.append(fsym_list)
			
		threads = [threading.Thread(target=find_cold_coins, args=(fsym_list,)) for fsym_list in fsym_list_buckets]
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
		
		print cold_coin_queue.qsize()
		cold_coin_list = list()
		while not cold_coin_queue.empty():
			cold_coin_list+= cold_coin_queue.get()
		print 'cold coin list size: ' + str(len(cold_coin_list))
		coin_short_list = [fsym for fsym in coin_full_list if fsym not in cold_coin_list]

		with open(short_list_file, 'w') as outf:
			outf.write('\n'.join(coin_short_list))
			print 'short list is done'
		
	return coin_short_list

def get_coin_price_day(coin_short_list_queue):
	while True:  # so every thread will continue get more coin to process, instead of only execute once, until all threads task_done make all queue tasks as done. if some code below failed, it will impact task_done
		fsym = coin_short_list_queue.get()
		tsym_list = ['USD','BTC','ETH']
		for tsym in tsym_list:
			if fsym != tsym:    # if fsym == tsym, json response in call_api will be 'error', so coin_price_day_data will be none.
				base_api = 'https://min-api.cryptocompare.com/data/histoday'
				parameters = 'fsym=' + fsym + '&tsym='+tsym+'&aggregate=1&e=CCCAGG&allData=true'
				api = base_api + '?' + parameters
				json = requests.get(api).json()
				if json['Response']=='Success':
					coin_price_day_data = json['Data']
					for price in coin_price_day_data:
						price.update({'fsym' : fsym, 'tsym' : tsym})
						price['time'] = time.strftime("%Y-%m-%d",time.gmtime(price['time']-86400))
				if coin_price_day_data is not None:
					conn = mysql.connect(user = 'root', passwd = 'root', host = '127.0.0.1', db='cc')
					cursor = conn.cursor()
					sql = "insert into coin_price_day_new (fsym, tsym, time, volumefrom, volumeto, open, close, high, low) values (%(fsym)s, %(tsym)s, %(time)s, %(volumefrom)s, %(volumeto)s, %(open)s, %(close)s, %(high)s, %(low)s)"
					rows = cursor.executemany(sql, coin_price_day_data)
					cursor.close()
					conn.commit()
					conn.close()
					print current_thread().name + ': ' + fsym + ' : ' + tsym + ' end... rows: ' + str(rows)
		coin_short_list_queue.task_done()

#def get_coin_price_hour():
	
def get_coin_price():
	for coin in get_short_coin_list():
	#for coin in ['COSS']:
		coin_short_list_queue.put(coin)
	for i in range(num_thread):
		thread = threading.Thread(target=get_coin_price_day, args=(coin_short_list_queue,))
		thread.setDaemon(True)
		thread.start()
		#thread.join()   # should not join here, NOT within the SAME loop, it will hold the process to wait 1st process to start and finish and then 2nd round of loop. should start all threads in a loop and join each of them in a different loop
	#print 'start get coin price function: ', ctime()
	coin_short_list_queue.join()   #use queue to hold the process, if unfinish task is not 0
	print 'end get coin price funtion: ', ctime()

timer = ctime()
db_init()
get_coin_price()
timer = timer + ' to: ' + ctime()
print timer




