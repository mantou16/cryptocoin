'''
Created on Dec 30, 2017

@author: zmei
'''

import  threading
from threading import current_thread
import Queue
import time
import requests
from time import ctime
import MySQLdb as mysql


coin_short_list_queue = Queue.Queue()
coin_price_multi_queue = Queue.Queue()
num_thread = 15

def fetch_coin_profile():
	api = 'https://min-api.cryptocompare.com/data/all/coinlist'
	json = requests.get(api).json()
	data = json['Data']
	#wanted_keys = {'Id', 'Symbol', 'CoinName', 'Algorithm', 'ProofType','FullyPremined','TotalCoinSupply'}
	wanted_keys = {'Id', 'Symbol',  'Algorithm', 'ProofType','FullyPremined'}
	coin_profile_list = list()
	for coin_data in data.values():
		coin_profile_dict = {key:value for key, value in coin_data.items() if key in wanted_keys}
		#if (coin_profile_dict['TotalCoinSupply'] == 'N/A') | (coin_profile_dict['TotalCoinSupply'] == 'None') :
		#	coin_profile_dict['TotalCoinSupply'] = None
		#totalcoinsupply = coin_profile_dict['TotalCoinSupply']
		#coin_profile_dict['TotalCoinSupply'] = unicode(totalcoinsupply).replace(',' , '') #some number has comma in it as separator, remove it so can insert to DB successfully
		coin_profile_list.append(coin_profile_dict)

	return coin_profile_list

def write_coin_profile(coin_profile_list):
	conn = init_connection()
	cursor = conn.cursor()
	table_name = 'coin_profile'
	sql = 'insert into ' + table_name + ' (id, symbol, algorithm, prooftype, fullypremined) values (%(Id)s, %(Symbol)s, %(Algorithm)s, %(ProofType)s, %(FullyPremined)s)'

	rows = cursor.executemany(sql, coin_profile_list)
	cursor.close()
	conn.commit()
	conn.close()
	return rows
	
''' no need snapshot yet
def fetch_coin_snapshot():
	ids = read_coin_full_id()
	for id in ids:
		api = 'https://www.cryptocompare.com/api/data/coinsnapshotfullbyid/?id=' + id
	json = requests.get(api).json()
	data = json['Data']
	wanted_keys = {'Id', 'Symbol', 'CoinName', 'Algorithm', 'ProofType','FullyPremined','TotalCoinSupply'}
	coin_profile_list = list()
	for coin_data in data.values():
		coin_profile_dict = {key:value for key, value in coin_data.items() if key in wanted_keys}
		coin_profile_list.append(coin_profile_dict)
			
	return coin_profile_list

def write_coin_snapshot():
'''

def write_coin_price_hist(granularity, price_data):
	conn = init_connection()
	cursor = conn.cursor()
	table_name = 'coin_price_'+granularity
	sql = 'insert into ' + table_name + ' (fsym, tsym, time, volumefrom, volumeto, open, close, high, low) values (%(fsym)s, %(tsym)s, %(time)s, %(volumefrom)s, %(volumeto)s, %(open)s, %(close)s, %(high)s, %(low)s)'
	rows = cursor.executemany(sql, price_data)
	cursor.close()
	conn.commit()
	conn.close()
	return rows

def call_price_hist_api(api, granularity, fsym, tsym):
	print api
	json = requests.get(api).json()
	coin_price_day_data = []
	if json['Response']=='Success':
		if json['ConversionType']['type']=='direct':  # only deal with direct price pair, indirectly ones will not only increase data volume and analysis mistake
			coin_price_day_data = json['Data']
			for price in coin_price_day_data:
				price.update({'fsym' : fsym, 'tsym' : tsym})
				if granularity == 'day':
					price['time'] = time.strftime("%Y-%m-%d",time.gmtime(price['time']-86400))
				elif granularity == 'hour':
					price['time'] = time.strftime("%Y-%m-%d %H",time.gmtime(price['time']))
				elif granularity == 'minute':
					price['time'] = time.strftime("%Y-%m-%d %H:%M",time.gmtime(price['time']))
	return coin_price_day_data   # if json response is not success, it will be none

def fetch_coin_price_worker(coin_short_list_queue,granularity,limit):
	while True:  # so every thread will continue get more coin to process, instead of only execute once, until all threads task_done make all queue tasks as done. if some code below failed, it will impact task_done
		fsym = coin_short_list_queue.get()
		real_limit = str()
		tsym_list = ['USD','BTC','ETH']
		if granularity == 'day':
			if limit == 'all':
				real_limit = 'allData=true'
			else:
				real_limit = 'limit=30'
		if granularity == 'hour':
			if limit == 'all':
				real_limit = 'limit=168' #use API's default range, in case of mistake
			else:
				real_limit = 'limit='+limit

		for tsym in tsym_list:
			base_api = 'https://min-api.cryptocompare.com/data/histo' + granularity
			parameters = 'fsym=' + fsym + '&tsym='+tsym+'&aggregate=1&e=CCCAGG&' + real_limit
			api = base_api + '?' + parameters
			price_data = call_price_hist_api(api, granularity, fsym, tsym)
			rows = write_coin_price_hist(granularity, price_data)
			print current_thread().name, ': ', fsym, ' : ', tsym, ' end... rows: ', rows
		coin_short_list_queue.task_done()
	
def fetch_coin_price(granularity,limit):
	for coin in read_coin_short_list():
	#for coin in ['BTC', 'SKY', 'ZET']:
		coin_short_list_queue.put(coin)
	for i in range(num_thread):
		thread = threading.Thread(target=fetch_coin_price_worker, args=(coin_short_list_queue,granularity,limit))
		thread.setDaemon(True)
		thread.start()
		#thread.join()   # should not join here, NOT within the SAME loop, it will hold the process to wait 1st process to start and finish and then 2nd round of loop. should start all threads in a loop and join each of them in a different loop
	#print 'start get coin price function: ', ctime()
	coin_short_list_queue.join()   #use queue to hold the process, if unfinish task is not 0
	print 'end get coin price funtion: ', ctime()


def read_coin_full_id():
	conn = init_connection()
	cursor = conn.cursor()
	sql = 'select distinct id from coin_profile'
	rows = cursor.execute(sql)
	result = [item[0] for item in cursor.fetchall()]
	cursor.close()
	conn.commit()
	conn.close()
	
	return result
	
def read_coin_full_symbol():
	conn = init_connection()
	cursor = conn.cursor()
	sql = 'select distinct symbol from coin_profile'
	cursor.execute(sql)
	result = [item[0] for item in cursor.fetchall()]
	#result = cursor.fetchmany(100)
	cursor.close()
	conn.commit()
	conn.close()
	
	return result
	
def read_coin_short_list():
	conn = init_connection()
	cursor = conn.cursor()
	sql = 'select distinct fsym from coin_price_multifull where tsym = %s and TOTALVOLUME24HTO > %s'
	rows = cursor.execute(sql, ('BTC',1))
	result = [item[0] for item in cursor.fetchall()]
	#result = cursor.fetchmany(100)
	cursor.close()
	conn.commit()
	conn.close()
	return result
	
def clear_coin_all():
	clear_coin_profile()
	#clear_coin_snapshot()
	clear_coin_price_multifull()
	clear_coin_price()
	
	
def clear_coin_profile():
	clear_table('coin_profile')

def clear_coin_snapshot():
	clear_table('coin_snapshot')

def clear_coin_price_multifull():
	clear_table('coin_price_multifull')

def clear_coin_price():
	clear_coin_price_day()
	clear_coin_price_hour()
	
def clear_coin_price_day():
	clear_table('coin_price_day')

def clear_coin_price_hour():
	clear_table('coin_price_hour')

def clear_table(table_name):
	conn = init_connection()
	cursor = conn.cursor()
	sql = 'truncate table ' + table_name
	cursor.execute(sql)
	cursor.close()
	conn.commit()
	conn.close()

def init_connection():
	conn = mysql.connect(user = 'root', passwd = 'root', host = '127.0.0.1', db='cc')
	return conn

def fetch_coin_price_multi_worker(fsym_list):
	coin_price_multi = list()
	fsyms = ','.join(fsym_list)
	api = 'https://min-api.cryptocompare.com/data/pricemultifull?fsyms='+fsyms+'&tsyms=USD,BTC,ETH,EUR,KWR'
	json = requests.get(api).json().get('RAW')
	if json is not None:
		for fsym in json.keys():
			for tsym in json.get(fsym).keys():
				coin_price = json.get(fsym).get(tsym)
				coin_price.update({'fsym' : fsym, 'tsym' : tsym})
				coin_price_multi.append(coin_price)
	else:
		print json
	coin_price_multi_queue.put(coin_price_multi)

	
def fetch_coin_price_multi():
	coin_full_list = read_coin_full_symbol()
	bucket_num = 40
	coin_list_size = len(coin_full_list)
	bucket_size = coin_list_size/bucket_num+1
	fsym_list_buckets = list()
	for i in range(bucket_num):
		fsym_list = coin_full_list[i*bucket_size:(i+1)*bucket_size]
		fsym_list_buckets.append(fsym_list)
	threads = [threading.Thread(target=fetch_coin_price_multi_worker, args=(fsym_list,)) for fsym_list in fsym_list_buckets]
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()

	print coin_price_multi_queue.qsize()

	
def write_coin_price_multi():

	coin_price_multi = list()
	while not coin_price_multi_queue.empty():
		coin_price_multi+= coin_price_multi_queue.get()

	print 'coin price multi size: ', len(coin_price_multi)
	# write to table cc.coin_price_multi
	conn = init_connection()
	cursor = conn.cursor()
	sql = ('insert into coin_price_multifull '
				'(fsym, tsym, PRICE, TOTALVOLUME24H, TOTALVOLUME24HTO)'
		 		' values ' 
		 		'(%(fsym)s, %(tsym)s, %(PRICE)s, %(TOTALVOLUME24H)s, %(TOTALVOLUME24HTO)s)'
		 )
	'''
	sql = ('insert into coin_price_multi '
				'(fsym, tsym, PRICE,LASTUPDATE,LASTVOLUME,LASTVOLUMETO,VOLUMEDAY,VOLUMEDAYTO,VOLUME24HOUR,VOLUME24HOURTO,OPENDAY,HIGHDAY,LOWDAY,OPEN24HOUR,HIGH24HOUR,LOW24HOUR,LASTMARKET,CHANGE24HOUR,CHANGEPCT24HOUR,CHANGEDAY,CHANGEPCTDAY,SUPPLY,MKTCAP,TOTALVOLUME24H,TOTALVOLUME24HTO)'
		 		' values ' 
		 		'(%(fsym)s, %(tsym)s, %(PRICE)s, %(LASTUPDATE)s, %(LASTVOLUME)s, %(LASTVOLUMETO)s, %(VOLUMEDAY)s, %(VOLUMEDAYTO)s, %(VOLUME24HOUR)s, %(VOLUME24HOURTO)s, %(OPENDAY)s, %(HIGHDAY)s, %(LOWDAY)s, %(OPEN24HOUR)s, %(HIGH24HOUR)s, %(LOW24HOUR)s, %(LASTMARKET)s, %(CHANGE24HOUR)s, %(CHANGEPCT24HOUR)s, %(CHANGEDAY)s, %(CHANGEPCTDAY)s, %(SUPPLY)s, %(MKTCAP)s, %(TOTALVOLUME24H)s, %(TOTALVOLUME24HTO)s)'
		 )
		'''
	for i in coin_price_multi:
		print i
		rows = cursor.execute(sql, i)
	#rows = cursor.executemany(sql, coin_price_multi)
	print 'insert done, row number: ', rows
	cursor.close()
	conn.commit()
	conn.close()
'''
def find_cold_coins(fsym_list):
	cold_coin_list = list()
	fsym = ','.join(fsym_list)
	api = 'https://min-api.cryptocompare.com/data/pricemultifull?fsyms='+fsym+'&tsyms=USD,BTC,ETH,EUR,KWR,&tryConversion=false'
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




def fetch_coin_price_day(coin_short_list_queue):
	while True:  # so every thread will continue get more coin to process, instead of only execute once, until all threads task_done make all queue tasks as done. if some code below failed, it will impact task_done
		fsym = coin_short_list_queue.get()
		tsym_list = ['USD','BTC','ETH']
		for tsym in tsym_list:
			base_api = 'https://min-api.cryptocompare.com/data/histoday'
			parameters = 'fsym=' + fsym + '&tsym='+tsym+'&aggregate=1&e=CCCAGG&allData=true'
			api = base_api + '?' + parameters
			price_data = call_api(api, fsym, tsym)
			rows = write_to_db(price_data)
			print current_thread().name + ': ' + fsym + ' : ' + tsym + ' end... rows: ' + str(rows)
		coin_short_list_queue.task_done()

#def get_coin_price_hour():
	
def get_coin_price():
	for coin in get_short_coin_list():
	#for coin in ['BTC', 'SKY', 'ZET']:
		coin_short_list_queue.put(coin)
	for i in range(num_thread):
		thread = threading.Thread(target=fetch_coin_price_day, args=(coin_short_list_queue,))
		thread.setDaemon(True)
		thread.start()
		#thread.join()   # should not join here, NOT within the SAME loop, it will hold the process to wait 1st process to start and finish and then 2nd round of loop. should start all threads in a loop and join each of them in a different loop
	#print 'start get coin price function: ', ctime()
	coin_short_list_queue.join()   #use queue to hold the process, if unfinish task is not 0
	print 'end get coin price funtion: ', ctime()

'''

timer = ctime()
#clear_coin_all()

#clear_coin_profile()
#write_coin_profile(fetch_coin_profile())

#clear_coin_price_multifull()
#fetch_coin_price_multi()
#write_coin_price_multi()

clear_coin_price_day()
fetch_coin_price('day', 'all')

#clear_coin_price_hour()
#fetch_coin_price('hour', '336')  # 336, 2 weeks data

timer = timer + ' to: ' + ctime()
print timer

# to be resolved problem
# 1. price multi full many other columns are not inserted because of db type
# 2. coin profile coin name unicode problem, some has non-unicode in name


