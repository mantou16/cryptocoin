'''
Created on Dec 17, 2017

@author: zmei
'''
import csv
import requests
import sys
import json
import csv
import time
import datetime
import MySQLdb as mysql
import os
import threading

class cryptocoin:

	def __init__(self):
		self.coin_list = self.get_coin_list()
		self.conn = self.db_init()
			
	def get_coin_list(self):
		api = 'https://min-api.cryptocompare.com/data/all/coinlist'
		json = requests.get(api).json()
		coin_data = json['Data']
		
		coin_keys = coin_data.keys()
		coin_symbols = list()
		for coin in coin_keys:
			Symbol = coin_data.get(coin).get('Symbol')
			CoinName = coin_data.get(coin).get('CoinName')
			Algorithm = coin_data.get(coin).get('Algorithm')
			ProofType = coin_data.get(coin).get('ProofType')
			FullyPremined = coin_data.get(coin).get('FullyPremined')
			TotalCoinSupply = coin_data.get(coin).get('TotalCoinSupply')
			PreMinedValue = coin_data.get(coin).get('PreMinedValue')
			TotalCoinsFreeFloat = coin_data.get(coin).get('TotalCoinsFreeFloat')
			
			coin_symbols.append(Symbol)
		
#		print(coin_symbols)
		return coin_symbols
	
	def get_coin_short_list(self):
		#self.coin_list = ['BVC','SLS']
		short_list_file = 'coin_short_list.csv'
		if (os.path.isfile(short_list_file)):
			with open(short_list_file, 'r') as inf:
				coin_short_list = inf.read().splitlines()
		else:
			coin_short_list = list(self.coin_list)
			bucket_num = 40
			coin_list_size = len(self.coin_list)
			bucket_size = coin_list_size/bucket_num+1
			for i in range(bucket_num):
				fsym_list = self.coin_list[i*bucket_size:(i+1)*bucket_size]
				fsym = ','.join(fsym_list)
				api = 'https://min-api.cryptocompare.com/data/pricemultifull?fsyms='+fsym+'&tsyms=USD,BTC'
				json = requests.get(api).json().get('RAW')
				for coin in fsym_list:
					print coin
					coin_price = json.get(coin,'None')
					if coin_price != 'None':
						coin_usd_result = coin_price.get('USD', dict())
						coin_btc_result = coin_price.get('BTC', dict())
						usd_volume = coin_usd_result.get('TOTALVOLUME24HTO', 0)
						btc_volume = coin_btc_result.get('TOTALVOLUME24HTO', 0)
						
						print ('USD_volume: ' + str(usd_volume))
						print ('BTC_volume: ' + str(btc_volume))
						
						if usd_volume < 5000 and btc_volume < 0.5:
							coin_short_list.remove(coin)
					else:
						coin_short_list.remove(coin)
					print('current coin short list number: ' + str(len(coin_short_list)))
			with open('coin_short_list.csv', 'w') as outf:
					outf.write('\n'.join(coin_short_list))
		return coin_short_list
		
	def save_to_db(self, json, fsym, tsym):
		conn = mysql.connect(user = 'root', passwd = 'root', host = '127.0.0.1', db='cc')
		if json['Response']=='Success':
			coin_price_day_data = json['Data']
			for price in coin_price_day_data:
				price.update({'fsym' : fsym, 'tsym' : tsym})
				price['time'] = time.strftime("%Y-%m-%d",time.gmtime(price['time']-86400))
			cursor = conn.cursor()
			sql = "insert into coin_price_day_test (fsym, tsym, time, volumefrom, volumeto, open, close, high, low) values (%(fsym)s, %(tsym)s, %(time)s, %(volumefrom)s, %(volumeto)s, %(open)s, %(close)s, %(high)s, %(low)s)"
			cursor.executemany(sql, coin_price_day_data)
			#print('insert done: ' + fsym + '		' + tsym)
			cursor.close()
			conn.commit()
			conn.close()


	def call_api(self, api):
		#print 'start api: ' + api.get('api') +  time.strftime('%H:%M:%S') + "\n"
		json = requests.get(api.get('api')).json()
		#print 'end api: ' +  time.strftime('%H:%M:%S') + "\n"
		#print 'start db: '  +  time.strftime('%H:%M:%S') + "\n"		
		self.save_to_db(json, api.get('fsym'), api.get('tsym'))
		#print 'end db: ' +  time.strftime('%H:%M:%S') + "\n"

	def get_coin_price(self, apis):
		threads = [threading.Thread(target=self.call_api, args=(api,)) for api in apis]
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
		
		
	def db_init(self):
		conn = mysql.connect(user = 'root', passwd = 'root', host = '127.0.0.1', db='cc')
		cursor = conn.cursor()
		sql = "truncate table coin_price_day_test"
		cursor.execute(sql)
		cursor.close()
		conn.commit()
		conn.close()
		
		
	def get_coin_price_day(self):
		
		from_symbols = self.get_coin_short_list()
#		from_symbols = ['ADA','ADX']
		to_symbols = ['USD','BTC','ETH']
		
		#coin_symbols = ['LIFE']
		print ('coin short list number: ' + str(len(from_symbols)))
		#bucket_num = 10
		#bucket_size = len(from_symbols)/bucket_num

		header_flag = 0
		count = 0
		apis = list()
		#coin_symbols = ['BTC']
		for fsym in from_symbols:
			for tsym in to_symbols:
				#fsym = ','.join(coin_symbols[i*bucket_size:(i+1)*bucket_size])
				base_api = 'https://min-api.cryptocompare.com/data/histoday'
				parameters = 'fsym=' + fsym + '&tsym='+tsym+'&aggregate=1&e=CCCAGG&allData=true'
				api = base_api + '?' + parameters
				apis.append({'fsym':fsym,'tsym':tsym,'api':api})
				#print(str(count) + '    ' + fsym + '	' + tsym + '	' + api)
				count += 1
				
		self.get_coin_price(apis)
		
		print 'closing db connection ==================================================================='

if __name__ == '__main__':
	timer = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
	cc = cryptocoin()
	cc.get_coin_price_day()
	timer = timer + ' to: ' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
	print timer
	
	pass