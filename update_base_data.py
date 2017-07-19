# -*- coding: utf-8 -*-
# 
import MySQLdb
import pandas as pd
import os

from libfile import pysql

def getTheFile(filename):
    return os.path.abspath(os.path.dirname(__file__)) +"/"+filename

def main():

	#黑名单
	sql1 = """
		select * from phone_black_list
	"""
	#黄牛党
	sql2 = """
		select * from phone_blacklist_fraud_group
	"""
	#消费金融
	sql3 = """
		select * from risk_number_label
	"""
	try:
		conn = pysql.conn_mysql()

		data = pd.read_sql_query(sql1,con = conn)
		data.to_csv(getTheFile('data/phone_black_list.csv'),index=False,encoding='utf_8_sig')

		data = pd.read_sql_query(sql2,con = conn)
		data.to_csv(getTheFile('data/phone_blacklist_fraud_group.csv'),index=False,encoding='utf_8_sig')

		data = pd.read_sql_query(sql3,con = conn)
		data.to_csv(getTheFile('data/risk_number_label.csv'),index=False,encoding='utf_8_sig')

		conn.close()
	except Exception,e:
		print "update_: Mysql Error %d: %s" % (e.args[0], e.args[1])

if __name__ == '__main__':
	main()