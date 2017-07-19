# -*- coding: utf-8 -*-
# 
import MySQLdb
import sys
import pandas as pd
import logging

from libfile import pysql

reload(sys)   
sys.setdefaultencoding('utf-8')

def extact_third_rawdata(user_id):
	
	if type(user_id) == list:
		user_id = "','".join(user_id)
	sql = """select user_id,trade_type,trade_time,call_time,receive_phone,call_type,created_time from third_mobile_tel_data where call_time is not null and receive_phone is not null and user_id in ('{}')""".format(user_id)
	data = pd.DataFrame()
	try:
		conn = pysql.conn_mysql()
		data = pd.read_sql_query(sql,con = conn)
		conn.close()
	except MySQLdb.Error,e:
		logging.warning("third_rawdata: Mysql Error %d: %s" % (e.args[0], e.args[1]))
	return data

def extract_user_info(user_id):

	if type(user_id) == list:
		user_id = "','".join(user_id)
	sql = """select id 'user_id',name,id_num,phone,edu_level,marital_status,house_hold,company_nature,age,date_created from user where id in ('{}')""".format(user_id)
	data = pd.DataFrame()
	try:
		conn = pysql.conn_mysql()
		data = pd.read_sql_query(sql,con = conn)
		conn.close()
	except MySQLdb.Error,e:
		logging.warning("user_info: Mysql Error %d: %s" % (e.args[0], e.args[1]))
	return data

def wait_audit_user(start_time,end_time):
	
	sql = """
		select user_id,merchant_id from ci_cash_submit_apply WHERE create_date >= "{}" and create_date < "{}"
	"""
	sql = sql.format(start_time,end_time)
	try:
		conn = pysql.conn_mysql()
		data = pd.read_sql_query(sql,con = conn)
		conn.close()
	except MySQLdb.Error,e:
		logging.warning("wait_audit: Mysql Error %d: %s" % (e.args[0], e.args[1]))
	return data

def insert_score(user_score):

	insert_code = 0
	try:
		conn = pysql.conn_test()
		sql = 'insert into user_score(user_id, score, create_time, last_update, function) values(%s,%s,%s,%s,%s)'
		cursor = conn.cursor()
		cursor.executemany(sql,user_score)
		cursor.close()
		conn.commit()
		conn.close()
	except MySQLdb.Error,e:
		logging.warning("insert score: Mysql Error %d: %s" % (e.args[0], e.args[1]))
		insert_code = 1

	return insert_code

def insert_static(user_score):

	insert_code = 0
	try:
		conn = pysql.conn_test()
		sql = 'insert into user_stastic(user_id, stastic, create_time, last_update) values(%s,%s,%s,%s)'
		cursor = conn.cursor()
		cursor.executemany(sql,user_score)
		cursor.close()
		conn.commit()
		conn.close()
	except MySQLdb.Error,e:
		logging.warning("insert ststic: Mysql Error %d: %s" % (e.args[0], e.args[1]))
		insert_code = 1

	return insert_code
 

if __name__ == '__main__':
	pass

