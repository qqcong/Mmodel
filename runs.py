# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 09:41:44 2017

@author: Amon
"""
import sys
import numpy as np
import datetime
import time
import logging
import os

import exchange_data as ed
import make_stastic
import run_model

reload(sys)   
sys.setdefaultencoding('utf-8')

from libfile import pysql
from libfile import Log

#文件路径
def getTheFile(filename):
    return os.path.abspath(os.path.dirname(__file__)) +"/"+filename

#初始化开始时间
def init_time():
	f = open(getTheFile('libfile/start_time.txt'),'rb')
	times = f.readline()
	f.flush()
	f.close()
	return times

#记录当前时间
def save_time(times):
	f = open(getTheFile('libfile/start_time.txt'),'w')
	f.write(times)
	f.flush()
	f.close()

#生成日志
def init_logs(time):
	log_file = str(time.date()) + '.log' 
	log = Log.Logger(str(time.date()), getTheFile('logs/'+log_file))
	return log

#对有问题的user_id的记录
def default(user_id):
	f = open(getTheFile('logs/default.txt'),'a')
	for uu in user_id:
		f.write(uu + '\n')
	f.flush()
	f.close()

#主程序
def main(function = 'logistic'):

	wait_pipe = [] #存放待处理的用户
	no_data_pipe = [] #存放暂无数据的用户
	start_time = init_time()
	s_time = datetime.datetime.strptime(start_time,'%Y-%m-%d %H:%M:%S')
	log_time = None
	while True:
		#更新日志文件
		if log_time != s_time.date():
			log = init_logs(s_time)
			log_time = s_time.date()

		e_time = s_time + datetime.timedelta(minutes=10)
		print 'Current detection time: {}'.format(str(e_time))
		print 'Remaining number of users: {}'.format(len(wait_pipe))
		print 'Remaining number in no data: {}'.format(len(no_data_pipe))

		if e_time > datetime.datetime.now():
			wait_pipe.extend(no_data_pipe)
			no_data_pipe = []
			time.sleep(600)
		else:
			try:
				#根据小时单位取用户数据（每个10分钟进行检测）
				add_user = ed.wait_audit_user(s_time.strftime("%Y-%m-%d %H:%M"),e_time.strftime("%Y-%m-%d %H:%M"))
				s_time = e_time
				if add_user.empty:
					log.log('Waiting data is none','info')
					continue
				wait_pipe.extend(list(add_user['user_id']))
				save_time(s_time.strftime("%Y-%m-%d %H:%M:%M"))
			except Exception,e:
				log.log('Waiting data is disconnect','warning')
				time.sleep(60)
		try:
			while wait_pipe:
				#分批次处理
				if len(wait_pipe) > 20 :
					user_id = wait_pipe[:20]
					wait_pipe = wait_pipe[20:]
				else:
					user_id = wait_pipe
					wait_pipe = []

				#获取运营商数据
				rawdata = ed.extact_third_rawdata(user_id)
				if rawdata.empty:
					log.log('Operator data is none','warning')
					no_data_pipe.extend(user_id)
					continue
				#获取用户信息
				user = ed.extract_user_info(user_id)
				if user.empty:
					log.log('Users register data is none','warning')
					no_data_pipe.extend(user_id)
					continue

				try:
					#模型数据计算和跑模型
					modeldata = make_stastic.model_data_group(user,rawdata)
					user_ids,fraud,prop = run_model.train_model_group(modeldata,function)
					ac_time = [s_time] * len(user_ids)
					now_time = [datetime.datetime.now()] * len(user_ids)
					function_list = [function] * len(user_ids)

					#插入数据
					user_score = zip(user_ids,prop[:,1],ac_time,now_time,function_list)
					status1 = ed.insert_score(user_score)

					stata_data = [str(modeldata.iloc[x].to_dict()) for x in range(len(modeldata))]
					user_stastic = zip(user_ids,stata_data,ac_time,now_time)
					status2 = ed.insert_static(user_stastic)

					if status1 == 0 and status2 == 0:
						log.log('数据插入成功({})！'.format(len(user_ids)),'info')
						no_data_pipe.extend(list(set(user_id)-set(user_ids)))
					else:
						log.log('数据插入失败！','waring')
						default(user_id)

				except Exception,e:
					print user_id
					log.log("Model calculate Error : %s" %(e),'warning')
					default(user_id)

		except Exception,e:
			log.log("Run Error : %s" %(e),'warning')


if __name__ == '__main__':
	main()