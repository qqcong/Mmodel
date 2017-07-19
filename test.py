# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 09:41:44 2017

@author: Amon
"""
import sys
import joblib
import numpy as np
import datetime
import logging
import os

import exchange_data as ed
import make_stastic
import run_model


def main():
	user_id = [u'372dd20c64de432fa53db96f2cdbdcfc', u'0b77d327709a4e2f92d25a88c1b1079e', u'71cefbe803894d7bb34cc50a7d249afe', u'11c8437e670d4815819d42460493d6c0', u'7d1a937110064eb3bf7324ddd1670be0', u'a4cf074fc1544a6288a9bd91b200e7af', u'd6c693ed3b1d45cdac58eb33c042eaab', u'fbb50245eda84d409d10d115e8fc307e', u'9bba3e71289a46c283b029714786486e', u'e585f4b8b13146c2bc361df8bf2e8b1c', u'f5e4828c8f7b4708bce5b04d81757078', u'eb63abdd99ce4be2a59f6eed81e3e57e', u'd22690b5540e41ad98d332adc426c44d', u'f3000b0a61d64b2fb755ec33463927c9', u'f30701e1c43f4f1781f86569b5bc57fa', u'476a0c4283924bb18650e9dd1c81dea1', u'4ee5a3271eae4c3aaacf94df6ff3a933', u'3e8f0c53b10d477cbe0f89bb571de52c', u'33a579e6b85d467e9e494a250c810c81', u'cc61e1e0ad244fb08f5e4837bfa2256a']

	rawdata = ed.extact_third_rawdata(user_id)
	if rawdata.empty:
		logging.warning('获取到运营商数据为空')
	user = ed.extract_user_info(user_id)
	if user.empty:
		logging.warning('获取到用户信息数据为空')
	modeldata = make_stastic.model_data_group(user,rawdata)
	user_id,fraud,prop = run_model.train_model_group(modeldata,'logistic')
	print fraud
	print prop


if __name__ == '__main__':
	main()
