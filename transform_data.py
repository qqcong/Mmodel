# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 14:43:54 2017

@author: Amon
"""

import pandas as pd
import sys
import os

from libfile import model_config

reload(sys)   
sys.setdefaultencoding('utf-8')

def getTheFile(filename):
	
    return os.path.abspath(os.path.dirname(__file__)) +"/"+filename


def transform_with_woe(model_data):

	cut_point = model_config.logistic_cut
	for key in cut_point.keys():
		cutss = cut_point[key]['cut_point']
		wwoe = cut_point[key]['woe']
		model_data[key] = pd.cut(model_data[key],bins=cutss,labels=range(len(cutss) - 1)).map(lambda x:wwoe[x])

	return model_data

def main():
    transform_with_woe({})

if __name__ == '__main__':
    main()