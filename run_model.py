# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 09:41:44 2017

@author: Amon
"""

import pandas as pd
import numpy as np
import joblib
import sys
import os

from libfile import model_config
import transform_data

reload(sys)   
sys.setdefaultencoding('utf-8')

def getTheFile(filename):
    return os.path.abspath(os.path.dirname(__file__)) +"/"+filename

def train_model(model_info,function='logistic'):

    model = joblib.load(getTheFile('models/{}.m').format(function))
    x = np.array([model_info[key] for key in sorted(model_info.keys())]).reshape(1,-1)
    yt = model.predict(x)
    ytp = model.predict_proba(x)
    return yt,ytp

def train_model_group(model_info,function='logistic'):

    model = joblib.load(getTheFile('models/{}.m').format(function))
    base_info = model_config.model_dim
    user_id = model_info['user_id']
    del model_info['user_id']

    for key,value in base_info.items():
    	if key not in model_info.columns:
    		model_info[key] = value

    #model_info = transform_data.transform_with_woe(model_info)
    model_info = model_info[sorted(model_info.columns)]
    x = np.matrix(model_info)
    yt = model.predict(x)
    ytp = model.predict_proba(x)
    return user_id,yt,ytp

def main():
    train_model({})

if __name__ == '__main__':
    main()