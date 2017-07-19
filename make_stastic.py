# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 09:41:44 2017

@author: Amon
"""

import pandas as pd
import numpy as np
import datetime
from math import isnan
import os
import sys

from libfile import model_config

reload(sys)   
sys.setdefaultencoding('utf-8')

def getTheFile(filename):
    return os.path.abspath(os.path.dirname(__file__)) +"/"+filename

def model_data_group(user,base_data):

    all_data = []

    base_data = base_data.drop_duplicates(['user_id','call_time'])
    base_data = base_data[base_data['trade_time'] != 1800]
    base_data = base_data[base_data['receive_phone'].map(rm_null)]
    base_data = base_data[base_data['call_time'].map(rm_null)]
    base_data['day'] = [time.date() for time in base_data['call_time']]
    base_data['month'] = [time.month for time in base_data['call_time']]
    base_data['hour'] = [time.hour for time in base_data['call_time']]
    base_data['call_time'] = base_data['call_time'].astype('datetime64[ns]')
    base_data['created_time'] = base_data['created_time'].astype('datetime64[ns]')

    #1 与外部的联系
    #1.1 借贷情况
    #1.1.1 与消费金融公司联系的情况
    risk_list = pd.read_csv(getTheFile(r'data\risk_number_label.csv'),low_memory=False)
    risk_num = risk_list['number'][risk_list['label'].map(rm_null)]
    risk_num = risk_num[risk_num.map(lambda x:x.startswith('0')|x.startswith('400'))]
    risk_num = [x[:-2] for x in risk_num]             
    risk_num = set(risk_num)
    temp = base_data[base_data['receive_phone'].map(lambda x: x[:-2] in risk_num)]
    if not temp.empty:
        #分主被 次数
        t1111_t = temp[temp['call_type']=='1']
        if not t1111_t.empty:
            t1111_1 = pd.pivot_table(t1111_t,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
            t1111_1.columns = ['contacting_xj_call_num']
            all_data.append(t1111_1)
            #数量
            t1112_1 = t1111_t[-t1111_t.duplicated(['user_id','receive_phone','call_type'])]
            t1112_1 = pd.pivot_table(t1112_1,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
            t1112_1.columns = ['contacting_xj_num']
            all_data.append(t1112_1)

        
        #次数
        t1111_t = temp[temp['call_type']=='2']
        if not t1111_t.empty:
            t1111_2 = pd.pivot_table(t1111_t,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
            t1111_2.columns = ['contacted_xj_call_num']
            all_data.append(t1111_2)
            #数量
            t1112_2 = t1111_t[-t1111_t.duplicated(['user_id','receive_phone','call_type'])]
            t1112_2 = pd.pivot_table(temp,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
            t1112_2.columns = ['contacted_xj_num']
            all_data.append(t1112_2)
        

    #1.2 环境
    #1.2.1 与黄牛的联系情况
    ph_black_fraud = pd.read_csv(getTheFile(r'data\phone_blacklist_fraud_group.csv'),low_memory=False)
    phone = set(ph_black_fraud['phone'].map(str))
    temp = base_data[base_data['receive_phone'].map(lambda x: x in phone)]
    if not temp.empty:
        #计算1月内与黄牛的联系次数
        tempp = temp[[x.days < 30 for x in (temp['created_time'] - temp['call_time'])]]
        if not tempp.empty:
            t1211 = pd.pivot_table(tempp,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)  
            t1211.columns = ['one_hn_num']
            all_data.append(t1211)     
        #计算2月内与黄牛的联系次数
        tempp = temp[(temp['created_time'] - temp['call_time']).map(lambda x:x.days < 60)]
        if not tempp.empty:
            t1212 = pd.pivot_table(tempp,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)  
            t1212.columns = ['two_hn_num']
            all_data.append(t1212)

    #1.2.2 与黑名单的联系情况
    ph_black = pd.read_csv(r'D:\workspace\finding\phone_black_list.csv',low_memory=False)
    phone = set(ph_black['phone'][ph_black['level']==1].map(str))
    temp = base_data[base_data['receive_phone'].map(lambda x: x in phone)]
    if not temp.empty:
        #1月
        tempp = temp[(temp['created_time'] - temp['call_time']).map(lambda x:x.days < 30)]
        if not tempp.empty:
            #次数
            t12211 = pd.pivot_table(tempp,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
            t12211.columns = ['contacting_one_black_onemonth_call_num']
            all_data.append(t12211)
            #人数
            tempp = tempp[-tempp.duplicated(['user_id','receive_phone','call_type'])]
            t12212 = pd.pivot_table(tempp,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
            t12212.columns = ['contacting_one_black_onemonth_num']
            all_data.append(t12212)
        #6月
        tempp = temp[(temp['created_time'] - temp['call_time']).map(lambda x:x.days < 180)]
        if not tempp.empty:
            #次数
            t12213 = pd.pivot_table(tempp,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
            t12213.columns = ['contacting_one_black_sixmonth_call_num']
            all_data.append(t12213)
            #人数
            tempp = tempp[-tempp.duplicated(['user_id','receive_phone','call_type'])]
            t12214 = pd.pivot_table(tempp,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
            t12214.columns = ['contacting_one_black_sixmonth_num']
            all_data.append(t12214)              
    
    #2级
    phone = set(ph_black['phone'][ph_black['level']==2].map(str))
    temp = base_data[base_data['receive_phone'].map(lambda x: x in phone)]
    if not temp.empty:
        #1月
        tempp = temp[(temp['created_time'] - temp['call_time']).map(lambda x:x.days < 30)]
        if not tempp.empty:
        #次数
            t12215 = pd.pivot_table(tempp,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
            t12215.columns = ['contacting_two_black_onemonth_call_num']
            all_data.append(t12215)
            #人数
            tempp = tempp[-tempp.duplicated(['user_id','receive_phone','call_type'])]
            t12216 = pd.pivot_table(tempp,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
            t12216.columns = ['contacting_two_black_onemonth_num']
            all_data.append(t12216)
        #6月
        tempp = temp[(temp['created_time'] - temp['call_time']).map(lambda x:x.days < 180)]
        if not tempp.empty:
        #次数
            t12217 = pd.pivot_table(tempp,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
            t12217.columns = ['contacting_two_black_sixmonth_call_num']
            all_data.append(t12217)
            #人数
            tempp = tempp[-tempp.duplicated(['user_id','receive_phone','call_type'])]
            t12218 = pd.pivot_table(tempp,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
            t12218.columns = ['contacting_two_black_sixmonth_num']
            all_data.append(t12218)     
                 
                
    #1.2.3 与同为借贷人情况


    #2 通话行为
    #2.1 个人行为习惯
    #2.1.1 实际通话天数
    #2.1.2 无通话比例
    temp = base_data[-base_data.duplicated(['user_id','day'])]
    all_day = []
    ac_day = []
    users = [] 
    for user_id,u_data in temp.groupby(['user_id']):
        users.append(user_id)
        all_day.append((max(u_data['day']) - min(u_data['day'])).days + 1)
        ac_day.append(len(u_data))
    t211 = pd.DataFrame()
    t211['actual_day'] = ac_day
    t211['all_day'] = all_day
    t211['no_day_rate'] = 1 - t211['actual_day']/t211['all_day'].map(float)
    t211.index = users

    all_data.append(t211)

    #2.1.3 待机天数（短中长时期次数）
    users = []
    time_short = []
    time_median = []
    time_long = []
    for user_id,u_data in temp.groupby(['user_id']):
        users.append(user_id)
        u_data = u_data.sort_values(by="day",ascending=True)
        u_data = u_data.reset_index(drop=True)
        u1 = np.array(u_data['day'])[:-1]
        u2 = np.array(u_data['day'])[1:]
        wait = pd.Series(u2-u1).map(lambda x:x.days-1)
        time_short.append(np.sum(wait.map(lambda x: x<=8 and x>=3)))
        time_median.append(np.sum(wait.map(lambda x: x<=19 and x>=9)))
        time_long.append(np.sum(wait.map(lambda x: x>=20)))
        
    t213 = pd.DataFrame()
    t213['time_short'] = time_short
    t213['time_median'] = time_median
    t213['time_long'] = time_long
    t213.index = users

    all_data.append(t213)

    #2.1.4 夜晚通话比例（次数、天数）
    temp = base_data[(base_data['hour'] <= 6) & (base_data['hour'] >= 1)]
    if not temp.empty:
        #次数
        t2141 = pd.pivot_table(temp,index=["user_id"],values=["day"],
                               aggfunc='count',fill_value=0)
        t2141.columns = ['night_nums']
        all_data.append(t2141)
        #天数                
        temp = temp[-temp.duplicated(['user_id','day'])]
        t2142 = pd.pivot_table(temp,index=["user_id"],values=["day"],
                               aggfunc='count',fill_value=0)
        t2142.columns = ['night_days']
        all_data.append(t2142)

    #2.1.5 通话月数量
    temp = base_data[-base_data.duplicated(['user_id','month'])]
    t215 = pd.pivot_table(temp,index=["user_id"],values=["month"],
                          aggfunc='count',fill_value=0)
    t215.columns = ['month_nums']

    all_data.append(t215)
          
    #2.1.6 无通话小时数量（在所有通话记录中都不在该小时的数量和
    t216 = pd.pivot_table(base_data,index=["user_id"],values=["call_time"],
                          columns=["hour"],aggfunc='count',fill_value=0)
    t216 = 24 - t216.apply(lambda x: np.sum(x!=0),axis=1)
    t216 = pd.DataFrame(t216)
    t216.columns = ['no_hour_nums']

    all_data.append(t216)

    #2.2.1 与110的通话次数
    temp = base_data[base_data['receive_phone'] == '110']
    if not temp.empty:
        t221 = pd.pivot_table(temp,index=["user_id"],values=["call_time"],
                              aggfunc='count',fill_value=0)
        t221.columns = ['call_110']

        all_data.append(t221)

    #2.2.2 与120的通话次数
    temp = base_data[base_data['receive_phone'] == '120']
    if not temp.empty:
        t222 = pd.pivot_table(temp,index=["user_id"],values=["call_time"],
                              aggfunc='count',fill_value=0)
        t222.columns = ['call_120']

        all_data.append(t222)

    #2.2.3 主叫比例
    t223_t = base_data[base_data['call_type'] == '1']
    if not t223_t.empty:
        t223_1 = pd.pivot_table(t223_t,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
        t223_1.columns = ['calling_num']
        all_data.append(t223_1)
    else:
        t223_1 = pd.Series()

    t223_t = base_data[base_data['call_type'] == '2']
    if not t223_t.empty:
        t223_2 = pd.pivot_table(t223_t,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
        t223_2.columns = ['called_num']
        all_data.append(t223_2)
    else:
        t223_2 = pd.Series()

    t223_3 = pd.pivot_table(base_data,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
    t223_3.columns = ['call_all_num']
    all_data.append(t223_3)

    if not t223_1.empty:
        t223_4 = t223_1['calling_num'].div(t223_3['call_all_num'].map(float),fill_value=0)
        t223_4 = pd.DataFrame(t223_4)
        t223_4.columns = ['calling_rate']
        all_data.append(t223_4)

    #2.2.4 夜间与银行联系的次数
    #temp = base_data[(base_data['hour'] <= 6) & (base_data['hour'] >= 1)]
    #t224 = pd.pivot_table(temp,index=["user_id"],values=["call_time"],
    #                      aggfunc='count',fill_value=0)                 
    #t224.columns = ['nights_num']
                
    #2.2.5 上午通话比例
    temp = base_data[(base_data['hour'] <= 12) & (base_data['hour'] >= 7)]
    if not temp.empty:
        t225 = pd.pivot_table(temp,index=["user_id"],values=["call_time"],
                              aggfunc='count',fill_value=0)                 
        t225.columns = ['morning_num']

        t225_t = pd.pivot_table(base_data,index=["user_id"],values=["call_time"],
                              aggfunc='count',fill_value=0)
        t225_t.columns = ['call_sum_num']  

        t225 = pd.concat([t225,t225_t],axis=1,join='outer')
        t225['morning_rate'] = t225['morning_num']/t225['call_sum_num'].map(float)
        del t225['morning_num'],t225['call_sum_num']

        all_data.append(t225)


    #2.3 通话质量
    #2.3.1 总体情况
    #2.3.1.1 基本数据
    #2.3.1.1.1 互通人数
    temp = base_data[-base_data.duplicated(['user_id','receive_phone','call_type'])]
    temp_t = pd.pivot_table(temp,index=["user_id","receive_phone"],values=["call_type"],
                            aggfunc='count',fill_value=0)
    temp_t = temp_t[temp_t['call_type']==2]
    if not temp_t.empty:
        temp_t['user_id'] = temp_t.index.map(lambda x:x[0])
        t23111 = pd.pivot_table(temp_t,index=["user_id"],values=["call_type"],
                                aggfunc='count',fill_value=0)
        t23111.columns = ['hu_num']

        all_data.append(t23111)

    #2.3.1.1.2 有效通话比例
    temp = base_data[base_data['trade_time'] >= 10 ]
    if not temp.empty:
        t23112 = pd.pivot_table(temp,index=["user_id"],values=["call_time"],
                              aggfunc='count',fill_value=0)                 
        t23112.columns = ['userful_num']

        t23112_t = pd.pivot_table(base_data,index=["user_id"],values=["call_time"],
                              aggfunc='count',fill_value=0)
        t23112_t.columns = ['call_sum_num']

        t23112 = pd.concat([t23112,t23112_t],axis=1,join='outer')
        t23112['useful_rate'] = t23112['userful_num']/t23112['call_sum_num'].map(float)
        del t23112['userful_num'],t23112['call_sum_num']

        all_data.append(t23112)

    #2.3.1.1.3 联系人数量

    temp = base_data[-base_data.duplicated(['user_id','receive_phone','call_type'])]
    temp_t = temp[temp['call_type']=='1']
    if not temp_t.empty:
        t231131_1 = pd.pivot_table(temp_t,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
        t231131_1.columns = ['calling_people_num']
        all_data.append(t231131_1)

    temp_t = temp[temp['call_type']=='2']
    if not temp_t.empty:
        t231131_2 = pd.pivot_table(temp_t,index=["user_id"],values=["call_time"],aggfunc='count',fill_value=0)
        t231131_2.columns = ['called_people_num']
        all_data.append(t231131_2)

    temp = temp[-temp.duplicated(['user_id','receive_phone'])]
    t23113 = pd.pivot_table(temp,index=["user_id"],values=["call_time"],
                          aggfunc='count',fill_value=0)
    t23113.columns = ['call_people_num']

    all_data.append(t23113)

    #2.3.1.1.4 平均通话时长
    temp = base_data[base_data['trade_time'] >= 10 ]
    if not temp.empty:
        t23114 = pd.pivot_table(temp,index=["user_id"],values=["trade_time"],
                              aggfunc='mean',fill_value=0)                 
        t23114.columns = ['call_avg_time']
        all_data.append(t23114)

    #2.3.1.1.5 通话高于1800次数
    temp = base_data[base_data['trade_time'] >= 1800 ]
    if not temp.empty:
        t23115 = pd.pivot_table(temp,index=["user_id"],values=["call_time"],
                              aggfunc='count',fill_value=0)                 
        t23115.columns = ['over_1800_num']

        all_data.append(t23115)

    #2.3.1.1.6 最常联系人
    #连续几月(5月以上)都有通话记录
    temp = base_data[-base_data.duplicated(['user_id','receive_phone','month'])]
    temp_t = pd.pivot_table(temp,index=["user_id","receive_phone"],values=["month"],
                            aggfunc='count',fill_value=0)
    temp_t = temp_t[temp_t['month'] >= 5]
    if not temp_t.empty:
        temp_t['user_id'] = temp_t.index.map(lambda x:x[0])
        t231161 = pd.pivot_table(temp_t,index=["user_id"],values=["month"],
                                 aggfunc='count',fill_value=0)
        t231161.columns = ['most_people_1']

        all_data.append(t231161)

        #最常联系人的平均通话时长
        temp_t['receive_phone'] = temp_t.index.map(lambda x:x[1])
        del temp_t['month']
        temp_t['go'] = 1

        temp = pd.merge(base_data,temp_t,on=['user_id','receive_phone'],how='left')
        temp = temp[temp['go'] == 1]
        if not temp.empty:
            t2311611 = pd.pivot_table(temp,index=["user_id"],values=["trade_time"],
                                      aggfunc='mean',fill_value=0)
            t2311611.columns = ['avg_most_people_1_time']

            all_data.append(t2311611)

    #累计通话时长超过1小时
    temp = base_data[base_data['trade_time'] >= 10 ]
    if not temp.empty:
        temp = pd.pivot_table(temp,index=["user_id","receive_phone"],values=["trade_time"],
                              aggfunc='sum',fill_value=0)
        temp = temp[temp['trade_time'] >= 3600]
        temp['user_id'] = temp.index.map(lambda x:x[0])
        t231162 = pd.pivot_table(temp,index=["user_id"],values=["trade_time"],
                                 aggfunc='count',fill_value=0)
        t231162.columns = ['most_people_2']

        all_data.append(t231162)

    #2.3.1.2 波动数据
    temp = pd.pivot_table(base_data,index=["user_id"],values=["call_time"],
                            aggfunc=[np.max],fill_value=0)
    temp['user_id'] = temp.index
    temp.columns = ['max_day','user_id']
    base_data = pd.merge(base_data,temp,how='left')
    base_data['call_day'] = (base_data['max_day']-base_data['call_time']).map(lambda x: x.days)
    base_data['month_type'] = (base_data['call_day']/30).map(int)
    temp = pd.pivot_table(base_data,index=["user_id"],values=["month_type"],
                            aggfunc=[np.max],fill_value=0)
    temp['user_id'] = temp.index
    temp.columns = ['month_type','user_id']
    temp['nuse'] = 1
    base_data = pd.merge(base_data,temp,on=['user_id','month_type'],how='left')
    temp = base_data[base_data['nuse'] != 1]

    if not temp.empty:

        #2.3.1.2.1 互通人数波动
        temp_t = temp[-temp.duplicated(['user_id','receive_phone','call_type','month_type'])]
        temp_t = pd.pivot_table(temp_t,index=["user_id","receive_phone","month_type"],values=["call_type"],
                                aggfunc='count',fill_value=0)
        temp_t = temp_t[temp_t['call_type']==2]
        if not temp_t.empty:
            temp_t['user_id'] = temp_t.index.map(lambda x:x[0])
            temp_t['month_type'] = temp_t.index.map(lambda x:x[2])

            temp_t = pd.pivot_table(temp_t,index=["user_id","month_type"],values=["call_type"],
                                    aggfunc='count',fill_value=0)
            temp_t['user_id'] = temp_t.index.map(lambda x:x[0])
            t23121 = pd.pivot_table(temp_t,index=["user_id"],values=["call_type"],
                                    aggfunc=[np.std,np.mean],fill_value=0)
            t23121.columns = ['std','mean']
            t23121['hu_v'] = t23121['std']/t23121['mean']
            del t23121['std'],t23121['mean']
            
            all_data.append(t23121)

        #2.3.1.2.2 呼叫人数波动
        temp_t = temp[-temp.duplicated(['user_id','receive_phone','month_type'])]
        temp_t = pd.pivot_table(temp_t,index=["user_id","month_type"],values=["call_type"],
                                aggfunc='count',fill_value=0)
        temp_t['user_id'] = temp_t.index.map(lambda x:x[0])
        t23122 = pd.pivot_table(temp_t,index=["user_id"],values=["call_type"],
                                aggfunc=[np.std,np.mean],fill_value=0)
        t23122.columns = ['std','mean']
        t23122['call_people_v'] = t23122['std']/t23122['mean'].map(float)
        del t23122['std'],t23122['mean']

        all_data.append(t23122)

        #2.3.1.2.3 呼叫幂率指数
        #2.3.1.2.4 呼叫次数波动
        temp_t = pd.pivot_table(temp,index=["user_id","month_type"],values=["call_type"],
                                aggfunc='count',fill_value=0)
        temp_t['user_id'] = temp_t.index.map(lambda x:x[0])
        t23124 = pd.pivot_table(temp_t,index=["user_id"],values=["call_type"],
                                aggfunc=[np.std,np.mean],fill_value=0)
        t23124.columns = ['std','mean']
        t23124['num_v'] = t23124['std']/t23124['mean'].map(float)
        del t23124['std'],t23124['mean']

        all_data.append(t23124)

        #2.3.1.2.5 主被叫比波动
        temp_t = pd.pivot_table(temp,index=["user_id","month_type"],values=["call_time"],
                                columns=["call_type"],aggfunc='count',fill_value=0)
        temp_t['user_id'] = temp_t.index.map(lambda x:x[0])
        temp_t.columns = ['calling_num','called_num','user_id']
        temp_t['ie_rate'] = temp_t['calling_num']/temp_t['called_num'].map(float)
        t23125 = pd.pivot_table(temp_t,index=["user_id"],values=["ie_rate"],
                                aggfunc=[np.std,np.mean],fill_value=0)
        t23125.columns = ['std','mean']
        t23125['ie_v'] = t23125['std']/t23125['mean'].map(float)
        del t23125['std'],t23125['mean']

        all_data.append(t23125)

    #2.3.2 近期情况
    temp = base_data[(base_data['created_time'] - base_data['call_time']).map(lambda x:x.days <= 60)]
    if not temp.empty:
        #2.3.2.1 近两月无效通话比例
        t2321 = pd.pivot_table(temp,index=["user_id"],values=["call_time"],
                                aggfunc='count',fill_value=0)
        t2321.columns = ['all_num']
        temp_t = temp[temp['trade_time'] < 10]
        if not temp_t.empty:
            t2321_t = pd.pivot_table(temp_t,index=["user_id"],values=["call_time"],
                                    aggfunc='count',fill_value=0)
            t2321_t.columns = ['no_use_num']
            t2321 = pd.concat([t2321,t2321_t],axis=1,join='outer')
            t2321['recent_no_rate'] = t2321['no_use_num']/t2321['all_num'].map(float)
            del t2321['no_use_num'],t2321['all_num']

            all_data.append(t2321)

        #2.3.2.2 近两月通话占比
        t2322 = pd.pivot_table(temp,index=["user_id"],values=["call_time"],
                                aggfunc='count',fill_value=0)
        t2322.columns = ['two_call_num']
        t2322_t = pd.pivot_table(base_data,index=["user_id"],values=["call_time"],
                                aggfunc='count',fill_value=0)
        t2322_t.columns = ['all_call_num']
        t2322 = pd.concat([t2322,t2322_t],axis=1,join='outer')
        t2322['recent_call_rate'] = t2322['two_call_num']/t2322['all_call_num'].map(float)
        del t2322['all_call_num'],t2322['two_call_num']
        all_data.append(t2322)

        #2.3.2.3近两月被骚扰的次数（）
        temp_t = temp[(temp['call_type'] == 2) & (temp['trade_time'] <= 10)]
        if not temp_t.empty:
            temp_t = pd.pivot_table(temp_t,index=["user_id","receive_phone"],values=["call_time"],
                                    aggfunc=[np.max,np.min],fill_value=0)
            temp_t['user_id'] = temp_t.index.map(lambda x:x[0])
            temp_t.columns = ['max','min','user_id']
            temp_t['n_day'] = (temp_t['max']-temp_t['min']).map(lambda x:x.days)
            temp_t = temp_t[temp_t['n_day'] < 5]

            t2323 = pd.pivot_table(temp_t,index=["user_id"],values=["n_day"],
                                    aggfunc='count',fill_value=0)
            t2323.columns = ['annoy']
            all_data.append(t2323)

        #2.3.2.4 近两月漫游占比（本地、外地）
        user_id = []
        local_rate = []
        for user_idd,u_data in temp.groupby(['user_id']):
            user_id.append(user_idd)
            local_rate.append(np.sum(u_data['trade_type']=='1')/float(len(u_data)))
        t2324 = pd.DataFrame()
        t2324['local_rate'] = local_rate
        t2324.index = user_id
        all_data.append(t2324)

    ##数据合并
    result = pd.concat(all_data,axis=1,join='outer')
    result = result.fillna(0)
    result['user_id'] = result.index

    del base_data

    #3 基本信息
    #3.9 10 11 12
    user['hours'] = [x.hour for x in user['date_created']]
    user['company_nature'] = [to_intstr(x) for x in user['company_nature']]
    user.loc[user['hours']==0,'hours'] = 24
    user = user[['user_id','edu_level','marital_status','house_hold','company_nature','hours','age']]
    user = user.fillna('na')
    #基本信息哑变量化
    base_info = model_config.base_info
    for key in base_info.keys():
        if base_info[key]['type'] == 'cut':
            temp_key = pd.cut(user[key],base_info[key]['cut_point'],labels=base_info[key]['label'])
        if base_info[key]['type'] == 'label':
            temp_key = user[key]
        n_lab = ['_'.join([key,label]) for label in base_info[key]['label']]
        temp_dum = pd.get_dummies(temp_key,prefix=key)
        rest_col = list(set(n_lab)-set(temp_dum.columns))
        if rest_col:
            for col in rest_col:
                temp_dum[col] = 0
        user = pd.merge(user,temp_dum,right_index=True, left_index=True)
        del user[key]
    result = pd.merge(result,user)
    return result


def rm_null(label):
    if type(label) in [float,long,int]:
        return False
    else:
        return True

def to_intstr(num):
    if type(num) is [float,long,int]:
        if isnan(num):
            return 'na'
        else:
            return str(int(num))
    else:
        return 'na'

if __name__ == '__main__':
    main()