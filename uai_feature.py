import datetime
import time
from datetime import datetime, timedelta





import pandas as pd
from pandas import DataFrame
from pandas import Series

'''
    u'start_geo_id', u'end_geo_id', u'create_date', u'id',
    u'driver_id', u'member_id', u'create_hour', u'status',
    u'estimate_money', u'estimate_distance', u'estimate_term', u'num'
'''
# 此模块用于最基本的数据梳理
july_order = pd.read_csv('data/train_July.csv')
july_order['num'] = 1
july_order_25_31 = july_order[july_order['create_date']>'2017-07-24'][['start_geo_id','end_geo_id','create_date','create_hour','num']]
july_order_0725_d = july_order_25_31[(july_order_25_31['create_date']=='2017-07-25')&(july_order_25_31['create_hour']%2 !=0)].groupby(['start_geo_id','end_geo_id','create_date','create_hour'])['num'].sum()
july_order_0726_s = july_order_25_31[(july_order_25_31['create_date']=='2017-07-26')&(july_order_25_31['create_hour']%2 ==0)].groupby(['start_geo_id','end_geo_id','create_date','create_hour'])['num'].sum()
july_order_0727_d = july_order_25_31[(july_order_25_31['create_date']=='2017-07-27')&(july_order_25_31['create_hour']%2 !=0)].groupby(['start_geo_id','end_geo_id','create_date','create_hour'])['num'].sum()
july_order_0728_s = july_order_25_31[(july_order_25_31['create_date']=='2017-07-28')&(july_order_25_31['create_hour']%2 ==0)].groupby(['start_geo_id','end_geo_id','create_date','create_hour'])['num'].sum()
july_order_0729_d = july_order_25_31[(july_order_25_31['create_date']=='2017-07-29')&(july_order_25_31['create_hour']%2 !=0)].groupby(['start_geo_id','end_geo_id','create_date','create_hour'])['num'].sum()
july_order_0730_s = july_order_25_31[(july_order_25_31['create_date']=='2017-07-30')&(july_order_25_31['create_hour']%2 ==0)].groupby(['start_geo_id','end_geo_id','create_date','create_hour'])['num'].sum()
july_order_0731_d = july_order_25_31[(july_order_25_31['create_date']=='2017-07-31')&(july_order_25_31['create_hour']%2 !=0)].groupby(['start_geo_id','end_geo_id','create_date','create_hour'])['num'].sum()
july_order_1 = july_order.drop(july_order[(july_order['create_date']=='2017-07-25')&(july_order['create_hour']%2 !=0)].index)
july_order_2 = july_order_1.drop(july_order_1[(july_order_1['create_date']=='2017-07-26')&(july_order_1['create_hour']%2 ==0)].index)
july_order_3 = july_order_2.drop(july_order_2[(july_order_2['create_date']=='2017-07-27')&(july_order_2['create_hour']%2 !=0)].index)
july_order_4 = july_order_3.drop(july_order_3[(july_order_3['create_date']=='2017-07-28')&(july_order_3['create_hour']%2 ==0)].index)
july_order_5 = july_order_4.drop(july_order_4[(july_order_4['create_date']=='2017-07-29')&(july_order_4['create_hour']%2 !=0)].index)
july_order_6 = july_order_5.drop(july_order_5[(july_order_5['create_date']=='2017-07-30')&(july_order_5['create_hour']%2 ==0)].index)
july_order_train = july_order_6.drop(july_order_6[(july_order_6['create_date']=='2017-07-31')&(july_order_6['create_hour']%2 !=0)].index)

july_order_test = pd.concat([july_order_0725_d,july_order_0726_s,july_order_0727_d,july_order_0728_s,july_order_0729_d,july_order_0730_s,july_order_0731_d]).reset_index()


print 'module 1 done'




###此模块用于构造特征群1
f1_tmp_1 = july_order_train[july_order_train['create_date']<='2017-07-24'].groupby(['start_geo_id'])['num'].sum()
### f1_1_1 代表第一个特征群第一个特征
f1_1_1 = (f1_tmp_1/24).reset_index()
f1_1_1 = f1_1_1.rename(columns={"num": "f1_1"})
f1_tmp_2 = july_order_train[(july_order_train['create_date']>'2017-07-21')&(july_order_train['create_date']<='2017-07-24')].groupby(['start_geo_id'])['num'].sum()
f1_1_2 = (f1_tmp_2/3).reset_index()
f1_1_2 = f1_1_2.rename(columns={"num": "f1_2"})
f1_tmp_3 = july_order_train[july_order_train['create_date']=='2017-07-24'].groupby(['start_geo_id'])['num'].sum()
f1_1_3 = (f1_tmp_3).reset_index()
f1_1_3 = f1_1_3.rename(columns={"num": "f1_3"})
f1_tmp_4 = july_order_test.groupby(['start_geo_id','create_date'])['num'].sum()
f1_1_4 = f1_tmp_4.reset_index()
f1_1_4 = f1_1_4.rename(columns={"num": "f1_4"})
f1_1_5 = f1_1_4.copy()
f1_1_5['create_date2'] = pd.to_datetime(f1_1_5['create_date'])
f1_1_5['create_date2'] = pd.to_datetime(pd.DatetimeIndex(f1_1_5.create_date2)- pd.DateOffset(1))
del f1_1_5['create_date']
f1_1_5['create_date'] = f1_1_5['create_date2'].map(lambda x: x.strftime('%Y-%m-%d'))
del f1_1_5['create_date2']
f1_1_5 = f1_1_5.rename(columns={"f1_4": "f1_5"})
f1_1_1_m = pd.merge(july_order_test,f1_1_1,on=['start_geo_id'],how='left')
f1_1_2_m = pd.merge(f1_1_1_m,f1_1_2,on=['start_geo_id'],how='left')
f1_1_3_m = pd.merge(f1_1_2_m,f1_1_3,on=['start_geo_id'],how='left')
f1_1_4_m = pd.merge(f1_1_3_m,f1_1_4,on=['start_geo_id','create_date'],how='left')
f1_1_5_m = pd.merge(f1_1_4_m,f1_1_5,on=['start_geo_id','create_date'],how='left')
f1_1_5_m = f1_1_5_m.fillna(0)
f1_1_5_m['f1_5'] = (f1_1_5_m['f1_5']+f1_1_5_m['f1_4'])/2





import datetime
import time
from datetime import datetime, timedelta
###此模块用于构造特征群2:个人认为特征群2都是强特
july_order_train
july_order_train_num = july_order_train[['start_geo_id','end_geo_id','create_date','create_hour','num']]
july_order_train_num_before_24 = july_order_train_num[july_order_train_num['create_date']<='2017-07-24']
test_select = july_order_test[['start_geo_id','end_geo_id','create_date','create_hour']]
feature_2 = test_select.copy()
f2_tmp_1 = pd.merge(test_select,july_order_train_num_before_24,on=['start_geo_id','end_geo_id','create_hour'],how='left')
f2_1 = f2_tmp_1.groupby(['start_geo_id','end_geo_id','create_hour'])['num'].sum()/24
f2_1 = f2_1.reset_index().rename(columns={"num": "f2_1"})
feature_2 = pd.merge(feature_2,f2_1,on=['start_geo_id','end_geo_id','create_hour'],how='left')
# f1 done
feature_2['week'] = pd.to_datetime(feature_2['create_date']).dt.weekday
july_order_train_num_before_24_week = july_order_train_num_before_24.copy()
july_order_train_num_before_24_week['week'] =  pd.to_datetime(july_order_train_num_before_24_week['create_date']).dt.weekday
test_select2_week = test_select.copy()
test_select2_week['week'] = pd.to_datetime(test_select2_week['create_date']).dt.weekday
del july_order_train_num_before_24_week['create_date']
f2_tmp_2 = pd.merge(test_select2_week,july_order_train_num_before_24_week,on=['start_geo_id','end_geo_id','week','create_hour'],how='left')
f2_2 = f2_tmp_2.groupby(['start_geo_id','end_geo_id','week','create_hour'])['num'].sum()/3
f2_2 = f2_2.reset_index().rename(columns={"num": "f2_2"})
feature_2 = pd.merge(feature_2,f2_2,on=['start_geo_id','end_geo_id','create_hour','week'],how='left')
# f2 done
july_order_train_num_before_7 = july_order_train_num_before_24[july_order_train_num_before_24['create_date']>='2017-07-18']
f2_tmp_3 = pd.merge(test_select,july_order_train_num_before_7,on=['start_geo_id','end_geo_id','create_hour'],how='left')
f2_3 = f2_tmp_3.groupby(['start_geo_id','end_geo_id','create_hour'])['num'].sum()/7
f2_3 = f2_3.reset_index().rename(columns={"num": "f2_3"})
feature_2 = pd.merge(feature_2,f2_3,on=['start_geo_id','end_geo_id','create_hour'],how='left')
# f3 done
feature_2['week'] = pd.to_datetime(feature_2['create_date']).dt.weekday
july_order_train_num_before_7_week = july_order_train_num_before_7.copy()
july_order_train_num_before_7_week['week'] =  pd.to_datetime(july_order_train_num_before_7_week['create_date']).dt.weekday
test_select2_week = test_select.copy()
test_select2_week['week'] = pd.to_datetime(test_select2_week['create_date']).dt.weekday
del july_order_train_num_before_7_week['create_date']
f2_tmp_4 = pd.merge(test_select2_week,july_order_train_num_before_7_week,on=['start_geo_id','end_geo_id','week','create_hour'],how='left')
f2_4 = f2_tmp_4.groupby(['start_geo_id','end_geo_id','week','create_hour'])['num'].sum()
f2_4 = f2_4.reset_index().rename(columns={"num": "f2_4"})
feature_2 = pd.merge(feature_2,f2_4,on=['start_geo_id','end_geo_id','create_hour','week'],how='left')
# f4 done
# 2017-09-03 变成前一天的 -》2017-09-02
july_order_train_num_25_31 = july_order_train_num[july_order_train_num['create_date']>'2017-07-25']
f2_tem_5 = july_order_train_num_25_31.copy()
f2_tem_5['create_date'] = pd.to_datetime(pd.DatetimeIndex(pd.to_datetime(f2_tem_5['create_date']))- pd.DateOffset(1))
f2_tem_5['create_date'] = f2_tem_5['create_date'].map(lambda x: x.strftime('%Y-%m-%d'))
f2_tem_5_1 = f2_tem_5.groupby(['start_geo_id','end_geo_id','create_date','create_hour'])['num'].sum()
f2_5 = f2_tem_5_1.reset_index().rename(columns={"num": "f2_5"})
feature_2 = pd.merge(feature_2,f2_5,on=['start_geo_id','end_geo_id','create_date','create_hour'],how='left')
# f5 done
# 后一小时的订单情况
july_order_train_num_25_31 = july_order_train_num[july_order_train_num['create_date']>='2017-07-25']
f2_tem_6 = july_order_train_num_25_31.copy()
f2_tem_6['create_hour'] = f2_tem_6['create_hour'].map(lambda x: 23 if x-1<0 else x-1)
# f2_tem_6_change['create_date'] = f2_tem_6_change['create_date'].map(lambda x: (datetime.strptime(x,'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d'))
f2_tem_6.loc[f2_tem_6['create_hour'] == 23, 'create_date'] = f2_tem_6['create_date'].map(lambda x: (datetime.strptime(x,'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d'))
f2_tem_6 = f2_tem_6.groupby(['start_geo_id','end_geo_id','create_date','create_hour'])['num'].sum()
f2_6 = f2_tem_6.reset_index().rename(columns={"num": "f2_6"})
f2_6.fillna(0)
feature_2 = pd.merge(feature_2,f2_6,on=['start_geo_id','end_geo_id','create_date','create_hour'],how='left')
july_order_train_num_25_31 = july_order_train_num[july_order_train_num['create_date']>='2017-07-24']
f2_tem_7 = july_order_train_num_25_31.copy()
f2_tem_7['create_hour'] = f2_tem_7['create_hour'].map(lambda x: 0 if x+1>23 else x+1)
f2_tem_7.loc[f2_tem_7['create_hour'] == 0, 'create_date'] = f2_tem_7['create_date'].map(lambda x: (datetime.strptime(x,'%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d'))
f2_tem_7 = f2_tem_7.groupby(['start_geo_id','end_geo_id','create_date','create_hour'])['num'].sum()
f2_7 = f2_tem_7.reset_index().rename(columns={"num": "f2_7"})
f2_7.fillna(0)
feature_2 = pd.merge(feature_2,f2_7,on=['start_geo_id','end_geo_id','create_date','create_hour'],how='left')
feature_2['f2_6_7'] = (feature_2['f2_6']+feature_2['f2_7'])/2
del feature_2['f2_6']
del feature_2['f2_7']
# f6_7 done,git done
print feature_2

