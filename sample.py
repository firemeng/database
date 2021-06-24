# -*- coding: utf-8 -*-
'''
 20210621 接口案例
'''
# import sys,os
# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
# sys.path.append(BASE_DIR)
import datetime as dt
import numpy as np
import pandas as pd
import winddb
import time


if __name__ == '__main__':
    
    T0 =time.time()

    Factor = 'adjopen,adjfactor,tradestatus'
    ID = '600654.SH,000001.SZ'
    startdate = dt.datetime(2021,6,1)
    enddate = dt.datetime(2021,6,20)

    w=winddb.winddb()#登陆数据库

    print("价格数据")
    pricedata=w.getprice(Factor,ID,startdate,enddate)
    print(pricedata.head(3))

    print("成分股数据")
    indexmember = w.getindexmember('000300.SH',enddate)
    print(indexmember.head(3))

    print("所有股票列表")
    allstock = w.getallstock(enddate)
    print(allstock.head(3))

    w.close()
    print("用时%.1f"%(time.time()-T0))
