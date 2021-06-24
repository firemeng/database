#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'Meng Linghao'

'''
20210618 万得落地数据库数据接口
  需申请接入权限  
数据库登陆：
  import winddb
  w=winddb.winddb(用户名，密码)
数据库断开：
  w.close()
函数：
  w.getprice(FactorName,Code,StartDate,EndDate) 获取日价格数据
  w.getindexmember(IndexCode,EndDate=today) 获取指数成分股列表
  w.getallstock(EndDate=today) 获取所有股票列表
'''

import datetime as dt
import cx_Oracle
import numpy as np
import pandas as pd

class winddb():
# 本地数据库接口程序
  def __init__(self,User = 'zyw_menglinghao',__Pwd = 'zywmlh0820'):
    IPAddr = '10.29.137.44'
    Port = 1521
    DBName = 'windzxdg'
    # 建立Oracle链接
    try:
      self.Connection = cx_Oracle.connect(User, __Pwd, cx_Oracle.makedsn(IPAddr, str(Port), DBName))
    except Exception as e:
      raise e
    if self.Connection is None: 
      print('出错了，万得落地数据库未连接！')
    else:
      print('万得落地数据库已连接')
    #载入 因子名-数据库字段名 对应表
    self.FieldTable = pd.read_csv('FieldTable.csv',index_col=0)

  def _FetchData(self,SQLStr):
    # 建立游标、提取数据、关闭游标
    Cursor = self.Connection.cursor()
    Cursor.execute(SQLStr)
    #fetchall的效率低，for row in Cursor: Data.append较快
    # Data = Cursor.fetchall()
    Data=[]
    for row in Cursor: 
      Data.append(row)
    Cursor.close()
    return Data

  def _CalendarTable(self,DBTableName,FieldName,Code,StartDate,EndDate):
    # 生成多股票、多因子的SQL语句，仅能取交易日因子，每次仅能查询一张表中的数据
    # 如一个600000.SH、000001.SZ的20210601至20210620之间的open\close数据
    # StartDate和EneDate是datetime格式
    SQLStr = "SELECT x.TRADE_DT,x.S_INFO_WINDCODE, "
    for ifield in FieldName: #因子名代入SQL
      SQLStr += "x."+ifield+", "
    SQLStr = SQLStr[:-2]+" FROM WINDZX."+DBTableName+" x "
    SQLStr += "WHERE x.TRADE_DT >= '"+StartDate.strftime("%Y%m%d")+"' "
    SQLStr += "AND x.TRADE_DT <= '"+EndDate.strftime("%Y%m%d")+"' "
    SQLStr += "AND ("
    for iCode in Code:#多个股票代码
      SQLStr += "x.S_INFO_WINDCODE = '"+iCode+"' OR "
    SQLStr = SQLStr[:-4]+") ORDER BY x.S_INFO_WINDCODE,x.TRADE_DT"
    return SQLStr

  def getprice(self,FactorName,Code,StartDate,EndDate):
    # 取得价格数据表
    # FactorName可选因子表内的因子
    # 查询结果为dataframe，表头为日期-Code-因子值1-因子值2-...
    DBTableName = 'ASHAREEODPRICES'
    FactorName = FactorName.split(',')
    FieldName = []#因子名转换为数据库字段名
    for ifactor in FactorName: 
      FieldName.append(self.FieldTable.loc[ifactor,'WindFieldName'])
    Code = Code.split(',')
    SQLStr = self._CalendarTable(DBTableName,FieldName,Code,StartDate,EndDate)
    RawData = self._FetchData(SQLStr)
    if not RawData: 
      print("返回控数据")
      return pd.DataFrame(columns=["Date", "ID"]+FactorName)
    else:
      return pd.DataFrame(np.array(RawData), columns=["Date", "ID"]+FactorName)

  def close(self):
    try:
      self.Connection.close()
    except Exception as e:
      raise e
    print('万得落地数据库已断开')

  def getindexmember(self,IndexCode,EndDate = dt.date.today()):
  # 取得截至EndDate的指数成分股列表
  # EndDate需要datetime格式,默认今天
    SQLStr = "SELECT x.S_CON_WINDCODE "
    SQLStr += "FROM WINDZX.AINDEXMEMBERS x "
    SQLStr += "WHERE (x.S_INFO_WINDCODE = '"+IndexCode+"') "
    SQLStr += "AND (x.S_CON_OUTDATE IS NULL "
    SQLStr += "OR x.S_CON_OUTDATE > '"+EndDate.strftime("%Y%m%d")+"') "
    SQLStr += "AND x.S_CON_INDATE <= '"+EndDate.strftime("%Y%m%d")+"' "
    SQLStr += "ORDER BY x.S_CON_WINDCODE"
    RawData = self._FetchData(SQLStr)
    ColumnsName = ["ID"]
    if not RawData: 
      print("返回空数据")
      return pd.DataFrame(columns=ColumnsName)
    else:
      return pd.DataFrame(np.array(RawData), columns=ColumnsName)

  def getallstock(self,EndDate = dt.date.today()):
    SQLStr = "SELECT x.S_INFO_WINDCODE,x.S_INFO_NAME FROM WINDZX.ASHAREDESCRIPTION x "
    SQLStr += "WHERE x.S_INFO_LISTDATE IS NOT NULL "
    SQLStr += "AND x.S_INFO_LISTDATE < '"+EndDate.strftime("%Y%m%d")+"' "
    SQLStr += "ORDER BY x.S_INFO_WINDCODE"
    RawData = self._FetchData(SQLStr)
    ColumnsName = ["ID","Name"]
    if not RawData: 
      print("返回空数据")
      return pd.DataFrame(columns=ColumnsName)
    else:
      return pd.DataFrame(np.array(RawData), columns=ColumnsName)
    

