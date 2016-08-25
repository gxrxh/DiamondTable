#!/usr/bin/python
# -*- coding: UTF-8 -*-
#coding=utf-8
   
   
import pandas as pd
import numpy as np
import MySQLdb
import json
import time 
import datetime
import xlrd
import sys,os.path
import xlsxwriter
import random
import re
import copy
import types
from SSH_TUNNEL import *
from Tree import * 
import threading
reload(sys)
sys.setdefaultencoding( "utf-8" )

DX, ZYW, CY, DXZYW = xrange(4)
calculate_series = [[ [u"点击率",u"点击",u"展现"],[u"千次展现成本(元)",u"消耗",u"展现"],[u"点击单价(元)",u"消耗",u"点击"],[u"3天点击回报率",u"3天成交金额",u"消耗"],[u"7天点击回报率",u"7天成交金额",u"消耗"],[u"15天点击回报率",u"15天成交金额",u"消耗"]]
    , [[ u"收藏率",u"店辅收藏数",u"宝贝收藏数",u"访客"],[ u"3天转化率",u"3天顾客订单数",u"访客"],[ u"7天转化率",u"7天顾客订单数",u"访客"],[ u"15天转化率",u"15天顾客订单数",u"访客"],[ u"3天加购率",u"3天加购物车数",u"访客"],[ u"7天加购率",u"7天加购物车数",u"访客"],[u"15天加购率",u"15天加购物车数",u"访客"]]]
timeout_sql = "set interactive_timeout = 24*3600"

def cut_time(m_begin_time,m_end_time):
    begin_time = datetime.datetime.strptime(m_begin_time, '%Y-%m-%d')
    end_time = datetime.datetime.strptime(m_end_time, '%Y-%m-%d')
    m_times=[]
    while begin_time<= end_time:
        m_times.append(begin_time.strftime('%Y-%m-%d'))
        begin_time +=datetime.timedelta(days = 1)
    return m_times
       
class  DiamondTable:
    def __init__(self,m_port = 3313):    
        self._port = m_port
        self._tree = tree()
        self._tablecat = [DX, ZYW, CY, DXZYW]
        self._tablecatstring = {"DX":DX ,"ZYW":ZYW ,"CY":CY ,"DXZYW":DXZYW }
        self._shopname = []
        self._categorys = []
        self._begin_time = ""
        self._end_time = ""
        self._tablename = [u"定向" , u"资源位" , u"创意" , u"定向资源位"]
        self._cur = []
        self._cur_table = ""
        self._tables1 = ["cps_zuanshi_dest_rpt_source_history ", " cps_zuanshi_adzone_rpt_source_history ", " cps_zuanshi_adboard_rpt_source_history ", "  cps_zuanshi_dest_adzone_rpt_source_history "]  
        self._tables2 = ["cps_zuanshi_dest_rpt_history_v2 ", " cps_zuanshi_adzone_rpt_history_v2 ", " cps_zuanshi_adboard_rpt_history_v2 ", "  cps_zuanshi_dest_adzone_rpt_history_v2 "]  
        self._shopscategory_data = {}   #店铺类目对照结构
        self._shops_nullroi = []
        self._Turnoverlist = {}
        self.cydatalists = []
        self.m_data_list = []
        self.m_data = []
        self.m_roidata = []
        self.cal_list = []
        self._table_struct1 = [
                  [
                      [u"类目",u"店铺名",u"定向类别",u"定向渠道",u"targetName",u"transName",u"campaignName",u"时间",u"adPv",u"click",u"ctrStr",u"charge",u"ecpm",u"ecpc",u"roi",u"roi7",u"roi15",
                       u"alipayInShopNum",u"alipayInShopNum7",u"alipayInShopNum15",u"dirShopColNum",u"inshopItemColNum",u"clickUv",
                       u"showCartNum3",u"showCartNum7",u"showCartNum15",u"3天成交金额",u"7天成交金额",u"15天成交金额"],
                      [u"类目",u"店铺名",u"定向类别",u"定向渠道",u"定向名称",u"推广单元基本信息",u"所属推广计划",u"时间",u"展现",u"点击",u"点击率",u"消耗",u"千次展现成本(元)",u"点击单价(元)",u"3天点击回报率",u"7天点击回报率",u"15天点击回报率",
                        u"3天顾客订单数",u"7天顾客订单数",u"15天顾客订单数",u"店辅收藏数",u"宝贝收藏数",u"访客",
                        u"3天加购物车数",u"7天加购物车数",u"15天加购物车数",u"3天成交金额",u"7天成交金额",u"15天成交金额"]
                  ] ,
                  [
                      [u"类目",u"店铺名",u"资源位类别",u"adzoneName",u"transName",u"campaignName",u"时间",u"adPv",u"click",u"ctrStr",u"charge",u"ecpm",u"ecpc",u"roi",u"roi7",u"roi15",
                       u"alipayInShopNum",u"alipayInShopNum7",u"alipayInShopNum15",u"dirShopColNum",u"inshopItemColNum",u"clickUv",
                       u"showCartNum3",u"showCartNum7",u"showCartNum15",u"3天成交金额",u"7天成交金额",u"15天成交金额"],
                      [u"类目",u"店铺名",u"资源位类别",u"资源位名称",u"推广单元基本信息",u"所属推广计划",u"时间",u"展现",u"点击",u"点击率",u"消耗",u"千次展现成本(元)",u"点击单价(元)",u"3天点击回报率",u"7天点击回报率",u"15天点击回报率",
                        u"3天顾客订单数",u"7天顾客订单数",u"15天顾客订单数",u"店辅收藏数",u"宝贝收藏数",u"访客",
                        u"3天加购物车数",u"7天加购物车数",u"15天加购物车数",u"3天成交金额",u"7天成交金额",u"15天成交金额"]
                  ] ,
                  [
                      [u"类目",u"店铺名",u"adboardName",u"adboardId",u"transName",u"campaignName",u"时间",u"adPv",u"click",u"ctrStr",u"charge",u"ecpm",u"ecpc",u"roi",u"roi7",u"roi15",
                       u"alipayInShopNum",u"alipayInShopNum7",u"alipayInShopNum15",u"dirShopColNum",u"inshopItemColNum",u"clickUv",
                       u"showCartNum3",u"showCartNum7",u"showCartNum15",u"3天成交金额",u"7天成交金额",u"15天成交金额",u"创意尺寸",u"创意链接"],
                      [u"类目",u"店铺名",u"创意名称",u"创意ID",u"推广单元基本信息",u"所属推广计划",u"时间",u"展现",u"点击",u"点击率",u"消耗",u"千次展现成本(元)",u"点击单价(元)",u"3天点击回报率",u"7天点击回报率",u"15天点击回报率",
                        u"3天顾客订单数",u"7天顾客订单数",u"15天顾客订单数",u"店辅收藏数",u"宝贝收藏数",u"访客",
                        u"3天加购物车数",u"7天加购物车数",u"15天加购物车数",u"3天成交金额",u"7天成交金额",u"15天成交金额",u"创意尺寸",u"创意链接"]
                  ] ,                  
                  [
                      [u"类目",u"店铺名",u"定向类别",u"定向渠道",u"targetName",u"资源位类别",u"adzoneName",u"transName",u"campaignName",u"时间",u"adPv",u"click",u"ctrStr",u"charge",u"ecpm",u"ecpc",u"roi",u"roi7",u"roi15",
                       u"alipayInShopNum",u"alipayInShopNum7",u"alipayInShopNum15",u"dirShopColNum",u"inshopItemColNum",u"clickUv",
                       u"showCartNum3",u"showCartNum7",u"showCartNum15",u"3天成交金额",u"7天成交金额",u"15天成交金额"],
                      [u"类目",u"店铺名",u"定向类别",u"定向渠道",u"定向名称",u"资源位类别",u"资源位名称",u"推广单元基本信息",u"所属推广计划",u"时间",u"展现",u"点击",u"点击率",u"消耗",u"千次展现成本(元)",u"点击单价(元)",u"3天点击回报率",u"7天点击回报率",u"15天点击回报率",
                        u"3天顾客订单数",u"7天顾客订单数",u"15天顾客订单数",u"店辅收藏数",u"宝贝收藏数",u"访客",
                        u"3天加购物车数",u"7天加购物车数",u"15天加购物车数",u"3天成交金额",u"7天成交金额",u"15天成交金额"]
                  ] 
               ]
        self._table_struct2 = [
                  [
                      [u"类目",u"店铺名", u"分块",u"定向类别",u"定向渠道",u"targetName",u"transName",u"campaignName",u"时间",u"adPv",u"click",u"ctr",u"charge",u"ecpm",u"ecpc",
                      u"dirShopColNum",u"inshopItemColNum",u"uv",u"roi",u"alipayInshopAmt",u"alipayInShopNum",u"cartNum",u"天数"],
                      [u"类目",u"店铺名", u"分块",u"定向类别",u"定向渠道",u"定向名称",u"推广单元基本信息",u"所属推广计划",u"时间",u"展现",u"点击",u"点击率",u"消耗",u"千次展现成本(元)",u"点击单价(元)",
                      u"店辅收藏数",u"宝贝收藏数",u"访客",u"点击回报率",u"成交金额",u"顾客订单数",u"加购物车数",u"天数"]
                  ] ,
                  [
                      [u"类目",u"店铺名", u"分块",u"资源位类别",u"adzoneName",u"transName",u"campaignName",u"时间",u"adPv",u"click",u"ctr",u"charge",u"ecpm",u"ecpc",
                      u"dirShopColNum",u"inshopItemColNum",u"uv",u"roi",u"alipayInshopAmt",u"alipayInShopNum",u"cartNum",u"天数"],
                      [u"类目",u"店铺名", u"分块",u"资源位类别",u"资源位名称",u"推广单元基本信息",u"所属推广计划",u"时间",u"展现",u"点击",u"点击率",u"消耗",u"千次展现成本(元)",u"点击单价(元)",
                      u"店辅收藏数",u"宝贝收藏数",u"访客",u"点击回报率",u"成交金额",u"顾客订单数",u"加购物车数",u"天数"]
                  ] ,
                  [
                      [u"类目",u"店铺名", u"分块",u"adboardName",u"adboardId",u"transName",u"campaignName",u"时间",u"adPv",u"click",u"ctr",u"charge",u"ecpm",u"ecpc",
                      u"dirShopColNum",u"inshopItemColNum",u"uv",u"roi",u"alipayInshopAmt",u"alipayInShopNum",u"cartNum",u"天数",u"创意尺寸",u"创意链接"],
                      [u"类目",u"店铺名", u"分块",u"创意名称",u"创意ID",u"推广单元基本信息",u"所属推广计划",u"时间",u"展现",u"点击",u"点击率",u"消耗",u"千次展现成本(元)",u"点击单价(元)",
                      u"店辅收藏数",u"宝贝收藏数",u"访客",u"点击回报率",u"成交金额",u"顾客订单数",u"加购物车数",u"天数",u"创意尺寸",u"创意链接"]
                  ] ,
                  [
                      [u"类目",u"店铺名", u"分块",u"定向类别",u"定向渠道",u"targetName",u"资源位类别",u"adzoneName",u"transName",u"campaignName",u"时间",u"adPv",u"click",u"ctr",u"charge",u"ecpm",u"ecpc",
                      u"dirShopColNum",u"inshopItemColNum",u"uv",u"roi",u"alipayInshopAmt",u"alipayInShopNum",u"cartNum",u"天数"],
                      [u"类目",u"店铺名", u"分块",u"定向类别",u"定向渠道",u"定向名称",u"资源位类别",u"资源位名称",u"推广单元基本信息",u"所属推广计划",u"时间",u"展现",u"点击",u"点击率",u"消耗",u"千次展现成本(元)",u"点击单价(元)",
                      u"店辅收藏数",u"宝贝收藏数",u"访客",u"点击回报率",u"成交金额",u"顾客订单数",u"加购物车数",u"天数"]
                  ] 
               ]

    def Clear(self):
        self._shopname = []
        self._categorys = []
        self._begin_time = ""
        self._end_time = ""
        self._cur = []
        self._cur_table = ""   #现在选择了定向报表
        self._shopscategory_data = {}   #店铺类目对照结构
        self._shops_nullroi = []
        self._Turnoverlist = {}
        self.cydatalists = []
        self.m_data_list = []
        self.m_data = []
        self.m_roidata = []
        self.cal_list = []
                
    def get_shopstring(self):
        shops = ""
        if self._shopname == []:
            shops = self.catfindshopsstring(self._categorys)
        else:
            shops = "("
            for sn in self._shopname: shops = shops + "'" + sn + "',"
            shops = shops[:len(shops) - 1]
            shops = shops + ")"
        return shops
    
    def datasqlstring(self, sql_col , m_table ,m_time):
        return "select " + sql_col + "from " + m_table  + " where nick in " + self.get_shopstring() + " and  logdate ='" + m_time + "'" 
    
    def data_translate(self,m_cur):
        data = []
        for row in m_cur.fetchall():   data.append(row)
        return data

    def shopfindcat(self,m_shop):
        for (d,v) in self._shopscategory_data.items():
            if d == u"全部类目": continue
            for x in v:
                if m_shop == x[0]: return d
        return None
    
    def catfindshops(self,m_cats):
        if len(m_cats) == 1 :
            return self._shopscategory_data[m_cats[0]]
        else:
            scd = []
            for cat in m_cats:
                scd.extend(self._shopscategory_data[cat])
            return scd
    
    def catfindshopsstring(self,m_cats):
        shops = " ( "
        for m_cat in m_cats:
            if m_cat in self._shopscategory_data.keys():
                for shop in self._shopscategory_data[m_cat]:
                    shops = shops + "'" + shop[0] + "',"
        shops = shops[:len(shops) - 1] + " ) "
        return shops
        
    def nickfindshopname(self,m_nick):
        for (d,v) in self._shopscategory_data.items():
            if d == u"全部类目": continue
            for x in v:
                if m_nick == x[0]: return x[1]
        
    def shops_category(self):
        conn = mysql_connection(self._port); 
        self._cur = conn.cursor()
        self._cur.execute('select shopcatname,nick,shopname from cps_shop')
        num = 0
        m_shopscategory_data = {}
        for row in self._cur.fetchall(): 
            num = num + 1
            cat = ""
            if row[0]:
                cat = str(row[0])
            else:
                cat = "空白类目"
        
            if cat in  m_shopscategory_data.keys():
                for (d,x) in m_shopscategory_data.items():
                    if d == cat:
                        x.append([row[1],row[2]])
            else:
                m_shopscategory_data[cat] = [[row[1],row[2]]]
            #全部类目字典添加
            if "全部类目" in  m_shopscategory_data.keys():
                m_shopscategory_data["全部类目"].append([row[1],row[2]])
            else:
                m_shopscategory_data["全部类目"] = [[row[1],row[2]]]
        self._cur.close()
        conn.close()
        m_shopscategory_data =  json.dumps(m_shopscategory_data, encoding="UTF-8", ensure_ascii=False) 
        m_shopscategory_data = json.loads( m_shopscategory_data )
        return m_shopscategory_data

    def get_per_order1(self,m_shops_nullroi):
        if m_shops_nullroi == "()":
            return None
        try:
            self.m_roidata = []
            tk = 0
            threadroi = []
            data = []
            for ct in  cut_time(self._begin_time,self._end_time):
                nullroi_sql = 'select log_date,nick,extra from cps_zuanshi_account_rpt where nick in ' + m_shops_nullroi + " and  log_date = '" + ct + "'" 
                z = threading.Thread(target=self.sql_theard,args=([nullroi_sql,2]))
                z.setDaemon(True) 
                threadroi.append(z)
                threadroi[tk].start()
                tk += 1
            for i in range(0,tk):
                threadroi[i].join()
                for row in self.m_roidata[i]:
                    col = []
                    col.append(row[1])
                    col.append(row[0])
                    rw = json.loads(row[2])
                    per_order3 = 0
                    per_order7 = 0
                    per_order15 = 0
                    if float(rw["alipayInShopNum"]) != 0:
                        per_order3 = float(rw["roi"]) * float(rw["charge"]) / float(rw["alipayInShopNum"])
                    if float(rw["alipayInShopNum7"]) != 0:
                        per_order7 = float(rw["roi7"]) * float(rw["charge"]) / float(rw["alipayInShopNum7"])
                    if float(rw["alipayInShopNum15"]) != 0:
                        per_order15 = float(rw["roi15"]) * float(rw["charge"]) / float(rw["alipayInShopNum15"])
                    col.append(per_order3)
                    col.append(per_order7)
                    col.append(per_order15)
                    data.append(col)
            return data
        except Exception,e: 
            print nullroi_sql
            return None

    def setroi(self,m_data_list,m_roilist):
        tmp = 0
        if  self._cur_table in [CY, ZYW]:
            tmp = 1
        if  self._cur_table == DXZYW:
            tmp = -2        
        rownum = 0
        for row in m_data_list.iterrows():
            if row[1][u"店铺名"] not in  self._shops_nullroi:
                rownum = rownum + 1
                continue
            for roirow in m_roilist:
                if (roirow[0] == row[1][u"店铺名"]) and (str(roirow[1]) == str(row[1][u"时间"])):
                    #3天roi更新
                    if float(m_data_list.iloc[rownum,11 - tmp]) > 0:
                        m_data_list.iloc[rownum,14 - tmp] = round((float(m_data_list.iloc[rownum,17 - tmp]) * float(roirow[2]) / float(m_data_list.iloc[rownum,11 - tmp])),2)
                    else:
                        m_data_list.iloc[rownum,14 - tmp] = 0
                    #7天roi更新
                    if float(m_data_list.iloc[rownum,11 - tmp]) > 0:
                        m_data_list.iloc[rownum,15 - tmp] = round((float(m_data_list.iloc[rownum,18 - tmp]) * float(roirow[3]) / float(m_data_list.iloc[rownum,11 - tmp])),2)
                    else:
                        m_data_list.iloc[rownum,15 - tmp] = 0
                    #15天roi更新
                    if float(m_data_list.iloc[rownum,11 - tmp]) > 0:
                        m_data_list.iloc[rownum,16 - tmp] = round((float(m_data_list.iloc[rownum,19 - tmp]) * float(roirow[4]) / float(m_data_list.iloc[rownum,11 - tmp])),2)
                    else:
                        m_data_list.iloc[rownum,16 - tmp] = 0
            rownum = rownum + 1
        return m_data_list
  
    def dx_catset(self,m_dxs):
        dx_cats=[]
        for dx in m_dxs:
            dx_cat = u""
            dx = str(dx)
            if dx.find(u'潜客') >= 0 :
                dx_cat = u'潜客'
            else:
                if dx.find(u'新客') >= 0 or dx.       find(u'扩展') >= 0 or dx.find(u'拉新') >= 0:
                    dx_cat = u'新客'
                else:
                    if dx.find(u'老顾客') >= 0 or dx.find(u'老客户') >= 0 or dx.find(u'老客') >= 0:
                        dx_cat = u'老顾客'
            dx_cats.append(dx_cat)
        return dx_cats

    def GetAdboardData(self,m_adboardidlist):                
        cydatalist = []
        m_adboardIdstr = "("
        for id in m_adboardidlist:
            m_adboardIdstr = m_adboardIdstr +"'" + str(id)  +"',"
        m_adboardIdstr= m_adboardIdstr[:len(m_adboardIdstr) - 1] + ")"
        conn = mysql_connection(self._port)
        m_cur = conn.cursor()
        sqlstr =  "select  adboardId ,nick , logdate , data from cps_zuanshi_aboard_package " + " where adboardId in " + m_adboardIdstr  
        m_cur.execute(sqlstr)
        rows = m_cur.fetchall()
        for row in rows: 
            cydata =[0 for i in range(3)]
            cydata[0] = row[0]
            cydata1 = pd.read_json('[' + row[3] + ']')
            cydata[1] = (cydata1.iloc[0,0])[u"adboardSize"]
            cydata[2] = u"'" + str((cydata1.iloc[0,0])[u"imagePath"])
            cydatalist.append(cydata)
        m_cur.close()
        conn.close()
        self.cydatalists.append(pd.DataFrame(cydatalist,columns = [u"创意ID",u"创意尺寸",u"创意链接"]))

    
    def source2_thread(self , data , col0 ,col1):
        data_list = pd.DataFrame()
        for d in data:
            m_time = str(d[0])   #记录时间
            m_shop = str(d[1])   #店铺名
            m_effect = str(d[2])   #记录当前记录是多少天的
            m_offset = str(d[3])   #记录当前的分块
            if d[3] is None: continue
            m_data_list = str(d[4])  #数据data
            m_data_list='[' + m_data_list + ']'
            m_data_list = pd.read_json(m_data_list) 
            if  m_data_list.size != 1: continue
            if  m_data_list.iloc[0,0] is None or m_data_list.iloc[0,0] == []: continue              
            m_data_list = m_data_list.iloc[0,0]
            m_data_list = pd.DataFrame(m_data_list)
                
            m_data_list[u"天数"] = m_effect
            m_data_list[u"分块"] = m_offset
            m_data_list[u"时间"] = m_time
            m_data_list[u"店铺名"] = m_shop
            m_data_list[u"类目"] = self.shopfindcat(m_shop)

            #计算量设置
            cal_sets = [[u"roi",u"alipayInshopAmt",u"charge"],[u"ecpc",u"charge",u"click"],[u"ctr",u"click",u"adPv"],[u"ecpm",u"charge",u"adPv"]]
            for cal_set in  cal_sets:
                if cal_set[0] not in m_data_list.columns:
                    t = 1
                    if cal_set[0] == u"ecpm":
                        t = 1000
                    m_data_list[cal_set[0]] = 0
                    for i in xrange(0,len(m_data_list)):
                        if float(m_data_list.loc[i,cal_set[2]]) != 0:
                            m_data_list.loc[i,cal_set[0]] = float(m_data_list.loc[i,cal_set[1]]) * t / float(m_data_list.loc[i,cal_set[2]])
                else:
                    for i in xrange(0,len(m_data_list)):
                        if np.isnan(m_data_list.loc[i,cal_set[0]]):
                            m_data_list.loc[i,cal_set[0]] = 0
                    
            #营业额设置
            if self._Turnoverlist != {}:
                turnoverflag = False
                if  m_offset == u"0":
                    for (d,v) in (self._Turnoverlist[m_time]).items():
                        if d == self.nickfindshopname(m_shop):
                            turnoverflag = True
                            m_data_list[u"营业额"] = float(v) / (float(len(m_data_list )) )
                            break
                if turnoverflag == False:
                    m_data_list[u"营业额"] = float(0)
                        
            if self._cur_table in [DX ,DXZYW]:
                m_data_list = pd.merge(m_data_list, pd.read_excel(u"数据指标对照表.xlsx",sheetname = u"人群类型对照表"),left_on = u"targetName", right_on=u'定向名称',how='left')
            if self._cur_table in [ZYW ,DXZYW]:
                m_data_list = pd.merge(m_data_list, pd.read_excel(u"数据指标对照表.xlsx",sheetname = u"资源位对照表"),left_on = u"adzoneName", right_on=u'资源位名称',how='left') 
            if self._cur_table == CY and u"adboardId" in m_data_list.columns:
                self.cydatalists.extend([x for x in m_data_list[u"adboardId"]])
            m_data_list = m_data_list[col0] 
            m_data_list.columns =  col1
  
            data_list = pd.concat([data_list,m_data_list] , ignore_index = True)
        self.m_data_list.append(data_list)
        
    def array_cut(self,pre_cut,batch_size):
        aft_cut = []
        num = 0
        while (len(pre_cut) > 0):
            if len(pre_cut) < batch_size:
                aft_cut.append(pre_cut)
                pre_cut = []
            else:
                aft_cut.append(pre_cut[:batch_size])
                pre_cut = pre_cut[batch_size:]   
            num += 1
        return aft_cut,num
        
    def get_source2(self,m_data):
        data_list = pd.DataFrame()  #报表容器
        if m_data:
            col0 = self._table_struct2[self._cur_table][0]
            col1 = self._table_struct2[self._cur_table][1]
            col2 = self._table_struct1[self._cur_table][1]
            index_num = 0
            if self._cur_table in [ZYW,CY] :
                index_num = 14
            if self._cur_table == DX:
                index_num = 15
            if self._cur_table == DXZYW:
                index_num = 17
            merge_col = (col1)[: index_num]
            self.cydatalists = []
            self.m_data_list = []
            if self._cur_table == CY :
                col0 = [col for col in col0 if (col != u"创意尺寸" and col != u"创意链接")] 
                col1 = [col for col in col1 if (col != u"创意尺寸" and col != u"创意链接")]
            if self._Turnoverlist != {}: 
                col0 = col0 + [u"营业额"]
                col1 = col1 + [u"营业额"]
                col2 = col2 + [u"营业额"]

            #多线程处理
            time0 = time.time()
            batch_size = int(len(m_data) / 10) + 1
            threads2 = []
            data ,tk = self.array_cut(m_data,batch_size)
            for i in xrange(0,tk):
                t = threading.Thread(target=self.source2_thread,args=([data[i],col0 ,col1]))
                t.setDaemon(True) 
                threads2.append(t)
                threads2[i].start()    
            for i in xrange(0,tk):
                threads2[i].join()  
                data_list =  pd.concat([data_list,self.m_data_list[i]] , ignore_index = True)
            time1 = time.time()
            print "--循环处理每行记录耗时:" + str(int(time1 - time0))

            #创意报表设置            
            threadscy = []
            if len(self.cydatalists) > 0:
                adboardidlist = self.cydatalists 
                self.cydatalists = []
                adboardidlist = list(set(adboardidlist))
                batch_size = 20
                adboardidlists ,tk = self.array_cut(adboardidlist,batch_size)
                for i in range(0,tk):
                    z = threading.Thread(target=self.GetAdboardData,args=([adboardidlists[i]]))
                    z.setDaemon(True) 
                    threadscy.append(z)
                for i in range(0,tk):
                    threadscy[i].start()
                    
            days = [3,7,15]
            lists = [data_list[data_list[u"天数"] == str(days[0])].copy(), data_list[data_list[u"天数"] == str(days[1])].copy(), data_list[data_list[u"天数"] == str(days[2])].copy()]
            for i  in xrange(0,len(lists)): 
                lists[i] = lists[i].rename(columns = {u"点击回报率":(str(days[i]) + u"天点击回报率"),u"成交金额":(str(days[i]) + u"天成交金额"),
                                        u"顾客订单数":(str(days[i]) + u"天顾客订单数"),u"加购物车数":(str(days[i]) + u"天加购物车数")})
            data_list = lists[0].copy()
            for i in xrange(1,len(lists) ):
                data_list = pd.merge(data_list , lists[i] ,on = merge_col,how = 'outer')
            mcolumns =  [de for de in col1  if de  not in ( merge_col  + [u"点击回报率", u"成交金额", u"顾客订单数",u"加购物车数"])]     
            time2 = time.time()
            print "--分裂日期耗时:" + str(int(time2 - time1))
            
            for i in xrange(0,len(data_list)):
                for col in mcolumns:
                    tmp = [ data_list[col][i], data_list[col + u"_x"][i], data_list[col + u"_y"][i]] 
                    for  d in tmp:
                        if (str(d)) != u"nan":
                            data_list.loc[i,col] = d
                            break
            time3 = time.time()
            print "--日期合并处理耗时:" + str(int(time3 - time2))
            if self._cur_table == CY:
                cydatalist = pd.DataFrame()
                for i in range(0,tk):
                    threadscy[i].join()
                    cydatalist =pd.concat([cydatalist ,self.cydatalists[i]],  ignore_index = True)  
                data_list = pd.merge(data_list,cydatalist,on = u"创意ID",how = 'left')  
            data_list = data_list[col2] 
        return data_list

    def get_source1(self,m_data):
        if m_data:
            data_list = pd.DataFrame()  #报表容器
            data_list.iteritems
            m_shops_nullroi = "("    #用于构造没有ROI的店铺字符串
            col0 = self._table_struct1[self._cur_table][0]
            col1 = self._table_struct1[self._cur_table][1]
            if self._Turnoverlist != {}: 
                col0 = col0 + [u"营业额"]
                col1 = col1 + [u"营业额"]
            time0 = time.time()
            self._shops_nullroi = []
            for d in m_data:
                m_shop = str(d[1])   #店铺名
                m_time = str(d[0])   #记录时间
                if d[2] is None:
                    continue
                m_data_list = str(d[2])  #数据data
                m_data_list='[' + m_data_list + ']'
                m_data_list = pd.read_json(m_data_list) 
                if  m_data_list.size != 3:
                    continue
                if  m_data_list.iloc[0,1] is None or m_data_list.iloc[0,1] == []:
                    continue
                m_data_list = m_data_list.iloc[0,1]
                m_data_list = pd.DataFrame(m_data_list)
                for m_roi in m_data_list[u"roi"]:
                    if m_roi is None and str(d[1]) not in self._shops_nullroi:
                        m_shops_nullroi = m_shops_nullroi + " '" + str(d[1]) + "',"
                        self._shops_nullroi.append(str(d[1]))
                        break

                m_data_list[u"时间"] = m_time
                m_data_list[u"店铺名"] = m_shop
                m_data_list[u"类目"] = self.shopfindcat(m_shop)
                m_data_list[u"ctrStr"] =[ float(x) for x in (m_data_list[u"ctrStr"])]
                m_data_list[u"3天成交金额"] = 0 #初始化全部置0
                m_data_list[u"7天成交金额"] = 0
                m_data_list[u"15天成交金额"] = 0

                #如果是创意报表需要特殊加入两个列
                if self._cur_table == CY:
                    adboardSize = []
                    imagePath = []
                    if "adboardDO" in m_data_list.keys() :
                        for j_data in m_data_list["adboardDO"]:
                            if j_data is None:
                                adboardSize.append(u"无数据")
                                imagePath.append(u"无数据")
                            else:
                                ad = json.dumps(j_data, encoding="UTF-8", ensure_ascii=False) 
                                ad = json.loads(str(ad))
                                adboardSize.append(ad["adboardSize"])
                                imagePath.append(u"'" + ad["imagePath"])
                    m_data_list[u"创意尺寸"] = adboardSize
                    m_data_list[u"创意链接"] = imagePath
                
                #营业额设置
                if self._Turnoverlist != {}:
                    turnoverflag = False
                    for (d,v) in (self._Turnoverlist[m_time]).items():
                        if d == self.nickfindshopname(m_shop):
                            turnoverflag = True
                            m_data_list[u"营业额"] = float(v) / float(len(m_data_list))
                            break
                    if turnoverflag == False:
                        m_data_list[u"营业额"] = float(0)
                if self._cur_table in [DX ,DXZYW]:
                    m_data_list = pd.merge(m_data_list, pd.read_excel(u"数据指标对照表.xlsx",sheetname = u"人群类型对照表"),left_on = u"targetName", right_on=u'定向名称',how='left')
                if self._cur_table in [ZYW ,DXZYW]:
                    m_data_list = pd.merge(m_data_list, pd.read_excel(u"数据指标对照表.xlsx",sheetname = u"资源位对照表"),left_on = u"adzoneName", right_on=u'资源位名称',how='left') 
                m_data_list =m_data_list[col0] 
                m_data_list.columns = col1
                data_list =  pd.concat([data_list,m_data_list],ignore_index=True)
            time1 = time.time()
            print "--循环处理每行记录耗时:" + str(int(time1 - time0))  
            if len(self._shops_nullroi) > 0:
                m_shops_nullroi = m_shops_nullroi[:(len(m_shops_nullroi) - 1)] + ")"
                roilist = self.get_per_order1(m_shops_nullroi)
                data_list = self.setroi(data_list,roilist)
            cal_set = [3 , 7 ,15]
            for cs in cal_set:
                t=[]
                for i in data_list[(str(cs) + u"天点击回报率")]:
                    if i is None :
                        i = float(0)
                    t.append(i)
                data_list[(str(cs) +u"天点击回报率")] = t
                data_list[(str(cs) + u"天成交金额")] = data_list[(str(cs) + u"天点击回报率")] * data_list[u"消耗"]
            time2 = time.time()
            print "--空值处理耗时:" + str(int(time2 - time1)) 
            return data_list
        else:
            return None   
            
    def GetTurnoverlist(self,m_shops , m_begintime,m_endtime):
        conn = []
        try:
            conn = mysql_connection(self._port)
            cur_data = []
            for ct in  cut_time(m_begintime,m_endtime):
                self._cur = conn.cursor()
                self._cur.execute(timeout_sql)
                turnover_sql = 'select log_date,shopname,payAmt from cps_shop_trade_rpt where shopname in ' + m_shops + " and  log_date ='" + ct + "' "
                self._cur.execute(turnover_sql)
                cur_data.extend(self.data_translate(self._cur))
                self._cur.close()
            conn.close()
            data = dict()
            for row in cur_data:
                if str(row[0]) in  data.keys():
                    data[str(row[0])].update({row[1]: str(row[2])})
                else:
                    data.update({str(row[0]):{row[1]: str(row[2])}})
            return data
        except Exception,e: 
            print e
            self._cur.close()
            conn.close()
            return None
    
    def sql_theard(self,sqlstr,mod):
        conn = mysql_connection(self._port)
        m_cur = conn.cursor()
        m_cur.execute(timeout_sql)
        m_cur.execute(sqlstr)
        if mod == 1:
            self.m_data.append(self.data_translate(m_cur))
        else:
            if mod == 2:
                self.m_roidata.append(self.data_translate(m_cur))
        m_cur.close()
        conn.close()
    
    def get_table(self,**kwargs):   #根据m_shopname 和 m_categorys获取 m_cur_table类型 的数据源表
        self._cur_table = self._tablecatstring[kwargs["m_cur_table"]]
        if ("m_shopname" not in  kwargs.iterkeys()) :
            self._shopname = []
        else:
            self._shopname = kwargs["m_shopname"]
        if ("m_categorys" not in  kwargs.iterkeys()) :
            self._categorys = []
        else:
            self._categorys = kwargs["m_categorys"]

        if ("m_source" not in  kwargs.iterkeys()) :
            m_source = True
        else:
            m_source = kwargs["m_source"]
            if type(m_source) != type(True):
                print "m_source必须是bool类型"
                return False
        if ("m_file_name1" not in  kwargs.iterkeys()) :
            m_file_name1 = ""
        else:
            m_file_name1 = kwargs["m_file_name1"]
        if ("m_turnover" not in  kwargs.iterkeys()) :
            m_turnover = False
        else:
            m_turnover = kwargs["m_turnover"]
            if type(m_turnover) != type(True):
                print "m_turnover必须是bool类型"
                return False
        self._begin_time = kwargs["m_begin_time"]
        self._end_time = kwargs["m_end_time"]

        if self._shopname is None and self._categorys is None:
            print "shopname 和 categorys 不能同时为空"
            return False
        if self._begin_time is None or self._end_time is None:
            print "begin_time 和 end_time 不能为空"
            return False
        if self._cur_table not in self._tablecat:
            print "cur_table 必须是' DX, ZYW, CY, DXZYW' 中的一个"
            return False
        if  os.path.exists(u'数据指标对照表.xlsx') == False :
            print "'数据指标对照表.xlsx' 不存在"
            return False
        print "---------"
        server = SSH_Set(self._port)    
        server.start()
        data_list = pd.DataFrame()
        file_name = ""
        try:
            time0 = time.time()
            self._shopscategory_data = self.shops_category()
            time1 = time.time()
            print "初始化店铺类目表耗时:" + str(int(time1 - time0))

            #营业额设置    
            if m_turnover:
                shopstring = self.get_shopstring()
                self._Turnoverlist = self.GetTurnoverlist(shopstring ,self._begin_time, self._end_time) 
                 
            begin_time = datetime.datetime.strptime(self._begin_time, '%Y-%m-%d')
            end_time = datetime.datetime.strptime(self._end_time, '%Y-%m-%d')
            spilt_time = datetime.datetime.strptime('2016-07-16', '%Y-%m-%d')            
            times = []
            if (begin_time >= spilt_time):
                times = [0,[begin_time.strftime('%Y-%m-%d'),end_time.strftime('%Y-%m-%d')]]
            else:
                if (end_time < spilt_time):
                    times = [[begin_time.strftime('%Y-%m-%d'),end_time.strftime('%Y-%m-%d')],0]
                else:
                    times = [ [begin_time.strftime('%Y-%m-%d') , '2016-07-15'], ['2016-07-16' , end_time.strftime('%Y-%m-%d')]]
            
            #数据报表处理
            timeall_begin = time.time()
            for i in xrange(0,len(times)):
                if times[i] == 0:
                    continue
                time2 = time.time()
                threadssql = []
                tk  = 0
                data = []
                self.m_data = []
                for ct in cut_time(times[i][0], times[i][1]):
                    if  i == 0:
                        execute_sql = self.datasqlstring( " logdate,nick,data " ,self._tables1[self._cur_table] ,ct);
                    else:
                        execute_sql = self.datasqlstring( " logdate,nick,effect,offset,data " ,self._tables2[self._cur_table] ,ct);

                    z = threading.Thread(target=self.sql_theard,args=([execute_sql,1]))
                    z.setDaemon(True) 
                    threadssql.append(z)
                    threadssql[tk].start()
                    tk += 1
                print "\n进入第" +str(i + 1) +"种钻展报表解析"
                print "共有" + str(tk) + "个主sql线程："
                for j in range(0,tk):
                    threadssql[j].join()
                    time3 = time.time()
                    print "-第" + str(j + 1) + "个sql执行及翻译耗时(" + str(j + 1) + "/" + str(tk) + "):" + str(int(time3 - time2))
                    if i == 0:
                        data = self.get_source1(self.m_data[j])
                    else:
                        data = self.get_source2(self.m_data[j])
                    time5 = time.time()
                    print "-第" + str(j + 1) + "个子报表结构处理耗时(" + str(j + 1) + "/" + str(tk) + "):" + str(int(time5 - time3)) + "\n"
                    data_list = pd.concat([data_list, data])
            timeall_end = time.time()
            print  "\n报表源数据总处理耗时:" + str(int(timeall_end - timeall_begin))
            if self._shopname == []:
                if len(self._categorys) > 1:
                    file_name = self._tablename[self._cur_table] + u"_" + self._categorys[0] + u"等_" + self._begin_time + u"至" + self._end_time + u"报表.xlsx"
                else:
                    file_name = self._tablename[self._cur_table] + u"_" + self._categorys[0] + u"_" + self._begin_time + u"至" + self._end_time + u"报表.xlsx"
            else:
                if len(self._shopname) > 1:
                    file_name = self._tablename[self._cur_table] + u"_" + self._shopname[0] + u"等_" + self._begin_time + u"至" + self._end_time + u"报表.xlsx"
                else:
                    file_name = self._tablename[self._cur_table] + u"_" + self._shopname[0] + u"_" + self._begin_time + u"至" + self._end_time + u"报表.xlsx"
            file_name = file_name.replace('/','')
  
            if m_file_name1 != "" and m_file_name1 is not None:
                s1= os.path.split(m_file_name1)
                if s1[0] != "":
                    if (os.path.isdir(s1[0]) == False):
                        os.mkdir(s1[0] + "/")
                    if s1[1] == "":
                        file_name = s1[0] + "\\\\"+ file_name
                    else:
                        file_name = m_file_name1 
                else:
                    if ("m_file_name2"  in  kwargs.iterkeys()) :
                        s2= os.path.split(kwargs["m_file_name2"])
                        if s2[0] != "":
                            if (os.path.isdir(s2[0]) == False):
                                os.mkdir(s2[0] + "/")
                        file_name = s2[0] + "\\\\" +  s1[1]
                m_source = True
            if m_source:
                data_list.to_excel(file_name, sheet_name = u'sheet1' , index = False, encoding='utf-8')
                timeall_save = time.time()
                print  "保存Excel("+ file_name +")耗时:" + str(int(timeall_save - timeall_end))
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        server.stop()
        self.Clear()
        print "---------"
        return data_list,file_name
            
    def sum_levels_set(self,levels):
        q=[]
        for x in levels:
            p = []
            for y in x:
                p.append(y)
            p.append(u"汇总")
            q.append(p)
        return q

    def sum_index_set(self,m_index, m_indexlevels_num , col_num):#m_index是要仿照的index col_num是指根据col_num修改m_index的列号，把这个列号对于的序列变为"汇总",m_indexlevels_num是levels里面“汇总”所在的序号 一般有几个索引就会有多少个元素
        m_labels = m_index.labels
        lbl = []
        for i in xrange(0 , len(m_labels)):
            if col_num + 1 <= i:
                lbl.append([m_indexlevels_num[i] - 1])
            else:
                lbl.append(m_labels[i])
        m_index = pd.MultiIndex(levels = m_index.levels,labels =lbl,names = m_index.names)
        return m_index

        
    def Caluate(self,A,eva,B):
        print 123
        if eva == u"+": return A + B
        if eva == u"-": return A - B
        if eva == u"*": return A * B
        if eva == u"/":
            if type(B) is types.FloatType or type(B) is types.IntType or type(B) is types.LongType : 
                if float(B) == 0:
                    return 0
                else:
                    return A / B
            C = B.copy()
            C[C != 0] = A / B
            return C
    
    def Calulate_Pivot(self,m_cal_col , m_coutlist):
        if type(m_cal_col) is types.DictionaryType:
            if u"sum" in m_cal_col.keys():
                return (self.Calulate_Pivot(m_cal_col[u"sum"],m_coutlist)).sum()  
            else:
                if u"mean" in m_cal_col.keys():
                    return (self.Calulate_Pivot(m_cal_col[u"mean"],m_coutlist)).mean() 
                else:
                    if u"min" in m_cal_col.keys():
                        return (self.Calulate_Pivot(m_cal_col[u"min"],m_coutlist)).min() 
                    else:
                        if u"max" in m_cal_col.keys():
                            return (self.Calulate_Pivot(m_cal_col[u"max"],m_coutlist)).max() 
                        else:
                            if u"median" in m_cal_col.keys():
                                return (self.Calulate_Pivot(m_cal_col[u"median"],m_coutlist)).median() 
                            else:
                                if u"abs" in m_cal_col.keys():
                                    return (self.Calulate_Pivot(m_cal_col[u"abs"],m_coutlist)).abs() 
                                else:
                                    if u"mode" in m_cal_col.keys():
                                        return (self.Calulate_Pivot(m_cal_col[u"mode"],m_coutlist)).mode() 
            return 0
        if type(m_cal_col) is types.UnicodeType or type(m_cal_col) is types.StringType:
            return m_coutlist[m_cal_col].copy()
        if type(m_cal_col) is types.FloatType or type(m_cal_col) is types.IntType or type(m_cal_col) is types.LongType :
            return m_cal_col
        if type(m_cal_col) is types.ListType and len(m_cal_col) == 3:
            return self.Caluate(self.Calulate_Pivot(m_cal_col[0],m_coutlist) ,m_cal_col[1] , self.Calulate_Pivot(m_cal_col[2],m_coutlist))

    def Caluate_Set(self,data1):
        for cal in self.cal_list :
            data1[cal[u"name"]] = self.Calulate_Pivot(cal[u"calulate_col"],data1)
            if u"classfy" in cal.keys():
                if len(cal[u"classfy"][0]) + 1 == len(cal[u"classfy"][1]):
                    data1[cal[u"name"]] = data1[cal[u"name"]].astype(unicode)
                    for d in xrange(0,len(data1[cal[u"name"]])):
                        find_flag = False
                        for cla in  xrange(0,len(cal[u"classfy"][0])):
                            if float(data1[cal[u"name"]][d]) <= cal[u"classfy"][0][cla]:
                                data1[cal[u"name"]][d] = cal[u"classfy"][1][cla]
                                find_flag = True
                                break
                        if find_flag == False:
                            data1[cal[u"name"]][d] = cal[u"classfy"][1][len(cal[u"classfy"][0])]
            else:
                if u"sort" in cal.keys() :
                    if cal[u"sort"] == 0:
                        data1 = data1.sort_index(by =cal[u"name"],ascending = False)
                    else:
                        data1 = data1.sort_index(by =cal[u"name"])
                save_num = 2
                if u"foramt" in cal.keys() :
                    if cal[u"foramt"] == u"%":
                        data1[cal[u"name"]] = [(str(round((x * 100),2)) + str(u"%")) for x in data1[cal[u"name"]] ] 
                        continue
                    try:
                        save_num = int(cal[u"foramt"])
                    except ValueError:
                        save_num = 2
                data1[cal[u"name"]] = [round(x,save_num) for x in data1[cal[u"name"]] ] 
        return data1
                                
    def pivot_table2(self,pivot_table1):#对初始化的pivot_table1做透视表 根据pivot_table1的索引做求和聚合
        #获取初始化透视表的索引

        indexlevels = pivot_table1.index.levels
        indexlabels = pivot_table1.index.labels
        indexlevels_num = [len(x) + 1 for x in indexlevels]
        lenindex = len(indexlabels)
        indexlabel = indexlabels[0:lenindex - 1]
        i_index = 0 #位于第几个索引
        i_begin = 0 #本次汇总的开头列
        i_end = 0 #本次汇总的结尾行
        labels = []
        for j in xrange(0,len(indexlabel)):
            label = []
            i_begin = 0
            for i in xrange(0, len(indexlabel[j])):
                if i == len(indexlabel[j]) - 1:
                    i_end = i + 1
                    label.append([i_begin,i_end,i_index])
                    continue
            
                label_flag = False #当时False 说明当前的iend指标与下一个index是不同的，应该分裂出index
                for t in xrange(0 , j + 1):            
                    if indexlabel[t][i] != indexlabel[t][i + 1]:
                        label_flag = True
                        break
                if label_flag:
                    i_end = i + 1
                    label.append([i_begin,i_end,i_index])
                    # i_sum = 0
                    i_begin = i_end 
            labels.append(label)
            i_index = i_index + 1
        
        #构造索引树
        header = node([0 , i_end , -1])#增加一个总索引 并以此作为母节点
        cur_node = []
        curnum = 0
        for i in xrange(0,len(labels)):
            if i > 0:
                curnum = 0
                cur_node = labels[i - 1][curnum] #把上层的第一个节点作为本次的初始对照
            for j in xrange(0,len(labels[i])):
                labels[i][j] = node(labels[i][j])
                if i == 0:
                    header.add(labels[i][j])
                else:
                    node_data = labels[i][j].getdata()
                    cur_nodedata = cur_node.getdata()
                    if int(node_data[1]) > int(cur_nodedata[1]):
                        curnum = curnum + 1
                        cur_node = labels[i - 1][curnum]
                    (labels[i - 1][curnum]).add(labels[i][j])#把这个节点放到当前的上层第cunum个节点下面
        self._tree.clear()
        self._tree.linktohead(header)
        tree_sortdata =  [x for x in self._tree.gettall() if (x != "header")]#去掉最后两个header

        #处理初始化后的的透视表
        data = pd.DataFrame()
        pivot_table1.index = pd.MultiIndex(levels = self.sum_levels_set(pivot_table1.index.levels),labels =pivot_table1.index.labels,names =pivot_table1.index.names)
        for i_sum in tree_sortdata:
            i_begin = i_sum[0]
            i_end = i_sum[1]
            index_curnum = i_sum[2]
            data1 = []
            data_sum = pd.DataFrame(pivot_table1[i_begin:i_end].sum()).T
            data_sum.index = self.sum_index_set((pivot_table1[i_end - 1 : i_end]).index , indexlevels_num , index_curnum)
            data_sum = self.Caluate_Set(data_sum)
            if index_curnum == lenindex - 2:
                data1 = pivot_table1[i_begin:i_end]
                if u"消耗" in data1.columns:
                    data1 = data1.sort_index(by =[u"消耗"],ascending = [1])
                data1 = self.Caluate_Set(data1)
                data1 = pd.concat([data1,data_sum])
            else:
                data1 = data_sum                                
            data = pd.concat([data,data1]) 
        return data
 
    def diamond_pivotset(self,m_pivot_table):
        for ser in calculate_series[0]:
            if ser[0] in m_pivot_table.columns and ser[1] in m_pivot_table.columns and ser[2] in m_pivot_table.columns:
                m_pivot_table.loc[m_pivot_table[ser[2]] <= 0,ser[0]] = 0
                if ser[0] == u"千次展现成本(元)": #千次展现成本的特殊处理
                    m_pivot_table.loc[m_pivot_table[ser[2]] > 0,ser[0]] =  m_pivot_table.loc[m_pivot_table[ser[2]] > 0,ser[1]] *1000 /  m_pivot_table.loc[m_pivot_table[ser[2]] > 0,ser[2]]
                else:
                    m_pivot_table.loc[m_pivot_table[ser[2]] > 0,ser[0]] =  m_pivot_table.loc[m_pivot_table[ser[2]] > 0,ser[1]] /  m_pivot_table.loc[m_pivot_table[ser[2]] > 0,ser[2]]
   
        for ser in calculate_series[1]:
            if len(ser) == 4:
                if  ser[1] in m_pivot_table.columns and ser[2] in m_pivot_table.columns and ser[3] in m_pivot_table.columns:
                    m_pivot_table.loc[m_pivot_table[ser[3]] <= 0,ser[0]] = 0
                    m_pivot_table.loc[m_pivot_table[ser[3]] > 0,ser[0]] =  (m_pivot_table.loc[m_pivot_table[ser[3]] > 0,ser[1]] + m_pivot_table.loc[m_pivot_table[ser[3]] > 0,ser[2]]) /  m_pivot_table.loc[m_pivot_table[ser[3]] > 0,ser[3]]
                    continue
            if  ser[1] in m_pivot_table.columns and ser[2] in m_pivot_table.columns:
                m_pivot_table.loc[m_pivot_table[ser[2]] <= 0,ser[0]] = 0
                m_pivot_table.loc[m_pivot_table[ser[2]] > 0,ser[0]] =  m_pivot_table.loc[m_pivot_table[ser[2]] > 0,ser[1]] /  m_pivot_table.loc[m_pivot_table[ser[2]] > 0,ser[2]]
        return m_pivot_table
    
    
    def To_diamond_pivot(self,**kwargs):  #根据m_datalist 的源数据 按照m_pivot_index为索引 ，m_pivot_col为透视列做透视表
    ###处理透视表
    #对datalsit初始化透视表
        self._shopname 
        self._categorys
        self._begin_time 
        self._end_time
    
        file_name = "pivot_table.xlsx"
        time6 = time.time()
        if ("m_datalist" not in  kwargs.iterkeys()) :
            print "m_datalist不能为空"
            return False
        else:
            m_datalist = kwargs["m_datalist"]
        if ("m_pivot_index" not in  kwargs.iterkeys()) :
            m_pivot_index = [u"类目",u"店铺名"]
        else:
            m_pivot_index = kwargs["m_pivot_index"]

        if ("m_file_name2"  in  kwargs.iterkeys()) and (kwargs["m_file_name2"] != "") :
            file_name = kwargs["m_file_name2"]
        else:
            print "m_file_name2 不可为空!"
            return False
        if ("m_cal_list"  in  kwargs.iterkeys()) :
            self.cal_list  = kwargs["m_cal_list"]
        m_pivot_col1 = []
        m_pivot_col = []
        print "---------"
        if ("m_pivot_col" not in  kwargs.iterkeys()) or (kwargs["m_pivot_col"] == []) :
            m_pivot_col1 = [u"展现",u"点击",u"点击率",u"消耗",u"千次展现成本(元)",u"点击单价(元)", u"3天点击回报率",u"7天点击回报率",u"15天点击回报率",u"3天顾客订单数",u"7天顾客订单数",u"15天顾客订单数",u"店辅收藏数",u"宝贝收藏数",u"访客",u"3天加购物车数",u"7天加购物车数",u"15天加购物车数",u"3天成交金额",u"7天成交金额",u"15天成交金额"]
            m_pivot_col = m_pivot_col1
        else:
            m_pivot_col1 = kwargs["m_pivot_col"]
            m_pivot_col = copy.deepcopy(kwargs["m_pivot_col"])
            for calculate_serie in calculate_series:
                for cs1 in calculate_serie:
                    if cs1[0] in m_pivot_col1:
                        for i in xrange(1,len(cs1)):
                            if cs1[i] not in m_pivot_col1: 
                                m_pivot_col1.append(cs1[i])
                    if cs1[0] not in m_datalist.columns:
                        m_datalist[cs1[0]] = 0
        for cal in self.cal_list :
            for li in re.findall("u\'(.*?)\'",str(self.cal_list)):
                li = li.decode("unicode_escape")
                if li in m_datalist.columns and li not in m_pivot_col1:
                    m_pivot_col1 += [li]
        #进一步聚合透视表
        if len(m_pivot_index) > 1:
            pivot_table1 = pd.pivot_table(m_datalist, index = m_pivot_index, values = m_pivot_col1,aggfunc = np.sum)
            pivot_table1 = pivot_table1[m_pivot_col1]
            time7 = time.time()
            print "初始化透视表耗时:" + str(int(time7 - time6))
            data = self.pivot_table2(pivot_table1)
        else:
            m_datalist.columns = m_datalist.columns
            data = pd.pivot_table(m_datalist, index = m_pivot_index, margins = True,values = m_pivot_col1,aggfunc = np.sum)
            time7 = time.time()
            print "初始化透视表耗时:" + str(int(time7 - time6))
            index1 = []
            for idx in data.index:
                index1.append(idx)
            index1[len(index1) - 1] =  u"汇总"
            data.index = (index1)
            
            data = data[m_pivot_col1]
            data.columns = m_pivot_col1
            data1  = pd.DataFrame(columns = data.columns , index = m_pivot_index)
            data = self.Caluate_Set(data)
            data = pd.concat([data1,data]) 
        time8 = time.time()
        print "聚合透视表耗时:" + str(int(time8 - time7))
        data = self.diamond_pivotset(data)
        for cal in self.cal_list:
            if u"name" in cal.keys() and cal[u"name"] not in m_pivot_col:
                m_pivot_col += [cal[u"name"]]
        data = data[m_pivot_col]
        data.columns = m_pivot_col
        s= os.path.split(file_name)
        if s[0] != "":
            if (os.path.isdir(s[0]) == False):
                os.mkdir(s[0] + "/")
            
        writer = pd.ExcelWriter(file_name, engine='xlsxwriter') #定义Excel输出器，使用xlsxwriter内核
        data.to_excel(writer, sheet_name = u'sheet1' , index = True, encoding='utf-8')  
        writer.save()
        self.Clear()
        print "---------"
        return data ,file_name
    
    def diamond_pivot_formatting(self,**kwargs):
        if ("readfile_name" not in  kwargs.iterkeys()) :
            print "readfile_name 不能为空"
            return False
        else:
            readfile_name = kwargs["readfile_name"]
        if ("savefile_name" not in  kwargs.iterkeys()) :
            savefile_name = readfile_name #覆盖原文件
        else:
            savefile_name = kwargs["savefile_name"]
        
        if not os.path.isfile(readfile_name):
            print u'文件路径不存在'
            sys.exit()
        time1 = time.time()
        data = xlrd.open_workbook(readfile_name)            # 打开文件
        table = data.sheet_by_index(0)              # 通过索引获取xls文件第0个sheet
        nrows = table.nrows                         # 获取table工作表总行数
        ncols = table.ncols 
        print "---------"
        s= os.path.split(savefile_name)
        if s[0] != "":
            if (os.path.isdir(s[0]) == False):
                os.mkdir(s[0] + "/")
        wb = xlsxwriter.Workbook(savefile_name)  #创建一个excel文件
        ws = wb.add_worksheet(u'sheet1')        #创建一个工作表对象
        
        nfm = wb.add_format({'num_format': '0.00%'})  #百分比数字
        nfm2 = wb.add_format({'num_format': '0.00'})  #取两位小数
    
        #首行空白格数
        sum_index = 0
        cat1 = [u"点击率",u"收藏率",u"3天转化率",u"7天转化率",u"15天转化率",u"3天加购率",u"7天加购率",u"15天加购率"]
        roicats =  [u"3天点击回报率",u"7天点击回报率",u"15天点击回报率"]
        rates = [u"点击率",u"收藏率",u"3天加购率"]
        cat2 = [u"千次展现成本(元)",u"3天点击回报率",u"7天点击回报率",u"15天点击回报率",u"点击单价(元)",u"3天成交金额",u"7天成交金额",u"15天成交金额"]
        cat1_index = []
        cat2_index = []
        roicats_index = []
        rates_index = []
        #设置单元格格式
        color = ["ffd966","f1c232" ,"e69138" ,"dd7e6b" ,"e6b8af" ,"00ffff" , "c27ba0" ,"d5a6bd" , "f1c232" , "87712f" , "f9cb9c"]
        format_title = wb.add_format({'border':1,'align':'center','bg_color':'e69138','font_size':10,'bold':True ,'font_name':'微软雅黑'})
        format_general = wb.add_format({'border':1,'align':'vcenter','font_size':9,'bold':False,'font_name':'微软雅黑'})
        format_mergetitle = wb.add_format({'border':1,'align':'vcenter','font_size':10,'bold':False,'font_name':'微软雅黑','bold':True,'bg_color':'d9ead3'})
        
        format_merges = []
        for i in xrange(8):
            f1 = wb.add_format({'border':1,'align':'vcenter','font_size':10,'bold':True,'font_name':'微软雅黑','bg_color':color[random.randint(0, 10)]})
            format_merges.append(f1)

        #复制数据
        sumcol = 0
        for i in xrange(nrows):
            sumflag  = False
            for j in  xrange(ncols):
                if table.cell_value(0,j) in  cat1:
                    cat1_index.append(j)
                if table.cell_value(0,j) in  cat2:
                    cat2_index.append(j)
                if table.cell_value(0,j) in  roicats:
                    roicats_index.append(j)
                if table.cell_value(0,j) in  rates:
                    rates_index.append(j)
                if table.cell_value(i,j) == u"汇总" :
                    if sumflag == False:
                        sumcol = j - 1
                    sumflag = True
                    sum_index = max(sum_index , j + 1)
                    ws.set_row(i, 13)
                if i == 0:
                    ws.write(i,j,table.cell_value(i,j),format_title)      #把获取到的值写入文件对应的行列
                else:
                    if sumflag:
                        ws.write(i,j,table.cell_value(i,j),format_merges[sumcol]) 
                    else:
                        ws.write(i,j,table.cell_value(i,j),format_general) 
        ws.set_column(0,sum_index - 1,17) 
        #合并单元格
        for j in xrange(sum_index - 1 , -1, -1):
            begin_merge = 1
            if (table.cell_value(0,j) == ""):
                ws.merge_range(0,j,1,j,table.cell_value(1,j),format_title)
                begin_merge = 2
            temp = table.cell_value(begin_merge,j)
            begin_row = begin_merge
            #format_merge.bg_color = color[j]
            for i in xrange(begin_row + 1, nrows):
                if table.cell_value(i,j) != "":
                    if begin_merge == i -1:
                        ws.write(i - 1,j,temp, format_mergetitle)
                    else:
                        ws.merge_range(begin_merge,j,i - 1,j,temp,format_mergetitle)
                    if sum_index - 1 >  j  and temp != u"汇总":
                        if sum_index - 1 == j + 1:
                            ws.write(i - 1,j + 1,temp + u" 合计",format_merges[j])
                        else:
                            ws.merge_range(i - 1,j + 1,i - 1,sum_index - 1,temp + u" 合计",format_merges[j])  
                    temp = table.cell_value(i,j)
                    begin_merge = i
        ws.merge_range(nrows - 1,0,nrows - 1,sum_index - 1, u"汇总",format_merges[sumcol])
        for j in xrange(sum_index ,ncols):
            if (table.cell_value(1,j) == ""):
                ws.merge_range(0,j,1,j,table.cell_value(0,j),format_title)

        #设置百分比和小数点
        for i in  cat1_index:
            ws.conditional_format(1, i, 10000, i, {'type':'cell', 'criteria':'!=', 'value':-1, 'format': nfm})
        for i in cat2_index:
            ws.conditional_format(1, i, 10000, i, {'type':'cell', 'criteria':'!=', 'value':-1, 'format': nfm2})
        
        wb.close()
        time2 = time.time()
        print "格式化耗时:" + str(int(time2 - time1))
        self.Clear()
        print "---------"

    def get_diamond_pivot(self,**kwargs):  #根据m_shopname 和 m_categorys获取 m_cur_table类型 的透视表 （按照m_pivot_index为索引 ，m_pivot_col为透视列做透视表）
        if ("m_cur_table" not in  kwargs.iterkeys()) :
            print "m_cur_table 不能为空"
            return False
        if ("m_shopname" not in  kwargs.iterkeys()) and ("m_categorys" not in  kwargs.iterkeys()):
            print "m_shopname 和 m_categorys不能同时为空"
            return False

        if ("m_begin_time" not in  kwargs.iterkeys()) :
            print "m_begin_time 不能为空"
            return False
        if ("m_end_time" not in  kwargs.iterkeys()) :
            print "m_end_time 不能为空"
            return False
        if self._tablecatstring[kwargs["m_cur_table"]] not in self._tablecat:
            print "m_cur_table 必须是' DX, ZYW, CY, DXZYW' 中的一个"
            return False
        if  u"m_file_name2"  in kwargs and u"m_file_name1"  not in kwargs :   
            s1= os.path.split(kwargs["m_file_name2"])
            if s1[0] != "":
                if (os.path.isdir(s1[0]) == False):
                    os.mkdir(s1[0] + "/") 
                kwargs["m_file_name1"] =  s1[0] + "\\\\"
        print "-----------"
        data,file_name = self.get_table(**kwargs)
        if data is False:
            return False
        kwargs["m_datalist"] = data
                        
        if u"m_file_name2"  in kwargs:
            s1= os.path.split(kwargs["m_file_name2"])
            s2= os.path.split(file_name)
            if s1[0] != "":
                if (os.path.isdir(s1[0]) == False):
                    os.mkdir(s1[0] + "/") 
                if s1[1] == "":
                    if re.findall("(.*?)\.xlsx",s2[1]):
                        file_name =  (re.findall("(.*?)\.xlsx",s2[1])[0])
                    kwargs["m_file_name2"] = s1[0] + "\\\\"+ file_name + "透视表.xlsx"
                         
        if u"m_file_name2" not in kwargs :
            if re.findall("(.*?)\.xlsx",file_name):
                file_name =  (re.findall("(.*?)\.xlsx",file_name)[0])
            kwargs["m_file_name2"] = file_name + "透视表.xlsx"
                    
        
        data1, readfile_name = self.To_diamond_pivot(**kwargs)
        kwargs["readfile_name"] = readfile_name
        kwargs["savefile_name"] = kwargs["m_file_name2"]
        time1 = time.time()
        if u"m_formatted" not in kwargs:
            self.diamond_pivot_formatting(**kwargs)
        else:
            if kwargs["m_formatted"] != False:
                self.diamond_pivot_formatting(**kwargs)
        self.Clear()
        print "-----------"
        print " "
        return data1
        

