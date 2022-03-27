





import logging
from numpy.lib.npyio import load
from pykiwoom.kiwoom import *
import os
import pandas as pd
import sqlite3
import requests
import pandas as pd
import time
import pymysql
from konfig import Config

logging.basicConfig(
    format='%(asctime)s : %(levelname)-10s : %(message)s',
    filename="log/Guardian_Angel.log",
    # filename="./log/example.log",
    filemode='a', 
    level=logging.DEBUG # 대문자 
)



def algo2_Fibonacci():
    # conn = sqlite3.connect("DataBase/buy_list.db")
    # c = conn.cursor()
    # c.execute(f"select code from code") 
    # conn.commit()
    # codes_1 = c.fetchall()
    # codes = []
    # for i in codes_1:
    #     codes.append(i[0])
    
    # ↑↑↑↑↑↑↑ 테스트용 codes 만들기....삭제할것.....#############################################################     
    
    for max_code in codes:
        try:
            sql = "SELECT open, close, high, low, vol, vol_pirce from sh{} ORDER by date DESC LIMIT 1".format(max_code) 
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            c.execute(sql)
            sum = c.fetchall()
            conn.close()
            고가 = sum[0][2]
            저가 = sum[0][3]
            바디 = (고가 - 저가)/저가*100
        except Exception as e:
            logging.exception("Exception occurred", exc_info=True)
        # sum[0][0] # 시가
        # sum[0][1] # 종가
        # sum[0][2] # 고가
        # sum[0][3] # 저가
        # sum[0][4] # 거래량
        # sum[0][5] # 거래대금
        
        # 당일 양봉 저가/고가 %로 종목선정 추가
        try:
            if sum[0][5] != 0 and sum[0][5] > 3000 and 바디 > 1.0: # 거래량이 없으면 거래정지 종목 / 거래대금 10억 이상인 종목
                if sum[0][0] < sum[0][1] : # 양봉만 검색
                    sql = "SELECT open, close, high, low, vol, vol_pirce from sh{} ORDER by date DESC LIMIT 3".format(max_code) 
                    sum = database(dbtype='sqlite3', load=True,db_name='buy_list', sql=sql)
                    conn.close()
                    if sum[0][0] < sum[0][1]:
                        if sum[1][0] < sum[1][1] or sum[1][0] == sum[1][1]:
                            if sum[2][0] < sum[2][1] or sum[2][0] == sum[2][1]:
                                #3
                                # sql = "SELECT open, close, high, low from sh{} ORDER by date DESC LIMIT 3".format(max_code) 
                                #시가
                                sql = "SELECT open from sh{} ORDER by date DESC LIMIT 3".format(max_code) 
                                open = database(dbtype='sqlite3', load=True,db_name='buy_list', sql=sql)
                                open = min(open)[0]
                                #저가
                                sql = "SELECT low from sh{} ORDER by date DESC LIMIT 3".format(max_code) 
                                low = database(dbtype='sqlite3', load=True,db_name='buy_list', sql=sql)
                                low = min(low)[0]
                                #고가
                                sql = "SELECT high from sh{} ORDER by date DESC LIMIT 3".format(max_code) 
                                high = database(dbtype='sqlite3', load=True,db_name='buy_list', sql=sql)
                                high = max(high)[0]
                                #종가
                                sql = "SELECT close from sh{} ORDER by date DESC LIMIT 3".format(max_code) 
                                close = database(dbtype='sqlite3', load=True,db_name='buy_list', sql=sql)
                                close = max(close)[0]
                                ############################################################
                                if high == close :
                                    top_per = 1 # 윗꼬리 %
                                    body_per = (high - low) / low * 100
                                else:
                                    top_per = ((high-close) / (high-low))*100 # 윗꼬리 %
                                    body_per = (high - low) / low * 100
                                    
                                target_close = close
                                target_open = open
                                print(f'{max_code}의 바디%는 {body_per} / 위꼬리는 {top_per}%입니다.')
                                pbo_236 =high-((high-low)*0.236)
                                pbo_382 =high-((high-low)*0.382)
                                pbo_5 =high-((high-low)*0.5)
                                pbo_618 =high-((high-low)*0.618)
                                pbo_707 =high-((high-low)*0.707)
                                pbo_786 =high-((high-low)*0.786)
                                target_p = pbo_5 
                                # if int(top_per) < 5 :
                                #     target_p = pbo_236
                                # elif int(top_per) < 10:
                                #     target_p = pbo_382
                                # elif int(top_per) < 15:
                                #     target_p = pbo_5
                                # elif int(top_per) < 20:
                                #     target_p = pbo_618
                                # elif int(top_per) < 25:
                                #     target_p = pbo_707
                                # else :
                                #     target_p = pbo_786
                                
                                
                                ma3_directivity_1 = database(dbtype='sqlite3' ,  sql = f'SELECT ma_3 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma3_directivity_1[0][0] > ma3_directivity_1[1][0] and ma3_directivity_1[1][0] >= ma3_directivity_1[2][0]:
                                    ma3_directivity = 'True'
                                else:
                                    ma3_directivity = 'False'
                                    
                                ma5_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_5 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma5_directivity[0][0] > ma5_directivity[1][0] and ma5_directivity[1][0] >= ma5_directivity[2][0]:
                                    ma5_directivity = 'True'                                    
                                else:
                                    ma5_directivity = 'False'
                                
                                ma10_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_10 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma10_directivity[0][0] > ma10_directivity[1][0] and ma10_directivity[1][0] >= ma10_directivity[2][0]:
                                    ma10_directivity = 'True'                                    
                                else:
                                    ma10_directivity = 'False'
                                
                                ma20_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_20 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma20_directivity[0][0] > ma20_directivity[1][0] and ma20_directivity[1][0] >= ma20_directivity[2][0]:
                                    ma20_directivity = 'True'                                    
                                else:
                                    ma20_directivity = 'False'
                                    
                                ma60_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_60 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma60_directivity[0][0] > ma60_directivity[1][0] and ma60_directivity[1][0] >= ma60_directivity[2][0]:
                                    ma60_directivity = 'True'                                    
                                else:
                                    ma60_directivity = 'False'
                                
                                
                                sql_p = "insert or replace into Pbo (code,target_p,target_close,target_open,body_per,top_per,pbo_236,pbo_382,pbo_5,pbo_618,pbo_707,pbo_786,ma3_directivity,ma5_directivity,ma10_directivity,ma20_directivity,ma60_directivity) values ('{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {})".format(str(max_code),int(target_p),int(target_close),int(target_open),int(body_per),int(top_per),int(pbo_236),int(pbo_382),int(pbo_5),int(pbo_618),int(pbo_707),int(pbo_786),str(ma3_directivity),str(ma5_directivity),str(ma10_directivity),str(ma20_directivity),str(ma60_directivity))
                                database(dbtype='sqlite3', db_name='buy_list', sql=sql_p)
                                # database(dbtype='maria', host='myid9734.synology.me', user='myid9734',database='Pbo', password='Fdsa5017460*', port=3307, sql=sql_p) 
                                sql_p = "replace into Pbo (code,target_p,target_close,target_open,body_per,top_per,pbo_236,pbo_382,pbo_5,pbo_618,pbo_707,pbo_786, ma3_directivity, ma5_directivity, ma10_directivity, ma20_directivity, ma60_directivity) values ('{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {})".format(str(max_code),int(target_p),int(target_close),int(target_open),int(body_per),int(top_per),int(pbo_236),int(pbo_382),int(pbo_5),int(pbo_618),int(pbo_707),int(pbo_786), ma3_directivity, ma5_directivity, ma10_directivity, ma20_directivity, ma60_directivity)
                                database(dbtype='maria', database='Pbo', sql=sql_p) 
                                    
                                
                            else : # 양봉2개
                                #시가
                                sql = "SELECT open from sh{} ORDER by date DESC LIMIT 2".format(max_code) 
                                open = database(dbtype='sqlite3', load=True,db_name='buy_list', sql=sql)
                                open = min(open)[0]
                                
                                #저가
                                sql = "SELECT low from sh{} ORDER by date DESC LIMIT 2".format(max_code) 
                                low = database(dbtype='sqlite3', load=True,db_name='buy_list', sql=sql)
                                low = min(low)[0]
                                
                                #고가
                                sql = "SELECT high from sh{} ORDER by date DESC LIMIT 2".format(max_code) 
                                high = database(dbtype='sqlite3', load=True,db_name='buy_list', sql=sql)
                                high = max(high)[0]
                                
                                #종가
                                sql = "SELECT close from sh{} ORDER by date DESC LIMIT 2".format(max_code) 
                                close = database(dbtype='sqlite3', load=True,db_name='buy_list', sql=sql)
                                close = max(close)[0]
                                if high == close :
                                    top_per = 1 # 윗꼬리 %
                                    body_per = (high - low) / low * 100
                                else:
                                    top_per = ((high-close) / (high-low))*100 # 윗꼬리 %
                                    body_per = (high - low) / low * 100
                                    
                                target_close = close
                                target_open = open
                                print(f'{max_code}의 바디%는 {body_per} / 위꼬리는 {top_per}%입니다.')
                                pbo_236 =high-((high-low)*0.236)
                                pbo_382 =high-((high-low)*0.382)
                                pbo_5 =high-((high-low)*0.5)
                                pbo_618 =high-((high-low)*0.618)
                                pbo_707 =high-((high-low)*0.707)
                                pbo_786 =high-((high-low)*0.786)
                                target_p = pbo_5 
                                
                                
                                ma3_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_3 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma3_directivity[0][0] > ma3_directivity[1][0] and ma3_directivity[1][0] >= ma3_directivity[2][0]:
                                    ma3_directivity = 'True'
                                else:
                                    ma3_directivity = 'False'
                                    
                                ma5_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_5 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma5_directivity[0][0] > ma5_directivity[1][0] and ma5_directivity[1][0] >= ma5_directivity[2][0]:
                                    ma5_directivity = 'True'                                    
                                else:
                                    ma5_directivity = 'False'
                                
                                ma10_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_10 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma10_directivity[0][0] > ma10_directivity[1][0] and ma10_directivity[1][0] >= ma10_directivity[2][0]:
                                    ma10_directivity = 'True'                                    
                                else:
                                    ma10_directivity = 'False'
                                
                                ma20_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_20 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma20_directivity[0][0] > ma20_directivity[1][0] and ma20_directivity[1][0] >= ma20_directivity[2][0]:
                                    ma20_directivity = 'True'                                    
                                else:
                                    ma20_directivity = 'False'
                                    
                                ma60_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_60 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma60_directivity[0][0] > ma60_directivity[1][0] and ma60_directivity[1][0] >= ma60_directivity[2][0]:
                                    ma60_directivity = 'True'                                    
                                else:
                                    ma60_directivity = 'False'
                                
                                
                                sql_p = "insert or replace into Pbo (code,target_p,target_close,target_open,body_per,top_per,pbo_236,pbo_382,pbo_5,pbo_618,pbo_707,pbo_786,ma3_directivity,ma5_directivity,ma10_directivity,ma20_directivity,ma60_directivity) values ('{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {})".format(str(max_code),int(target_p),int(target_close),int(target_open),int(body_per),int(top_per),int(pbo_236),int(pbo_382),int(pbo_5),int(pbo_618),int(pbo_707),int(pbo_786),str(ma3_directivity),str(ma5_directivity),str(ma10_directivity),str(ma20_directivity),str(ma60_directivity))
                                database(dbtype='sqlite3', db_name='buy_list', sql=sql_p)
                                # database(dbtype='maria', host='myid9734.synology.me', user='myid9734',database='Pbo', password='Fdsa5017460*', port=3307, sql=sql_p) 
                                sql_p = "replace into Pbo (code,target_p,target_close,target_open,body_per,top_per,pbo_236,pbo_382,pbo_5,pbo_618,pbo_707,pbo_786,ma3_directivity,ma5_directivity,ma10_directivity,ma20_directivity,ma60_directivity) values ('{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {})".format(str(max_code),int(target_p),int(target_close),int(target_open),int(body_per),int(top_per),int(pbo_236),int(pbo_382),int(pbo_5),int(pbo_618),int(pbo_707),int(pbo_786),str(ma3_directivity),str(ma5_directivity),str(ma10_directivity),str(ma20_directivity),str(ma60_directivity))
                                database(dbtype='maria', database='Pbo', sql=sql_p) 
                                
                        else: # 당일양봉
                            #시가
                                sql = "SELECT open from sh{} ORDER by date DESC LIMIT 1".format(max_code) 
                                open = database(dbtype='sqlite3', load=True,db_name='buy_list', sql=sql)
                                open = min(open)[0]
                                
                                #저가
                                sql = "SELECT low from sh{} ORDER by date DESC LIMIT 1".format(max_code) 
                                low = database(dbtype='sqlite3', load=True,db_name='buy_list', sql=sql)
                                low = min(low)[0]
                                
                                #고가
                                sql = "SELECT high from sh{} ORDER by date DESC LIMIT 1".format(max_code) 
                                high = database(dbtype='sqlite3', load=True,db_name='buy_list', sql=sql)
                                high = max(high)[0]
                                
                                #종가
                                sql = "SELECT close from sh{} ORDER by date DESC LIMIT 1".format(max_code) 
                                conn = sqlite3.connect("DataBase/buy_list.db")
                                close = database(dbtype='sqlite3', load=True,db_name='buy_list', sql=sql)
                                close = max(close)[0]
                                if high == close :
                                    top_per = 1 # 윗꼬리 %
                                    body_per = (high - low) / low * 100
                                else:
                                    top_per = ((high-close) / (high-low))*100 # 윗꼬리 %
                                    body_per = (high - low) / low * 100
                                    
                                target_close = close
                                target_open = open
                                print(f'{max_code}의 바디%는 {body_per} / 위꼬리는 {top_per}%입니다.')
                                pbo_236 =high-((high-low)*0.236)
                                pbo_382 =high-((high-low)*0.382)
                                pbo_5 =high-((high-low)*0.5)
                                pbo_618 =high-((high-low)*0.618)
                                pbo_707 =high-((high-low)*0.707)
                                pbo_786 =high-((high-low)*0.786)
                                target_p = pbo_5 
                                
                                
                                ma3_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_3 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma3_directivity[0][0] > ma3_directivity[1][0] and ma3_directivity[1][0] >= ma3_directivity[2][0]:
                                    ma3_directivity = 'True'
                                else:
                                    ma3_directivity = 'False'
                                    
                                ma5_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_5 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma5_directivity[0][0] > ma5_directivity[1][0] and ma5_directivity[1][0] >= ma5_directivity[2][0]:
                                    ma5_directivity = 'True'                                    
                                else:
                                    ma5_directivity = 'False'
                                
                                ma10_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_10 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma10_directivity[0][0] > ma10_directivity[1][0] and ma10_directivity[1][0] >= ma10_directivity[2][0]:
                                    ma10_directivity = 'True'                                    
                                else:
                                    ma10_directivity = 'False'
                                
                                ma20_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_20 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma20_directivity[0][0] > ma20_directivity[1][0] and ma20_directivity[1][0] >= ma20_directivity[2][0]:
                                    ma20_directivity = 'True'                                    
                                else:
                                    ma20_directivity = 'False'
                                    
                                ma60_directivity = database(dbtype='sqlite3' ,  sql = f'SELECT ma_60 from sh{max_code} ORDER by date DESC LIMIT 3', load = True, db_name='buy_list')
                                if ma60_directivity[0][0] > ma60_directivity[1][0] and ma60_directivity[1][0] >= ma60_directivity[2][0]:
                                    ma60_directivity = 'True'                                    
                                else:
                                    ma60_directivity = 'False'
                                
                                
                                sql_p = "insert or replace into Pbo (code,target_p,target_close,target_open,body_per,top_per,pbo_236,pbo_382,pbo_5,pbo_618,pbo_707,pbo_786,ma3_directivity,ma5_directivity,ma10_directivity,ma20_directivity,ma60_directivity) values ('{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {})".format(str(max_code),int(target_p),int(target_close),int(target_open),int(body_per),int(top_per),int(pbo_236),int(pbo_382),int(pbo_5),int(pbo_618),int(pbo_707),int(pbo_786),str(ma3_directivity),str(ma5_directivity),str(ma10_directivity),str(ma20_directivity),str(ma60_directivity))
                                database(dbtype='sqlite3', db_name='buy_list', sql=sql_p)
                                # database(dbtype='maria', host='myid9734.synology.me', user='myid9734',database='Pbo', password='Fdsa5017460*', port=3307, sql=sql_p) 
                                sql_p = "replace into Pbo (code,target_p,target_close,target_open,body_per,top_per,pbo_236,pbo_382,pbo_5,pbo_618,pbo_707,pbo_786,ma3_directivity,ma5_directivity,ma10_directivity,ma20_directivity,ma60_directivity) values ('{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {})".format(str(max_code),int(target_p),int(target_close),int(target_open),int(body_per),int(top_per),int(pbo_236),int(pbo_382),int(pbo_5),int(pbo_618),int(pbo_707),int(pbo_786),str(ma3_directivity),str(ma5_directivity),str(ma10_directivity),str(ma20_directivity),str(ma60_directivity))
                                database(dbtype='maria', database='Pbo', sql=sql_p) 
                                
        except Exception as e:
            logging.exception("Exception occurred", exc_info=True)

def algo_1():
    try:    
        for max in codes: # Sqlite3로 DB저장
            max_code  = max
            # 종가 불러오기
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            c.execute(f"select date, close from sh{max} ORDER BY `date` DESC limit 1")
            conn.commit()
            close = c.fetchone()
            close = close[1]

            # print(close, 'close', max)
            conn.close()
            sql = "select date, high from sh{} where open < close and (high-low)*100/close > 1.0 and `close` > {} ORDER BY `date` DESC limit 240".format(max, close)
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            result = pd.read_sql_query(sql,conn)    
            result_index = result['high'].sort_index() # 최고값 정렬  
            conn.close()

            list_list =[]
            last = []     
            count = 0
            before = 0
            for max_1 in result_index:
                # print(max_1)
                if int(max_1) > int(close) and int(before) < int(max_1) and int(count) <= 5 and max_1 not in list_list:
                    count +=1
                    before = max_1
                    list_list.append(max_1)

            code_name = kiwoom.GetMasterCodeName(max_code) # 한글 종목명 가져오기
            ma3 = ma(max_code, 3)
            ma5 = ma(max_code, 5)
            ma10 = ma(max_code, 10)
            ma20 = ma(max_code, 20)
            ma60 = ma(max_code, 60)
            if len(list_list) == 6 :
                sql1 = 'replace into list_buy (code, name, last0, last1, last2, last3, last4, last5, close, ma_3, ma_5, ma_10, ma_20, ma_60) values ("{}", "{}", {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})'.format(str(max_code), code_name, list_list[0], list_list[1], list_list[2], list_list[3], list_list[4], list_list[5], close, ma3, ma5, ma10, ma20, ma60)
                # print(max_code,'--------------------')
                print(str(max_code), code_name, list_list[0], list_list[1], list_list[2], list_list[3], list_list[4], list_list[5], close, ma3, ma5, ma10, ma20, ma60)
                # exit()
            elif len(list_list) == 5 :
                sql1 = 'replace into list_buy (code, name, last0, last1, last2, last3, last4, close, ma_3, ma_5, ma_10, ma_20, ma_60) values ("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", {}, {}, {}, {}, {})'.format(max_code, code_name, list_list[0], list_list[1], list_list[2], list_list[3], list_list[4], close, ma3, ma5, ma10, ma20, ma60)
                print(str(max_code), code_name, list_list[0], list_list[1], list_list[2], list_list[3], list_list[4], close)
            elif len(list_list) == 4 :
                sql1 = 'replace into list_buy (code, name, last0, last1, last2, last3, close, ma_3, ma_5, ma_10, ma_20, ma_60) values ("{}", "{}", "{}", "{}", "{}", "{}", "{}", {}, {}, {}, {}, {})'.format(max_code, code_name, list_list[0], list_list[1], list_list[2], list_list[3], close, ma3, ma5, ma10, ma20, ma60)
                print(str(max_code), code_name, list_list[0], list_list[1], list_list[2], list_list[3], close)
            elif len(list_list) == 3 :
                sql1 = 'replace into list_buy (code, name, last0, last1, last2, close, ma_3, ma_5, ma_10, ma_20, ma_60) values ("{}", "{}", "{}", "{}", "{}", "{}", {}, {}, {}, {}, {})'.format(max_code, code_name, list_list[0], list_list[1], list_list[2], close, ma3, ma5, ma10, ma20, ma60)
                print(str(max_code), code_name, list_list[0], list_list[1], list_list[2], close)
            elif len(list_list) == 2 :
                sql1 = 'replace into list_buy (code, name, last0, last1, close, ma_3, ma_5, ma_10, ma_20, ma_60) values ("{}", "{}", "{}", "{}", "{}", {}, {}, {}, {}, {})'.format(max_code, code_name, list_list[0], list_list[1], close, ma3, ma5, ma10, ma20, ma60)
                print(str(max_code), code_name, list_list[0], list_list[1], close)
            elif len(list_list) == 1 :
                sql1 = 'replace into list_buy (code, name, last0, close, ma_3, ma_5, ma_10, ma_20, ma_60) values ("{}", "{}", "{}", "{}", {}, {}, {}, {}, {})'.format(max_code, code_name, list_list[0], close, ma3, ma5, ma10, ma20, ma60)
                print(str(max_code), code_name, list_list[0], close)
            else:
                pass
            try:
                last_db = sqlite3.connect('DataBase/buy_list.db')
                c = last_db.cursor()
                c.execute(sql1)
                last_db.commit()
                last_db.close()

                conn = pymysql.connect(host='myid9734.synology.me', user='myid9734',database='list_buy', password='Fdsa5017460*', port=3307) 
                cursor = conn.cursor() 
                cursor.execute(sql1) 
                conn.commit() 
                conn.close()
            except Exception as e:
                logging.exception("Exception occurred", exc_info=True)
    
    except Exception as e:
        logging.exception("Exception occurred", exc_info=True)
        
        
def algo_2():
    try:    
        for max in codes: # Sqlite3로 DB저장
            max_code  = max
            # 종가 불러오기
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            c.execute(f"select date, close from sh{max} ORDER BY `date` DESC limit 1")
            conn.commit()
            close = c.fetchone()
            close = close[1]

            # print(close, 'close', max)
            conn.close()
            sql = "select date, high from sh{} where open < close and (high-low)*100/close > 5.0 ORDER BY `date` DESC limit 240".format(max)
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            result = pd.read_sql_query(sql,conn)    
            result_index = result['high'].sort_index() # 최고값 정렬  
            conn.close()

            list_list =[]
            last = []     
            count = 0
            before = 0
            for max_1 in result_index:
                # print(max_1)
                if int(max_1) > int(close) and int(before) < int(max_1) and int(count) <= 5 and max_1 not in list_list:
                    count +=1
                    before = max_1
                    list_list.append(max_1)

            code_name = kiwoom.GetMasterCodeName(max_code) # 한글 종목명 가져오기
            ma3 = ma(max_code, 3)
            ma5 = ma(max_code, 5)
            ma10 = ma(max_code, 10)
            ma20 = ma(max_code, 20)                                                             
            ma60 = ma(max_code, 60)
            if len(list_list) == 6 :
                sql1 = 'replace into list_buy (code, name, last0, last1, last2, last3, last4, last5, close, ma_3, ma_5, ma_10, ma_20, ma_60) values ("{}", "{}", {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})'.format(str(max_code), code_name, list_list[0], list_list[1], list_list[2], list_list[3], list_list[4], list_list[5], close, ma3, ma5, ma10, ma20, ma60)
                # print(max_code,'--------------------')
                print(str(max_code), code_name, list_list[0], list_list[1], list_list[2], list_list[3], list_list[4], list_list[5], close, ma3, ma5, ma10, ma20, ma60)
                # exit()
            elif len(list_list) == 5 :
                sql1 = 'replace into list_buy (code, name, last0, last1, last2, last3, last4, close, ma_3, ma_5, ma_10, ma_20, ma_60) values ("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", {}, {}, {}, {}, {})'.format(max_code, code_name, list_list[0], list_list[1], list_list[2], list_list[3], list_list[4], close, ma3, ma5, ma10, ma20, ma60)
                print(str(max_code), code_name, list_list[0], list_list[1], list_list[2], list_list[3], list_list[4], close)
            elif len(list_list) == 4 :
                sql1 = 'replace into list_buy (code, name, last0, last1, last2, last3, close, ma_3, ma_5, ma_10, ma_20, ma_60) values ("{}", "{}", "{}", "{}", "{}", "{}", "{}", {}, {}, {}, {}, {})'.format(max_code, code_name, list_list[0], list_list[1], list_list[2], list_list[3], close, ma3, ma5, ma10, ma20, ma60)
                print(str(max_code), code_name, list_list[0], list_list[1], list_list[2], list_list[3], close)
            elif len(list_list) == 3 :
                sql1 = 'replace into list_buy (code, name, last0, last1, last2, close, ma_3, ma_5, ma_10, ma_20, ma_60) values ("{}", "{}", "{}", "{}", "{}", "{}", {}, {}, {}, {}, {})'.format(max_code, code_name, list_list[0], list_list[1], list_list[2], close, ma3, ma5, ma10, ma20, ma60)
                print(str(max_code), code_name, list_list[0], list_list[1], list_list[2], close)
            elif len(list_list) == 2 :
                sql1 = 'replace into list_buy (code, name, last0, last1, close, ma_3, ma_5, ma_10, ma_20, ma_60) values ("{}", "{}", "{}", "{}", "{}", {}, {}, {}, {}, {})'.format(max_code, code_name, list_list[0], list_list[1], close, ma3, ma5, ma10, ma20, ma60)
                print(str(max_code), code_name, list_list[0], list_list[1], close)
            elif len(list_list) == 1 :
                sql1 = 'replace into list_buy (code, name, last0, close, ma_3, ma_5, ma_10, ma_20, ma_60) values ("{}", "{}", "{}", "{}", {}, {}, {}, {}, {})'.format(max_code, code_name, list_list[0], close, ma3, ma5, ma10, ma20, ma60)
                print(str(max_code), code_name, list_list[0], close)
            else:
                pass
            try:
                last_db = sqlite3.connect('DataBase/buy_list.db')
                c = last_db.cursor()
                c.execute(sql1)
                last_db.commit()
                last_db.close()

                conn = pymysql.connect(host='myid9734.synology.me', user='myid9734',database='list_buy', password='Fdsa5017460*', port=3307) 
                cursor = conn.cursor() 
                cursor.execute(sql1) 
                conn.commit() 
                conn.close()
            except Exception as e:
                logging.exception("Exception occurred", exc_info=True)
    
    except Exception as e:
        logging.exception("Exception occurred", exc_info=True)        

def algo_4():
    try:    
        for max in codes: # Sqlite3로 DB저장
            print(f'{max} 알고리즘 4 계산중')
            max_code  = max
            # 종가 불러오기
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            sql = "select date, open, close, vol_pirce from sh{} ORDER BY `date` DESC limit 1".format(max)
            c.execute(sql)
            conn.commit()
            close = c.fetchone()
            
            # where open < close and vol_pirce <5000
            # print(close, 'close', max)
            open = close[1]
            close_p = close[2]
            vol = int(close[3])
            
            if (open < close_p) and (vol > 5000):
                print(open, close_p, vol, max_code)
                code_name = kiwoom.GetMasterCodeName(max_code) # 한글 종목명 가져오기
            
                sql1 = 'replace into list_buy (code, name) values ("{}", "{}")'.format(max_code, code_name)
            try:
                last_db = sqlite3.connect('DataBase/buy_list.db')
                c = last_db.cursor()
                c.execute(sql1)
                last_db.commit()
                last_db.close()

                conn = pymysql.connect(host='myid9734.synology.me', user='myid9734',database='list_buy', password='Fdsa5017460*', port=3307) 
                cursor = conn.cursor() 
                cursor.execute(sql1) 
                conn.commit() 
                conn.close()
            except Exception as e:
                logging.exception("Exception occurred", exc_info=True)
    
    except Exception as e:
        logging.exception("Exception occurred", exc_info=True)                
        

def ma(code_no, ma_num):
    sql = f"SELECT close from sh{str(code_no)} ORDER by date DESC LIMIT {ma_num}"
    conn = sqlite3.connect("DataBase/buy_list.db")
    c = conn.cursor()
    c.execute(sql)
    sum = c.fetchall()
    result_ma = 0
    global ma_data
    ma_data=0
    for i in range(len(sum)):
        sum_ma = sum[i][0]
        sum_ma = int(sum_ma)
        ma_data = ma_data + sum_ma
    result_ma = int(ma_data / ma_num)
    return result_ma

def collector(): # 종목 데이터 콜렉팅
    count_codes = 0
    conn = sqlite3.connect("DataBase/buy_list.db")
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS code ("code"	TEXT)')
    conn.commit()
    now = time.localtime()
    nowDate = '%04d%02d%02d' % (now.tm_year,now.tm_mon,now.tm_mday)
    for code in codes:
        update =[]
        conn = sqlite3.connect("DataBase/buy_list.db")
        c = conn.cursor()
        c.execute(f"select code from code")
        conn.commit()
        up = c.fetchall()
        for a in up:
            update.append(a[0])
            
        # if code not in update:
        #     try:
        #         time.sleep(1)
        #         conn = sqlite3.connect("DataBase/buy_list.db")
        #         sql = 'ALTER TABLE sh{} ADD COLUMN ma_240 INTEGER'.format(code)
        #         print(f'{code} 테이블 생성.....{count_codes}/{len(codes)}')
        #         c = conn.cursor()
        #         c.execute(sql)
        #         conn.commit()
        #         conn.close()
        #     except:
        #         pass

        # 업데이트한 코드 종목 제외 코드  추가할것..------------------------------------------
        if code not in update:
            time.sleep(1)
            conn = sqlite3.connect("DataBase/buy_list.db")
            sql = 'CREATE TABLE IF NOT EXISTS sh{} ("date"	TEXT, "open" INTEGER, "close" INTEGER, "high" INTEGER, "low" INTEGER, "vol" INTEGER, "vol_pirce" INTEGER, "d1_diff_rate" REAL, "ma_3" INTEGER,  "ma_5" INTEGER, "ma_10" INTEGER, "ma_20" INTEGER,"ma_60" INTEGER, "ma_240" INTEGER, PRIMARY KEY("date"))'.format(code)
            print(f'{code} 테이블 생성.....{count_codes}/{len(codes)}')
            c = conn.cursor()
            c.execute(sql)
            conn.commit()
            conn.close()
    # 
        # # 업데이트한 종목인지 체크

        if code not in update:
            df = kiwoom.block_request('opt10086', 종목코드=code, 기준일자=nowDate, 표시구분=1, output='pp', next=0)
            # print(df)
            df2 = pd.DataFrame(df, columns=['날짜', '시가', '종가', '고가', '저가', '거래량', '금액(백만)', '등락률'])
            # print(df2)

            for i in range(0, len(df2)):
                try:
                    print(df2['날짜'][i], df2['시가'][i], df2['종가'][i], df2['고가'][i], df2['저가'][i], df2['거래량'][i], df2['등락률'][i], '------------------- 코드:', code)
                    sql = "insert or replace into sh{} (date, open, close, high, low, vol, vol_pirce, d1_diff_rate) values ({}, {}, {},  {}, {}, {}, {}, {})".format(str(code), str(df2['날짜'][i]), abs(int(df2['시가'][i])), abs(int(df2['종가'][i])), abs(int(df2['고가'][i])), abs(int(df2['저가'][i])), df2['거래량'][i], df2['금액(백만)'][i], df2['등락률'][i])
                    # print(sql)
                    conn = sqlite3.connect("DataBase/buy_list.db")
                    c = conn.cursor()
                    c.execute(sql)
                    conn.commit()
                    conn.close()
                    time.sleep(0.1)
                except Exception as e:
                    logging.exception("Exception occurred", exc_info=True)

            # 업데이트 완료 종목 추가
        sql = 'insert or replace into code (`code`) values ("{}")'.format(code)
        conn = sqlite3.connect ('DataBase/buy_list.db')
        c = conn.cursor()
        c.execute(sql)
        conn.commit()
        conn.close()
        count_codes +=1 
        os.system('cls')





def info(): # 환경정보 읽어오기
    setup = Config("../ini/collector.ini")
    print(setup.keys())
    host = setup['db_info']['host']    # 실투 비밀번호
    # user = setup['db_info']['user']
    # database = setup['db_info']['database']
    # password = setup['db_info']['password']
    # port = setup['db_info']['port']
    

def 로그인():
    global kiwoom
    kiwoom = Kiwoom(login=True)
    global codes
    codes = kiwoom.GetCodeListByMarket(0) + kiwoom.GetCodeListByMarket(10) # 코스피 / 코스닥 코드리스트 가져오기
    
    # for i in codes:
    #     database(dbtype='sqlite3', load=False, sql='insert or replace into code (`code`) values ("{}")'.format(i),db_name = 'buy_list')

def send_msg(msg=''): # 라인 메세지 전송
        response = requests.post(
        'https://notify-api.line.me/api/notify',
        headers={
            'Authorization' : 'Bearer rs5WOIz2jZQ3NGOBZqN1cbm6WHmnqMCt2K4ydqxFkco'
        },
        data={
            'message': msg
            }
        )

def database(dbtype='sqlite3', load=False, sql=None, host='myid9734.synology.me', user='myid9734', db_name=None, database='list_buy', password='Fdsa5017460*', port=3307):
    """host='myid9734.synology.me', user='myid9734',database='list_buy', password='Fdsa5017460*', port=3307) """
    if dbtype == 'sqlite3':
        conn = sqlite3.connect(f"DataBase/{db_name}.db")
        c = conn.cursor()
        c.execute(sql)
        if load == True:
            data = c.fetchall()
            return data
        else:
            conn.commit()
            conn.close()
    elif dbtype == 'maria' :
        conn = pymysql.connect(host=host, user=user, database=database, password=password , port=port) 
        cursor = conn.cursor() 
        cursor.execute(sql) 
        if load == True:
            data = cursor.fetchall()
            return data
        else:
            conn.commit() 
            conn.close()

def get_code():
    codes = kiwoom.GetCodeListByMarket(0) + kiwoom.GetCodeListByMarket(10)
    for i in codes:
        print(codes)

def update_ok():
    database(
        dbtype='sqlite3',
        sql = 'DELETE FROM code',
        db_name= 'buy_list')
    
def order_money():
    # 주문가능금액
    account_list = kiwoom.GetLoginInfo('ACCNO')
    account = account_list[0]
    df = kiwoom.block_request('opw00001', 계좌번호=account, 비밀번호입력매체구분='00', 조회구분=1, output='예수금상세현황', next=0)
    주문가능금액 = int(df['주문가능금액'][0])
    database(
        dbtype='sqlite3',
        sql = 'update Info set total_money={}'.format(주문가능금액),
        db_name= 'Guardian Angel')
    try:
        database(
            dbtype = 'maria',
            sql = 'update Info set total_money={}'.format(주문가능금액),
            database = 'Guardian Angel')
    except Exception as e:
        logging.exception("Exception occurred", exc_info=True)
    
    
def update_boyou():
    # 보유종목 수량 업데이트
    account_list = kiwoom.GetLoginInfo('ACCNO')
    account = account_list[0]
    df = kiwoom.block_request('opw00004', 계좌번호=account, 비밀번호='6215', 상장폐지조회구분=0, 비밀번호입력매체구분='00', output='종목코드', next=0)
    conn = sqlite3.connect("DataBase/Guardian Angel.db")
    c = conn.cursor()
    c.execute('update Info set bo_you={}'.format(len(df)))
    conn.commit()
    
    df = kiwoom.block_request('opw00004', 계좌번호=account, 비밀번호='6215', 상장폐지조회구분=0, 비밀번호입력매체구분='00', output='종목코드', next=0)
    sql = f'CREATE TABLE IF NOT EXISTS "boyou" ("code" TEXT DEFAULT "005930", "price_avg"	INTEGER DEFAULT 0, "qua" INTEGER DEFAULT 0, PRIMARY KEY("code"))'

    # 보유종목 코드 / 단가 / 수량 입력을 위한 데이테 베이스 생성
    database(
        dbtype='sqlite3',
        sql = sql,
        db_name= 'buy_list')
    try:
        database(
            dbtype='maria',
            sql = sql,
            database = 'boyou')
        
    except Exception as e:
            logging.exception("Exception occurred", exc_info=True)
    
    # 보유종목 코드 / 단가 / 수량 입력을 위한 데이테 베이스 저장
    try:
        for i in range(len(df)):
            sql = "replace into boyou (code, price_avg, qua) values ('{}', '{}', {})".format(str(df['종목코드'][i][1:]), int(df['평균단가'][i]), int(df['보유수량'][i]))
            try:
                database(
                    dbtype='sqlite3',
                    sql = sql,
                    load = False,
                    db_name= 'buy_list')
                database(
                    dbtype = 'maria',
                    sql = sql,
                    load = False,
                    database = 'boyou')
            except Exception as e:
                logging.exception("Exception occurred", exc_info=True)
    except:
        pass


    보유종목 = []
    for i in range(len(df)):
        보유종목.append(df['종목코드'][i][1:])
    database(
        dbtype = 'sqlite3',
        sql = 'update Info set boyou_code="{}"'.format(보유종목),
        db_name= 'Guardian Angel')
    try:
        database(
        dbtype = 'maria',
        sql = 'update Info set boyou_code="{}"'.format(보유종목),
        database = 'Guardian Angel')
    except Exception as e:
        logging.exception("Exception occurred", exc_info=True)
    
def ma_update(): # 이평선 추가
    
    # codes_list = ['000020'] #, '000040', '005930']
    for i in codes:
        # try :
        #     sql = f"ALTER TABLE sh{i} ADD 'ma_60' INTEGER" # , 'ma_10' INTEGER, 'ma_20' INTEGER, 'ma_60' INTEGER"
        #     database(dbtype='sqlite3', sql=sql, db_name='buy_list')
        #     print(f'{i} 생성완료')
        # except Exception as e:
        #     logging.exception("Exception occurred", exc_info=True)
        try:
            sql = f"INSERT OR replace INTO  'sh{i}' \
                (date, open, close, high, low, vol, vol_pirce, d1_diff_rate, ma_3) \
                    SELECT date, open, close, high, low, vol, vol_pirce, d1_diff_rate, AVG(close) OVER\
                        (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_average FROM 'sh{i}' WHERE date=date"
            database(dbtype='sqlite3', sql=sql, db_name='buy_list')
            
            sql = f"INSERT OR replace INTO  'sh{i}' \
                (date, open, close, high, low, vol, vol_pirce, d1_diff_rate, ma_3, ma_5) \
                    SELECT date, open, close, high, low, vol, vol_pirce, d1_diff_rate, ma_3, AVG(close) OVER\
                        (ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moving_average FROM 'sh{i}' WHERE date=date"
            database(dbtype='sqlite3', sql=sql, db_name='buy_list')
            
            sql = f"INSERT OR replace INTO  'sh{i}' \
                (date, open, close, high, low, vol, vol_pirce, d1_diff_rate, ma_3, ma_5, ma_10) \
                    SELECT date, open, close, high, low, vol, vol_pirce, d1_diff_rate, ma_3, ma_5, AVG(close) OVER\
                        (ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS moving_average FROM 'sh{i}' WHERE date=date"
            database(dbtype='sqlite3', sql=sql, db_name='buy_list')
            
            sql = f"INSERT OR replace INTO  'sh{i}' \
                (date, open, close, high, low, vol, vol_pirce, d1_diff_rate, ma_3, ma_5, ma_10, ma_20) \
                    SELECT date, open, close, high, low, vol, vol_pirce, d1_diff_rate, ma_3, ma_5, ma_10, AVG(close) OVER\
                        (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS moving_average FROM 'sh{i}' WHERE date=date"
            database(dbtype='sqlite3', sql=sql, db_name='buy_list')
            
            sql = f"INSERT OR replace INTO  'sh{i}' \
                (date, open, close, high, low, vol, vol_pirce, d1_diff_rate, ma_3, ma_5, ma_10, ma_20, ma_60) \
                    SELECT date, open, close, high, low, vol, vol_pirce, d1_diff_rate, ma_3, ma_5, ma_10, ma_20, AVG(close) OVER\
                        (ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS moving_average FROM 'sh{i}' WHERE date=date"
            database(dbtype='sqlite3', sql=sql, db_name='buy_list')

            sql = f"INSERT OR replace INTO  'sh{i}' \
                (date, open, close, high, low, vol, vol_pirce, d1_diff_rate, ma_3, ma_5, ma_10, ma_20, ma_60, ma_240) \
                    SELECT date, open, close, high, low, vol, vol_pirce, d1_diff_rate, ma_3, ma_5, ma_10, ma_20, ma_60, AVG(close) OVER\
                        (ORDER BY date ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS moving_average FROM 'sh{i}' WHERE date=date"
            database(dbtype='sqlite3', sql=sql, db_name='buy_list')
        except Exception as e:
            logging.exception("Exception occurred", exc_info=True)

def stop_buy(): # 거래량으로 거래정지 종목 DB 삭제
    for i in codes:
        try:
            sql = f"select vol from sh{i} ORDER BY `date` DESC limit 3"
            close = database(dbtype='sqlite3',
                            load=True,
                            sql = sql,
                            db_name = 'buy_list')
            
            if close[0][0] != 0:
                pass
                # print('거래중지 검색중....')
            else:
                # try:
                    print('거재중지 종목', i)
                    sql = f"DELETE from Pbo WHERE code='{i}'"    
                    database(
                        dbtype='sqlite3',
                        sql=sql,
                        db_name='buy_list')

                    database(
                        dbtype='sqlite3',
                        sql=f"DELETE from list_buy WHERE code='{i}'",
                        db_name='buy_list')
                    try:
                        database(
                        dbtype='maria',
                        sql=sql,
                        database = 'buy_list')

                        database(
                        dbtype='maria',
                        sql=f"DELETE from list_buy WHERE code='{i}'",
                        database = 'buy_list')

                    except Exception as e:
                        logging.exception("Exception occurred", exc_info=True)
                # except:
                    # pass
        except Exception as e:
            logging.exception("Exception occurred", exc_info=True)
    
if __name__ == '__main__':
    now = time.localtime()
    now_1 = '%04d%02d%02d' % (now.tm_year,now.tm_mon,now.tm_mday)
    
    """ 데이터 베이스 삭제 - sqlite3 알고리즘1 """
    # 알고1 데이터 불러와서 DataFrame 으로 변환
    # cnx = sqlite3.connect('DataBase/buy_list.db')
    # df = pd.read_sql_query("SELECT * FROM list_buy", cnx)
    # df.to_excel(f'excel/buy_list/{now_1}.xlsx', index=False)

    
    # cnx = sqlite3.connect('DataBase/buy_list.db')
    # df = pd.read_sql_query("SELECT * FROM Pbo", cnx)
    # df.to_excel(f'excel/Pbo/{now_1}.xlsx', index=False)
    
    database(dbtype='sqlite3', load = False, sql = 'DELETE FROM list_buy', db_name = 'buy_list', database='list_buy')
    # database(dbtype='maria', load = False, sql = 'DELETE FROM list_buy', db_name = 'buy_list', database='list_buy')

    database(dbtype='sqlite3', load = False, sql = 'DELETE FROM Pbo', db_name = 'buy_list', database='list_buy')
    # database(dbtype='maria', load = False, sql = 'DELETE FROM list_buy', db_name = 'buy_list', database='list_buy')


    database(dbtype='sqlite3', load = False, sql = 'DELETE FROM boyou', db_name = 'buy_list', database='list_buy')
    # database(dbtype='maria', load = False, sql = 'DELETE FROM boyou', database='boyou')
    
    
    로그인()
    
    send_msg(f'로그인 완료')

    order_money() # 주문가능금액 업데이트
    update_boyou()
    collector() # 종목 정보 수집
    send_msg(f'종목 정보 수집 완료')
    
    # algo2_Fibonacci()
    # # send_msg(f'피보나치 데이터 가공 완료')
    
    update_ok() # 업데이트 완료후 데이터베이스 코드 삭제
    # send_msg(f'업데이트 완료')
    
    ma_update() # 이동평균 계산
    # send_msg(f'이동편균 계산 완료')
    
    
    # algo_2()
    # send_msg(f'돌파매매 데이터 가공 완료')

    algo_4()
    send_msg(f'거래대금 데이터 가공 완료')

    stop_buy()
    # database(dbtype='sqlite3', load = False, sql = 'DELETE FROM code', db_name = 'buy_list', database='list_buy')
    send_msg(f'업데이트 완료')
    # send_msg(f'최종 업데이트 완료')

    send_msg(f'----------------------------------------------------')