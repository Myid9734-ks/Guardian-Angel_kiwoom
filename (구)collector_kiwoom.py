from pykiwoom.kiwoom import *
import os
import pandas as pd
import sqlite3
import requests
import time
import pymysql







now = time.localtime()
now_1 = '%04d%02d%02d' % (now.tm_year,now.tm_mon,now.tm_mday)
conn = sqlite3.connect("DataBase/Guardian Angel.db")
c = conn.cursor()
c.execute('select update_date from Info')
now_date = c.fetchone()[0]
nowDate = now_date

def send_msg(msg=''):
        response = requests.post(
        'https://notify-api.line.me/api/notify',
        headers={
            'Authorization' : 'Bearer t1MMPiNW0mmfoC4JKxNnHeOl7PDrdgiaArLaOwM6oNx'
        },
        data={
            'message': msg
            }
        )

if now_date != now_1:
    피보카운터 = 0
    count_codes = 0
    kiwoom = Kiwoom(login=True)

    codes = kiwoom.GetCodeListByMarket(0) + kiwoom.GetCodeListByMarket(10) # 코스피 / 코스닥 코드리스트 가져오기
    print(len(codes))

    conn = sqlite3.connect("DataBase/buy_list.db")
    sql = 'DELETE FROM list_buy'
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    conn.close()

    conn = sqlite3.connect("DataBase/buy_list.db")
    sql = 'DELETE FROM Pbo'
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    conn.close()



    conn = pymysql.connect(host='myid9734.synology.me', user='myid9734',database='list_buy', password='As5017460*', port=3307) 
    sql = 'DELETE FROM list_buy'
    cursor = conn.cursor() 
    cursor.execute(sql) 
    conn.commit() 
    conn.close()

    send_msg(f'종목 데이터 수집 / 처리 시작')
    #################################################################################################################################################################
    #종목별 데이터 콜렉팅
    for code in codes:
        update =[]
        conn = sqlite3.connect("DataBase/all.db")
        c = conn.cursor()
        c.execute(f"select code from code")
        conn.commit()
        up = c.fetchall()
        for a in up:
            update.append(a[0])

        # 업데이트한 코드 종목 제외 코드  추가할것..------------------------------------------
        if code not in update:
            time.sleep(1)
            conn = sqlite3.connect("DataBase/buy_list.db")
            sql = 'CREATE TABLE IF NOT EXISTS sh{} ("date"	TEXT, "open" INTEGER, "close" INTEGER, "high" INTEGER, "low" INTEGER, "vol" INTEGER, "vol_pirce" INTEGER, "d1_diff_rate" REAL, PRIMARY KEY("date"))'.format(code)
            print(f'{code} 테이블 생성.....{count_codes}/{len(codes)}')
            c = conn.cursor()
            c.execute(sql)
            conn.commit()
            conn.close()

        # 업데이트한 종목인지 체크

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
                except:
                    pass

            # 업데이트 완료 종목 추가
        sql = 'insert or replace into code (`code`) values ("{}")'.format(code)
        conn = sqlite3.connect ('DataBase/all.db')
        c = conn.cursor()
        c.execute(sql)
        conn.commit()
        conn.close()
        count_codes +=1 
    #################################################################################################################################################################

    # # 콜링팅 데이터 가동 ---------> 최근 고가 데이터 추출

    error_count = 0
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
            sql = "select date, high from sh{} where open < close and (high-low)*100/close > 5.0 and `close` > {} ORDER BY `date` DESC limit 240".format(max, close)
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

            try:
                print(i[0], i[1], list_list[0], list_list[1], list_list[2], list_list[3], list_list[4], list_list[5], close)
            except:
                pass
            
            code_name = kiwoom.GetMasterCodeName(max_code)

            sql = f"SELECT close from sh{str(max_code)} ORDER by date DESC LIMIT 3"
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            c.execute(sql)
            sum = c.fetchall()
            ma3 = 0
            for i in range(len(sum)):
                sum_ma = sum[i][0]
                sum_ma = int(sum_ma)
                ma3 = ma3 + sum_ma
            ma3 = int(ma3 / 3)

            sql = f"SELECT close from sh{str(max_code)} ORDER by date DESC LIMIT 5"
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            c.execute(sql)
            sum = c.fetchall()
            ma5 = 0
            for i in range(len(sum)):
                sum_ma = sum[i][0]
                sum_ma = int(sum_ma)
                ma5 = ma5 + sum_ma
            ma5 = int(ma5 / 5)

            sql = f"SELECT close from sh{str(max_code)} ORDER by date DESC LIMIT 10"
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            c.execute(sql)
            sum = c.fetchall()
            ma10 = 0
            for i in range(len(sum)):
                sum_ma = sum[i][0]
                sum_ma = int(sum_ma)
                ma10 = ma10 + sum_ma
            ma10 = int(ma10 / 10)

            sql = f"SELECT close from sh{str(max_code)} ORDER by date DESC LIMIT 20"
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            c.execute(sql)
            sum = c.fetchall()
            ma20 = 0
            for i in range(len(sum)):
                sum_ma = sum[i][0]
                sum_ma = int(sum_ma)
                ma20 = ma20 + sum_ma
            ma20 = int(ma20 / 20)

            sql = f"SELECT close from sh{str(max_code)} ORDER by date DESC LIMIT 60"
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            c.execute(sql)
            sum = c.fetchall()
            ma60 = 0
            for i in range(len(sum)):
                sum_ma = sum[i][0]
                sum_ma = int(sum_ma)
                ma60 = ma60 + sum_ma
            ma60 = int(ma60 / 60)


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

                conn = pymysql.connect(host='myid9734.synology.me', user='myid9734',database='list_buy', password='As5017460*', port=3307) 
                cursor = conn.cursor() 
                cursor.execute(sql1) 
                conn.commit() 
                conn.close()
            except:        
                error_count +=1
                
    # 피보나치
            print(f'피보 -------> {피보카운터} / {max_code}')
            sql = "SELECT open, close, high, low from sh{} ORDER by date DESC LIMIT 1".format(max_code) 
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            c.execute(sql)
            sum = c.fetchall()
            conn.close()
            피보카운터 += 1
            for i in range(len(sum)):
                body_per = (sum[0][2]-sum[0][3])/sum[0][3]*100 # 바디 %
                if sum[0][0] < sum[0][1]: # 양봉일때만
                    if sum[0][2] == sum[0][1]: # 고가와 종가가 같을때
                        top_per = 1 # 윗꼬리 %
                        
                    else:
                        top_per = ((sum[0][2]-sum[0][1]) / (sum[0][2]-sum[0][3]))*100 # 윗꼬리 %
                    print(f'{max_code}의 body_persms{body_per} / top_per는 {top_per}')
                    
                    if body_per > 6.0 and sum[0][0] < sum[0][1]:
                        target_close = sum[i][1]
                        target_open = sum[i][0]
                        pbo_236 =sum[0][2]-((sum[0][2]-sum[0][3])*0.236)
                        pbo_382 =sum[0][2]-((sum[0][2]-sum[0][3])*0.382)
                        pbo_5 =sum[0][2]-((sum[0][2]-sum[0][3])*0.5)
                        pbo_618 =sum[0][2]-((sum[0][2]-sum[0][3])*0.618)
                        pbo_707 =sum[0][2]-((sum[0][2]-sum[0][3])*0.707)
                        pbo_786 =sum[0][2]-((sum[0][2]-sum[0][3])*0.786)
                        if int(top_per) < 5 :
                            target_p = pbo_236
                        elif int(top_per) < 10:
                            target_p = pbo_382
                        elif int(top_per) < 15:
                            target_p = pbo_5
                        elif int(top_per) < 20:
                            target_p = pbo_618
                        elif int(top_per) < 25:
                            target_p = pbo_707
                        else :
                            target_p = pbo_786
                        # print(target_p,'목표가')
                        if top_per == 0:
                            top_per = 1
                        sql_p = "insert or replace into Pbo (code,target_p,target_close,target_open,body_per,top_per,pbo_236,pbo_382,pbo_5,pbo_618,pbo_707,pbo_786) values ('{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format(str(max_code),int(target_p),int(target_close),int(target_open),int(body_per),int(top_per),int(pbo_236),int(pbo_382),int(pbo_5),int(pbo_618),int(pbo_707),int(pbo_786))
                        # print(sql_p)
    
                        p_conn = sqlite3.connect("DataBase/buy_list.db")
                        pc = p_conn.cursor()
                        pc.execute(sql_p)
                        p_conn.commit()
                        p_conn.close()          
                        print(f'{max_code}의 피보니차 값은 {target_p} 입니다.')
                        print(sql_p)
                        # exit()
                        print(max_code , int(target_close), int(target_open), int(body_per), int(top_per), int(pbo_236), int(pbo_382), int(pbo_5), int(pbo_618), int(pbo_707), int(pbo_786))
                        
                    
        print(error_count, 'error_count')
    except:
        pass
        
                      

    conn = sqlite3.connect("DataBase/all.db")
    sql = 'DELETE FROM code'
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    conn.close()

    # 주금가능금액
    account_list = kiwoom.GetLoginInfo('ACCNO')
    account = account_list[0]
    df = kiwoom.block_request('opw00001', 계좌번호=account, 비밀번호입력매체구분='00', 조회구분=1, output='예수금상세현황', next=0)
    주문가능금액 = int(df['주문가능금액'][0])
    print(주문가능금액)
    conn = sqlite3.connect("DataBase/Guardian Angel.db")
    c = conn.cursor()
    c.execute('update Info set total_money={}'.format(주문가능금액))
    conn.commit()


    # 보유종목 수량 업데이트
    df = kiwoom.block_request('opw00004', 계좌번호=account, 비밀번호='6215', 상장폐지조회구분=0, 비밀번호입력매체구분='00', output='종목코드', next=0)
    conn = sqlite3.connect("DataBase/Guardian Angel.db")
    c = conn.cursor()
    c.execute('update Info set bo_you={}'.format(len(df)))
    conn.commit()

    df = kiwoom.block_request('opw00004', 계좌번호=account, 비밀번호='6215', 상장폐지조회구분=0, 비밀번호입력매체구분='00', output='종목코드', next=0)
    보유종목 = []
    for i in range(len(df)):
        보유종목.append(df['종목코드'][i][1:])
    conn = sqlite3.connect("DataBase/Guardian Angel.db")
    c = conn.cursor()
    c.execute('update Info set boyou_code="{}"'.format(보유종목))
    conn.commit()



    send_msg(f'종목 데이터 수집 / 처리완료 / 피보{피보카운터}')


    now = time.localtime()
    now_1 = '%04d%02d%02d' % (now.tm_year,now.tm_mon,now.tm_mday)
    conn = sqlite3.connect("DataBase/Guardian Angel.db")
    c = conn.cursor()
    c.execute("update Info set update_date='{}'".format(now_1))
    conn.commit()
else:
    print('업데이트 완료')


