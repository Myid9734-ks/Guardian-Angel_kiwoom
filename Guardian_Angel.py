import time
# 주식체결 실시간타입 구독신청
import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
import pymysql
import logging
import sqlite3
import time
import requests
import threading
import pprint
# 주문가능금액
# 최대보유수량
class MyWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.mesu_algo = 4
        self.주문가능금액 = ''
        self.최대보유수량 = ''
        self.주문전송종목 = []
        self.현재보유수량 = ''
        self.메세지전송 = []
        self.보유종목 = []   
        try:
            b_code = sub_pg.database(dbtype='maria', load=True, sql='SELECT boyou_code from Info',\
                        host='myid9734.synology.me', user='myid9734', db_name='Guardian Angel', \
                            database='Guardian Angel', password='As5017460*', port=3307)[0][0]
            b_code = b_code.replace("[","").replace("]","").replace("'","").replace(",","").replace(" ","")
            reange_len = len(b_code)
            self.보유종목 = []
            c=0
            for i in range(0, int(reange_len / 6)):
                aaa = (b_code[int(i+c):int(i+c+6)])
                c = c+5
                self.보유종목.append(aaa)
        except:
            b_code = sub_pg.database(dbtype='sqlite3', load=True, sql='SELECT boyou_code from Info',\
                        host='myid9734.synology.me', user='myid9734', db_name='Guardian Angel', \
                            database='Guardian Angel', password='As5017460*', port=3307)[0][0]
            b_code = b_code.replace("[","").replace("]","").replace("'","").replace(",","").replace(" ","")
            reange_len = len(b_code)
            self.보유종목 = []
            c=0
            for i in range(0, int(reange_len / 6)):
                aaa = (b_code[int(i+c):int(i+c+6)])
                c = c+5
                self.보유종목.append(aaa)
        try:
            load_data = sub_pg.database(dbtype='maria', load=True, sql='SELECT total_money, mesu_count, bo_you from Info',\
                        host='myid9734.synology.me', user='myid9734', db_name='Guardian Angel', \
                            database='Guardian Angel', password='As5017460*', port=3307)[0]    
            
            
        except:
            load_data = sub_pg.database(dbtype='sqlite3', load=True, sql='SELECT total_money, mesu_count, bo_you from Info',\
                        host='myid9734.synology.me', user='myid9734', db_name='Guardian Angel', \
                            database='Guardian Angel', password='As5017460*', port=3307)[0]
            
            
            load_data_1 = sub_pg.database(dbtype='sqlite3', db_name='buy_list', load=True, sql='SELECT * from mesu')            
            load_data_1 = len(load_data_1)
        
        self.주문가능금액 = load_data[0]
        self.최대보유수량 = load_data[1]
        self.현재보유수량 = load_data[2]
        self.종목당주문금액 = self.주문가능금액 /  (self.최대보유수량 - self.현재보유수량)
        if self.mesu_algo == 3 or self.mesu_algo == '3' or self.mesu_algo == 4 or self.mesu_algo == '4':
            # self.최대보유수량 = load_data_1
            self.종목당주문금액 = int(self.주문가능금액 /  (self.최대보유수량))
            
        # 매수대기중인 종목을 코드와 목표가 딕셔너리로 저장
        if self.mesu_algo == 1: # 돌파매매
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            c.execute('SELECT code, last0, ma_3, ma_5, ma_10, ma_20, ma_60 from list_buy WHERE (close * 0.15 + close) > last0')
            tt = c.fetchall()
            self.mesu_code_list = {}
            for i in tt :
                self.mesu_code_list[i[0]] = i[1],i[2],i[3],i[4],i[5],i[6]
                
        elif self.mesu_algo == 2: # 피보나치 매물대
            try:
                tt = sub_pg.database(dbtype='maria', load=True, sql='SELECT code, target_p, pbo_236, pbo_382, pbo_5, pbo_618, pbo_707, pbo_786 from Pbo', host='myid9734.synology.me', user='myid9734', db_name='buy_list', database='Pbo', password='As5017460*', port=3307)                
            except:
                tt = sub_pg.database(dbtype='sqlite3', load=True, sql='SELECT code, target_p, pbo_236, pbo_382, pbo_5, pbo_618, pbo_707, pbo_786 from Pbo', db_name='buy_list')
            """code, target_p, pbo_236, pbo_382, pbo_5, pbo_618, pbo_707, pbo_786"""
            """  0      1         1        2       3       4        5        6 """
            self.mesu_code_list = {}
            for i in tt :
                self.mesu_code_list[i[0]] = i[1],i[2],i[3],i[4],i[5],i[6],i[7]
        elif self.mesu_algo == 3 or self.mesu_algo == '3':
            tt = sub_pg.database(dbtype='sqlite3', load=True, sql='SELECT code, target_p, pbo_236, pbo_382, pbo_5, pbo_618, pbo_707, pbo_786 from mesu', db_name='buy_list')
            self.mesu_code_list = {}
            for i in tt :
                self.mesu_code_list[i[0]] = i[1],i[2],i[3],i[4],i[5],i[6],i[7]
                
        # 실시간 감시를 위한 그룹지정
        self.group_1 =''
        self.group_2 =''
        self.group_3 =''
        self.group_4 =''
        self.group_5 =''
        self.group_6 =''
        self.group_7 =''
        self.group_8 =''
        self.group_9 =''
        self.group_10 =''
        self.group_11 =''
        self.group_12 =''
        self.group_13 =''
        self.group_14 =''
        self.group_15 =''
        self.group_16 =''
        self.group_17 =''
        self.group_18 =''
        self.group_19 =''
        self.group_20 =''
        self.group_21 =''
        self.group_22 =''
        self.group_23 =''
        self.group_24 =''
        self.group_25 =''
        self.group_26 =''
        self.group_27 =''
        self.group_28 =''
        self.group_29 =''
        self.group_30 =''
        self.group_31 =''
        self.group_32 =''
        self.group_33 =''

        
        if self.mesu_algo == 1:
            sql = "SELECT code,last0 FROM list_buy WHERE (close * 0.15 + close) > last0 and close < 100000"
            conn = sqlite3.connect("DataBase/buy_list.db")
            c = conn.cursor()
            c.execute(sql)
            # conn.close()
        elif self.mesu_algo == 2:
            try:
                buy_list = sub_pg.database(dbtype='maria', load=True, sql='SELECT code, target_p from Pbo',\
                        host='myid9734.synology.me', user='myid9734', db_name='Guardian Angel', \
                            database='Pbo', password='As5017460*', port=3307)
            except:
                buy_list = sub_pg.database(dbtype='sqlite3', load=True, sql='SELECT code, target_p from Pbo',\
                        host='myid9734.synology.me', user='myid9734', db_name='buy_list', \
                            database='Pbo', password='As5017460*', port=3307)
        
        elif self.mesu_algo == 3 or self.mesu_algo == '3':
            try:
                buy_list = sub_pg.database(dbtype='maria', load=True, sql='SELECT code, target_p from Pbo',\
                        host='myid9734.synology.me', user='myid9734', db_name='Guardian Angel', \
                            database='Pbo', password='As5017460*', port=3307)
            except:
                buy_list = sub_pg.database(dbtype='sqlite3', load=True, sql='SELECT code, target_p from mesu',\
                        host='myid9734.synology.me', user='myid9734', db_name='buy_list', \
                            database='Pbo', password='As5017460*', port=3307)
        
        
        elif self.mesu_algo == 4 or self.mesu_algo == '4': #분봉 돌파
            buy_list = sub_pg.database(dbtype='sqlite3', load=True, sql='SELECT code, name from list_buy',\
                    host='myid9734.synology.me', user='myid9734', db_name='buy_list', \
                        database='Pbo', password='As5017460*', port=3307)
        group_no = 1
        add_code = ''
        count = 0
        self.algo4 = {}
        for di in buy_list:
            self.algo4[str(di[0])] = {f"code":0, "mesu_ok":False, 'cal_min':[], 'min_max':[], 'min_min':[] }


            
        for i in range(len(buy_list)):
            if len(self.group_1) <= 629:
                self.group_1 = self.group_1 + buy_list[i][0]+';'
            elif len(self.group_1) >= 629 and len(self.group_2) <= 629:
                self.group_2 = self.group_2 + buy_list[i][0]+';'

            elif len(self.group_2) >= 629 and len(self.group_3) <= 629:
                self.group_3 = self.group_3 + buy_list[i][0]+';'

            elif len(self.group_3) >= 629 and len(self.group_4) <= 629:
                self.group_4 = self.group_4 + buy_list[i][0]+';'

            elif len(self.group_4) >= 629 and len(self.group_5) <= 629:
                self.group_5 = self.group_5 + buy_list[i][0]+';'

            elif len(self.group_5) >= 629 and len(self.group_6) <= 629:
                self.group_6 = self.group_6 + buy_list[i][0]+';'

            elif len(self.group_6) >= 629 and len(self.group_7) <= 629:
                self.group_7 = self.group_7 + buy_list[i][0]+';'

            elif len(self.group_7) >= 629 and len(self.group_8) <= 629:
                self.group_8 = self.group_8 + buy_list[i][0]+';'
            elif len(self.group_8) >= 629 and len(self.group_9) <= 629:
                self.group_9 = self.group_9 + buy_list[i][0]+';'

            elif len(self.group_9) >= 629 and len(self.group_10) <= 629:
                self.group_10 = self.group_10 + buy_list[i][0]+';'

            elif len(self.group_10) >= 629 and len(self.group_11) <= 629:
                self.group_11 = self.group_11 + buy_list[i][0]+';'
            elif len(self.group_11) >= 629 and len(self.group_12) <= 629:
                self.group_12 = self.group_12 + buy_list[i][0]+';'

            elif len(self.group_12) >= 629 and len(self.group_13) <= 629:
                self.group_13 = self.group_13 + buy_list[i][0]+';'

            elif len(self.group_13) >= 629 and len(self.group_14) <= 629:
                self.group_14 = self.group_14 + buy_list[i][0]+';'

            elif len(self.group_14) >= 629 and len(self.group_15) <= 629:
                self.group_15 = self.group_15 + buy_list[i][0]+';'

            elif len(self.group_15) >= 629 and len(self.group_16) <= 629:
                self.group_16 = self.group_16 + buy_list[i][0]+';'

            elif len(self.group_16) >= 629 and len(self.group_17) <= 629:
                self.group_17 = self.group_17 + buy_list[i][0]+';'

            elif len(self.group_17) >= 629 and len(self.group_18) <= 629:
                self.group_18 = self.group_18 + buy_list[i][0]+';'

            elif len(self.group_18) >= 629 and len(self.group_19) <= 629:
                self.group_19 = self.group_19 + buy_list[i][0]+';'

            elif len(self.group_19) >= 629 and len(self.group_20) <= 629:
                self.group_20 = self.group_20 + buy_list[i][0]+';'
            elif len(self.group_20) >= 629 and len(self.group_21) <= 629:
                self.group_21 = self.group_21 + buy_list[i][0]+';'
            elif len(self.group_21) >= 629 and len(self.group_22) <= 629:
                self.group_22 = self.group_22 + buy_list[i][0]+';'
            elif len(self.group_22) >= 629 and len(self.group_23) <= 629:
                self.group_23 = self.group_23 + buy_list[i][0]+';'
            elif len(self.group_23) >= 629 and len(self.group_24) <= 629:
                self.group_24 = self.group_24 + buy_list[i][0]+';'
            elif len(self.group_24) >= 629 and len(self.group_25) <= 629:
                self.group_25 = self.group_25 + buy_list[i][0]+';'
            elif len(self.group_25) >= 629 and len(self.group_26) <= 629:
                self.group_26 = self.group_26 + buy_list[i][0]+';'
            elif len(self.group_26) >= 629 and len(self.group_27) <= 629:
                self.group_27 = self.group_27 + buy_list[i][0]+';'
            elif len(self.group_27) >= 629 and len(self.group_28) <= 629:
                self.group_28 = self.group_28 + buy_list[i][0]+';'
            elif len(self.group_28) >= 629 and len(self.group_29) <= 629:
                self.group_29 = self.group_29 + buy_list[i][0]+';'
            elif len(self.group_29) >= 629 and len(self.group_30) <= 629:
                self.group_30 = self.group_30 + buy_list[i][0]+';'
            elif len(self.group_30) >= 629 and len(self.group_31) <= 629:
                self.group_31 = self.group_31 + buy_list[i][0]+';'
            elif len(self.group_31) >= 629 and len(self.group_32) <= 629:
                self.group_32 = self.group_32 + buy_list[i][0]+';'
            elif len(self.group_32) >= 629 and len(self.group_33) <= 629:
                self.group_33 = self.group_33 + buy_list[i][0]+';'
        self.group_1 = self.group_1[:-1]
        self.group_2 = self.group_2[:-1]
        self.group_3 = self.group_3[:-1]
        self.group_4 = self.group_4[:-1]
        self.group_5 = self.group_5[:-1]
        self.group_6 = self.group_6[:-1]
        self.group_7 = self.group_7[:-1]
        self.group_8 = self.group_8[:-1]
        self.group_9 = self.group_9[:-1]
        self.group_10 = self.group_10[:-1]
        self.group_11 = self.group_11[:-1]
        self.group_12 = self.group_12[:-1]
        self.group_13 = self.group_13[:-1]
        self.group_14 = self.group_14[:-1]
        self.group_15 = self.group_15[:-1]
        self.group_16 = self.group_16[:-1]
        self.group_17 = self.group_17[:-1]
        self.group_18 = self.group_18[:-1]
        self.group_19 = self.group_19[:-1]
        self.group_20 = self.group_20[:-1]
        self.group_21 = self.group_21[:-1]
        self.group_22 = self.group_22[:-1]
        self.group_23 = self.group_23[:-1]
        self.group_24 = self.group_24[:-1]
        self.group_25 = self.group_25[:-1]
        self.group_26 = self.group_26[:-1]
        self.group_27 = self.group_27[:-1]
        self.group_28 = self.group_28[:-1]
        self.group_29 = self.group_29[:-1]
        self.group_30 = self.group_30[:-1]
        self.group_31 = self.group_31[:-1]
        self.group_32 = self.group_32[:-1]
        
        self.setGeometry(300, 300, 300, 300)
        self.setWindowTitle("") # 위젯 타이틀

        self.plain_text_edit = QPlainTextEdit(self)
        self.plain_text_edit.setReadOnly(True)
        self.plain_text_edit.move(10, 10)
        self.plain_text_edit.resize(280, 280)

        self.ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self.ocx.OnEventConnect.connect(self._handler_login)
        self.ocx.OnReceiveRealData.connect(self._handler_real_data)
        self.ocx.OnReceiveTrData.connect(self._handler_tr_data)
        self.CommConnect()
        
    def request_opw00004(self):
        self.SetInputValue("계좌번호", '3358259411')
        self.SetInputValue("비밀번호", "6215")
        self.SetInputValue("상장폐지조회구분", 0)
        self.SetInputValue("비밀번호입력매체구분", "00")
        self.CommRqData("계좌평가현황", "opw00004", 0, "9002")

    def send_msg(self, msg=''):
        response = requests.post(
        'https://notify-api.line.me/api/notify',
        headers={
            'Authorization' : 'Bearer rs5WOIz2jZQ3NGOBZqN1cbm6WHmnqMCt2K4ydqxFkco'
        },
        data={
            'message': msg
            }
        )

    def CommConnect(self):
        self.ocx.dynamicCall("CommConnect()")

    def _handler_login(self, err_code):
        if err_code == 0:
            self.plain_text_edit.appendPlainText("로그인 완료")
            self.send_msg('로그인 완료')
            print('로그인 완료')
            # 로그인 후 실시간 구독
            # self.subscribe_stock_conclusion('2')
            # self.request_opw00004()
            
            # 로그인완료 Tr
            # 로그인 완료후 잔고조회요청 ---> 매입가 계산후 손익률 계산
            
            
            if len(self.group_1) != 0:
                self.subscribe_stock_conclusion(screen_no='1213', group_list=self.group_1)
            if len(self.group_2) != 0:
                self.subscribe_stock_conclusion(screen_no='1214', group_list=self.group_2)
            if len(self.group_3) != 0:
                self.subscribe_stock_conclusion(screen_no='1215', group_list=self.group_3)
            if len(self.group_4) != 0:
                self.subscribe_stock_conclusion(screen_no='1216', group_list=self.group_4)
            if len(self.group_5) != 0:
                self.subscribe_stock_conclusion(screen_no='1217', group_list=self.group_5)
            if len(self.group_6) != 0:
                self.subscribe_stock_conclusion(screen_no='1218', group_list=self.group_6)
            if len(self.group_7) != 0:
                self.subscribe_stock_conclusion(screen_no='1219', group_list=self.group_7)
            if len(self.group_8) != 0:
                self.subscribe_stock_conclusion(screen_no='1220', group_list=self.group_8)
            if len(self.group_9) != 0:
                self.subscribe_stock_conclusion(screen_no='1221', group_list=self.group_9)
            if len(self.group_10) != 0:
                self.subscribe_stock_conclusion(screen_no='1222', group_list=self.group_10)
            if len(self.group_11) != 0:
                self.subscribe_stock_conclusion(screen_no='1223', group_list=self.group_11)
            if len(self.group_12) != 0:
                self.subscribe_stock_conclusion(screen_no='1224', group_list=self.group_12)
            if len(self.group_13) != 0:
                self.subscribe_stock_conclusion(screen_no='1225', group_list=self.group_13)
            if len(self.group_14) != 0:
                self.subscribe_stock_conclusion(screen_no='1226', group_list=self.group_14)
            if len(self.group_15) != 0:
                self.subscribe_stock_conclusion(screen_no='1227', group_list=self.group_15)
            if len(self.group_16) != 0:
                self.subscribe_stock_conclusion(screen_no='1228', group_list=self.group_16)
            if len(self.group_17) != 0:
                self.subscribe_stock_conclusion(screen_no='1229', group_list=self.group_17)
            if len(self.group_18) != 0:
                self.subscribe_stock_conclusion(screen_no='1230', group_list=self.group_18)
            if len(self.group_19) != 0:
                self.subscribe_stock_conclusion(screen_no='1231', group_list=self.group_19)
            if len(self.group_20) != 0:
                self.subscribe_stock_conclusion(screen_no='1232', group_list=self.group_20)
            if len(self.group_21) != 0:
                self.subscribe_stock_conclusion(screen_no='1233', group_list=self.group_21)
            if len(self.group_22) != 0:
                self.subscribe_stock_conclusion(screen_no='1234', group_list=self.group_22)
            if len(self.group_23) != 0:
                self.subscribe_stock_conclusion(screen_no='1235', group_list=self.group_23)
            if len(self.group_24) != 0:
                self.subscribe_stock_conclusion(screen_no='1236', group_list=self.group_24)
            if len(self.group_25) != 0:
                self.subscribe_stock_conclusion(screen_no='1237', group_list=self.group_25)
            if len(self.group_26) != 0:
                self.subscribe_stock_conclusion(screen_no='1238', group_list=self.group_26)
            if len(self.group_27) != 0:
                self.subscribe_stock_conclusion(screen_no='1239', group_list=self.group_27)
            if len(self.group_28) != 0:
                self.subscribe_stock_conclusion(screen_no='1240', group_list=self.group_28)
            if len(self.group_29) != 0:
                self.subscribe_stock_conclusion(screen_no='1241', group_list=self.group_29)
            if len(self.group_30) != 0:
                self.subscribe_stock_conclusion(screen_no='1242', group_list=self.group_30)
            if len(self.group_31) != 0:
                self.subscribe_stock_conclusion(screen_no='1243', group_list=self.group_31)
            if len(self.group_32) != 0:
                self.subscribe_stock_conclusion(screen_no='1244', group_list=self.group_32)

    # 실시간 타입을 위한 메소드
    def SetRealReg(self, screen_no, code_list, fid_list, real_type):
        self.ocx.dynamicCall("SetRealReg(QString, QString, QString, QString)", 
                              screen_no, code_list, fid_list, real_type)

    def GetCommRealData(self, code, fid):
        data = self.ocx.dynamicCall("GetCommRealData(QString, int)", code, fid) 
        return data

    def DisConnectRealData(self, screen_no):
        self.ocx.dynamicCall("DisConnectRealData(QString)", screen_no)

    # 실시간 이벤트 처리 핸들러 
    def _handler_real_data(self, code, real_type, real_data):
        if real_type == "주식체결":
            체결시간 = self.GetCommRealData(code, 20)
            현재가 = abs(int(self.GetCommRealData(code, 10)))
            시가 = abs(int(self.GetCommRealData(code, 16)))
            고가 = abs(int(self.GetCommRealData(code, 17)))
            저가 = abs(int(self.GetCommRealData(code, 18)))
            등락율 = float(self.GetCommRealData(code, 12))
            체결강도 = self.GetCommRealData(code, 228)
            cnt = int(self.종목당주문금액 / 현재가)
            
            cnt = int(5000)
            
            # now = time.localtime()
            # now_time = '%02d%02d%02d' % (now.tm_hour, now.tm_min, now.tm_sec )
            
            
            if self.mesu_algo == 1:
                try:#매수 여부 결정
                    목표가 = self.mesu_code_list[code][0] 
                    ma3 = self.mesu_code_list[code][1]
                    ma5 = self.mesu_code_list[code][2] 
                    ma10 = self.mesu_code_list[code][3]
                    ma20 = self.mesu_code_list[code][4]
                    ma60  = self.mesu_code_list[code][5]
                    # ma5 > ma10 and ma10 > ma20 and 
                    if int(목표가) <int(현재가) and int(ma5) > int(ma20) and int(ma5) < int(현재가) and code not in self.주문전송종목 and int(self.최대보유수량) > int(self.현재보유수량) and code not in self.보유종목 and 시가 < 현재가 : # 보유종목수량 비교
                        print(code, 체결시간, 현재가, self.mesu_code_list[str(code)][0], 체결강도)
                        # 시가 < 현재가
                        if cnt >= 1:
                            self.SendOrder("매수", code[-4:], 3358259411, 1, code , cnt, 0, "03", "")
                            self.현재보유수량 += 1 
                            self.주문전송종목.append(code)
                            time.sleep(0.05)
                            self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)][0]}) / 현재가({현재가})')
                        else:
                            self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)][0]}) / 현재가({현재가}) ----> 매수금액 부족')
                            self.주문전송종목.append(code)
                    else: 
                        print(code, 체결시간, 현재가, self.mesu_code_list[str(code)][0], 체결강도)
                except KeyError:
                    print(code, 체결시간, 현재가, 체결강도)
                    pass
                
            elif self.mesu_algo == 2:
                # try:
                print(f'code: {code}, 현재가: {현재가}, 등락율: {등락율}, 체결강도: {체결강도}')
                목표가 = self.mesu_code_list[code][0]
                if code not in self.주문전송종목 and int(self.최대보유수량) > int(self.현재보유수량) and code not in self.보유종목:
                    if 목표가 >  시가:
                        if 시가 > self.mesu_code_list[code][4]:
                            if int(현재가) < self.mesu_code_list[code][4]:
                                if cnt >=1:
                                    self.SendOrder("매수", code[-4:], 3358259411, 1, code , cnt, 0, "03", "")
                                    self.현재보유수량 += 1 
                                    self.주문전송종목.append(code)
                                    self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도})')
                                    print(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가} #1)')
                                else:
                                    self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) ----> 매수금액 부족 #1')
                                    print(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) ----> 매수금액 부족')
                                    self.주문전송종목.append(code)
                        
                        elif 시가 < self.mesu_code_list[code][4] and 시가 > self.mesu_code_list[code][5]:
                            if int(현재가) < self.mesu_code_list[code][5]:
                                if cnt >=1:
                                    self.SendOrder("매수", code[-4:], 3358259411, 1, code , cnt, 0, "03", "")
                                    self.현재보유수량 += 1 
                                    self.주문전송종목.append(code)
                                    self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도})')
                                    print(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) #2')
                                else:
                                    self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) ----> 매수금액 부족 #2')
                                    print(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) ----> 매수금액 부족')
                                    self.주문전송종목.append(code)
                        
                        elif 시가 < self.mesu_code_list[code][5] and 시가 > self.mesu_code_list[code][6]:
                            if int(현재가) < self.mesu_code_list[code][6]:
                                if cnt >=1:
                                    self.SendOrder("매수", code[-4:], 3358259411, 1, code , cnt, 0, "03", "")
                                    self.현재보유수량 += 1 
                                    self.주문전송종목.append(code)
                                    self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) #3')
                                    print(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도})')
                                else:
                                    self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) ----> 매수금액 부족 #3')
                                    print(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) ----> 매수금액 부족')
                                    self.주문전송종목.append(code)
                        
                        elif 시가 < self.mesu_code_list[code][6]:
                            if int(현재가) > self.mesu_code_list[code][6]:
                                if cnt >=1:
                                    self.SendOrder("매수", code[-4:], 3358259411, 1, code , cnt, 0, "03", "")
                                    self.현재보유수량 += 1 
                                    self.주문전송종목.append(code)
                                    self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) #4')
                                    print(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도})')
                                else:
                                    self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) ----> 매수금액 부족 #4')
                                    print(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) ----> 매수금액 부족')
                                    self.주문전송종목.append(code)
                                    
                    elif 목표가 < 시가 and 등락율 > -4.0 and 목표가 >= 현재가: #등락율로 많이 떨어진 종목 매수금지
                        if cnt >=1:
                            self.SendOrder("매수", code[-4:], 3358259411, 1, code , cnt, 0, "03", "")
                            self.현재보유수량 += 1 
                            self.주문전송종목.append(code)
                            self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) #5')
                            print(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도})')
                        else:
                            self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) ----> 매수금액 부족 #5')
                            print(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) ----> 매수금액 부족')
                            self.주문전송종목.append(code)
                # except KeyError:
                #     print(code, 체결시간, 현재가, 등락율)
                #     pass
            elif self.mesu_algo == 3 or self.mesu_algo == '3':
            # try:
                print(f'code: {code}, 현재가: {현재가}, 등락율: {등락율}, 체결강도: {체결강도}')
                목표가 = self.mesu_code_list[code][0]
                if code not in self.주문전송종목 and code not in self.보유종목:
                    if 현재가 > 시가 and cnt >=1:
                        # self.SendOrder("매수", code[-4:], 3358259411, 1, code , cnt, 0, "03", "")
                        self.주문전송종목.append(code)
                        # self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도})')
                        # self.메세지전송.append(code)
                        print(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가} #1)')
                    elif 현재가 < 시가 and cnt >=1:
                        if code not in self.메세지전송:
                            # self.send_msg(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) ----> 음봉 발생... #1')
                            # self.메세지전송.append(code)
                            print(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) ----> 매수금액 부족')
                        else:
                            print(f'종목포착({code}) / 목표가({self.mesu_code_list[str(code)]}) / 현재가({현재가}), 체결강도({체결강도}) ----> 매수금액 부족')
            
            elif self.mesu_algo == 4 or self.mesu_algo == '4':
                # print(f'code: {code}, 현재가: {현재가}, 등락율: {등락율}, 체결강도: {체결강도}, 시가: {시가}, 고가:{고가}, 저가:{저가}')
        
                now = 체결시간[0:7]
                # try:
                if int(now) < 90100 : # if int(now) < 90059: 
                    per = 1
                    self.algo4[code]['cal_min'].append(현재가) # 목표가 계산을 위한 1분봉이 될때까지 모음
                    self.algo4[code]['code'] = max(self.algo4[code]['cal_min']) # 모은 분봉중 최고값을 목표가로 저장
                    # self.algo4[code]['code'] = self.algo4[code]['min_max']
                    try:
                        per = (현재가 - self.algo4[code]['code']) / self.algo4[code]['code'] * 100 
                        per = float(per)
                    except:
                        pass
                    if per <= -1.5: # (self.algo4[code]['code'] < 현재가) and per <= -1.5
                        self.algo4[code]['mesu_ok']= 'True'
                    else:
                        print(f"현재가 : {현재가} / 목표가 {self.algo4[code]['code']} _____________ {code}")

                else:
                    print(f"현재가 : {현재가} / 목표가 {self.algo4[code]['code']} _____________ {code}")
                    if int(self.algo4[code]['code']) < 현재가:
                        # self.algo4[code]['code'] = self.algo4[code]['min_max']   
                        self.algo4[code]['code'] = 현재가 # 목표가 보다 현재가가 큰 경우

                        if (cnt >= 1) and (self.algo4[code]['mesu_ok'] == 'True') and (code not in self.주문전송종목):
                            self.send_msg(f"목표가{self.algo4[code]['code']}, {현재가}, {code}")
                            self.주문전송종목.append(code)
                            self.현재보유수량 += 1
                    elif int(self.algo4[code]['code']) == 현재가: # 목표가 보다 같을 경우
                        pass    
                    elif int(self.algo4[code]['code']) > 현재가: # 목표가 보다 작을 경우
                        per = (현재가 - self.algo4[code]['code']) / self.algo4[code]['code'] * 100 
                        per = float(per)
                        if per <= -1.5:
                            self.algo4[code]['mesu_ok']= 'True'
                        else:
                            print('눌림 대기중')
                # except Exception as e:
                #     break


                    # print('pppppppppppppppppppppppppppppppppp')
                    # if (self.현재보유수량 <= self.최대보유수량):
                    #     print('*************************************')
                    #     if str(code) not in self.주문전송종목:
                    #         print('-------------------------------------------------------------')
                    #         # if len(self.algo4[code]['min_max']) != 0:

                    #         print(self.algo4[code]['min_max'], code, type(self.algo4[code]['min_max']),'0000000000000000000000000')
                    #         if self.algo4[code]['min_max'] < 현재가 and (cnt >=1) and (self.algo4[code]['mesu_ok'] == 'True'):
                    #             # self.send_msg(f'종목포착({code}, 목표가{self.algo4[code]['min_max']}, 현재가{현재가}')
                                
                    #             self.send_msg(f"목표가{self.algo4[code]['min_max']}, {현재가}, {code}")
                    #             self.주문전송종목.append(code)
                    #             self.현재보유수량 += 1

                    #         else:
                    #             print('*************************************')
                    #             if len(self.algo4[code]['cal_min']) != 0:
                    #             #     # self.algo4[code]['min_max'] = max(self.algo4[code]['cal_min'])
                    #             #     # self.algo4[code]['min_max'] = self.algo4[code]['min_max']
                    #                 if (self.algo4[code]['min_max']) < 현재가:
                    #                     self.algo4[code][code] = 현재가
                    #                     print('고점 갱신', self.algo4[code][code], 현재가)
                    #                 elif (self.algo4[code]['min_max']) == 현재가:
                    #                     print('pass', self.algo4[code][code], 현재가)

                    #                 else:
                    #                     per = ((int((self.algo4[code]['min_max'])-현재가) / 현재가 * 100 ))
                    #                     # print(f"{self.algo4[code]['min_max']}, {현재가}, {per}, {code}")
                    #                     print((self.algo4[code]['min_max']), 현재가, code, per, '0000000000000000000000')
                    #                     if per >= 1.5:
                    #                         self.algo4[code]['mesu_ok']= 'True'
                    #                         print((self.algo4[code]['min_max']), 현재가, code, per, '0000000000000000000000')
                                            
                    #                     else:
                    #                         print('눌림 대기중')
                                            # print('눌림 대기중')dfsdfs

                # print(self.900070)
                """
                if 현재가가 딕셔너리 보다 크고 매수 대기가 True이면
                    매수주문
                else
                    if 딕셔너리보다 고가가 크면
                        딕셔너리 갱신
                    else
                        if 딕셔너리보다 저가가 1.5% 떨어졌는지
                            매수대기 = true
                        else 
                            pass
                """
                
            
    def SendOrder(self, rqname, screen, accno, order_type, code, quantity, price, hoga, order_no):
        self.ocx.dynamicCall("SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
                             [rqname, screen, accno, order_type, code, quantity, price, hoga, order_no])
        
    def subscribe_stock_conclusion(self, screen_no, group_list):
        self.SetRealReg(screen_no, group_list, "20", 0)
        self.plain_text_edit.appendPlainText("주식체결 구독신청")
        
    
    def _handler_tr_data(self, screen_no, rqname, trcode, record, next):
        if rqname == "KODEX일봉데이터":
            일자 = self.GetCommData(trcode, rqname, 0, "일자")
            고가 = self.GetCommData(trcode, rqname, 0, "고가")
            저가 = self.GetCommData(trcode, rqname, 0, "저가")
            self.range = int(고가) - int(저가)
            info = f"일자: {일자} 고가: {고가} 저가: {저가}"
            self.plain_text_edit.appendPlainText(info)
        elif rqname == "예수금조회":
            주문가능금액 = self.GetCommData(trcode, rqname, 0, "주문가능금액")
            주문가능금액 = int(주문가능금액)
            self.amount = int(주문가능금액 * 0.2)
            self.plain_text_edit.appendPlainText(f"투자금액: {self.amount}")

        elif rqname == "계좌평가현황":
            rows = self.GetRepeatCnt(trcode, rqname)
            for i in range(rows):
                종목코드 = self.GetCommData(trcode, rqname, i, "종목코드")
                보유수량 = self.GetCommData(trcode, rqname, i, "보유수량")    
                손익율 = self.GetCommData(trcode, rqname, i, "손익율")
                손익율 = int(손익율)/10000
                print(f'{종목코드} : {보유수량} : {손익율}' )

    # TR 요청을 위한 메소드
    def SetInputValue(self, id, value):
        self.ocx.dynamicCall("SetInputValue(QString, QString)", id, value)
    def GetRepeatCnt(self, trcode, rqname):
        ret = self.ocx.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)
        return ret

    def CommRqData(self, rqname, trcode, next, screen_no):
        self.ocx.dynamicCall("CommRqData(QString, QString, int, QString)", 
                              rqname, trcode, next, screen_no)

    def GetCommData(self, trcode, rqname, index, item):
        data = self.ocx.dynamicCall("GetCommData(QString, QString, int, QString)", 
                                     trcode, rqname, index, item)
        return data.strip()


class  sub_pg():
    def __init__(self):
        pass

    def database(dbtype='sqlite3', load=False, sql=None, host='myid9734.synology.me', user='myid9734', db_name=None, database='list_buy', password='As5017460*', port=3307):
        if dbtype == 'sqlite3':
            print('sqlite3........')
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
            print('maria........')
            conn = pymysql.connect(host=host, user=user, database=database, password=password , port=port) 
            cursor = conn.cursor() 
            cursor.execute(sql) 
            if load == True:
                data = cursor.fetchall()
                return data
            else:
                conn.commit() 
                conn.close()
        else: 
            print('dbtype error')



if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MyWindow()
    # window.show()
    
    app.exec_()