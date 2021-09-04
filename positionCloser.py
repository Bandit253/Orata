import datetime
import threading
import requests
import pandas as pd

from config.Ttrader_config import chat_id,bot_token,TRADE_UNIT,DB_CONFIG, CYCLE_TIME
from util import DBaccess as db
from markettools import CBs, reportbalance, getbalance

DB = db.postgres(DB_CONFIG)

def send_direct_mess( message):
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    data = {'chat_id': {chat_id}, 'text': message}
    res = requests.post(url, data).json()
    print(res)
    return

def getorders():
    try:
        sql = """select id, product_id, filled_size, side, model """
        sql += """ from trades """
        sql += f""" where extract(EPOCH from now()::timestamp - done_at::timestamp at time zone 'UTC') > delay """
        sql += """ and sold = '0' and delay > 0 """
        sql += """ ORDER BY created_at desc; """
        openbuys = DB.querydb(sql)
    except Exception as e:
        print(e)
    return openbuys

def closeout(tradeids):
    try:
        sql = """ update trades """
        sql += f""" set sold = '{tradeids[0]}'"""
        sql += f""" where id = '{tradeids[1]}'; """    
        DB.update(sql)
        sql = """ update trades """
        sql += f""" set sold = '{tradeids[1]}'"""
        sql += f""" where id = '{tradeids[0]}'; """
        DB.update(sql)
    except Exception as e:
        print(e)
    return 


def updatefailedrev(tradeid):
    try:
        sql = """ update trades """
        sql += f""" set sold = 'REVERSE FAILED'"""
        sql += f""" where id = '{tradeid}'; """    
        DB.update(sql)
    except Exception as e:
        print(e)
    return 

def getprofit(buyid):
    try:
        sql = """ select B.executed_value - A.executed_value as Profit """
        sql += f""" from trades A, trades B """
        sql += f""" where A.id ='{buyid}' and A.id = B.sold  """
        profit = DB.querydb(sql)[0]
    except Exception as e:
        print(e)
    return profit

def closebuys(trades):
    # print('Start closebuys')
    try:
        cb2report = []
        if len(trades) > 0:
            for row in trades:
                modelindex = row[4]
                action = row[3]
                profit = 0
                if action == 'buy':
                    if DB.checkbalances(modelindex, btc=row[2] ):
                        traderes = CBs[modelindex].marketSell(row[1], row[2])
                        nextaction = 'SELL'
                    else:
                        message = f"Reverse trade SELL for protfolio {modelindex} failed due to insuffient BTC"
                        send_direct_mess(message)
                        nextaction = 'SKIP'
                else:
                    if DB.checkbalances(modelindex, dollars=TRADE_UNIT):
                        traderes = CBs[modelindex].marketBuy(row[1], TRADE_UNIT)
                        nextaction = 'BUY'
                    else:
                        message = f"Reverse trade BUY for protfolio {modelindex} failed due to insuffient USD"
                        send_direct_mess(message)
                        nextaction = 'SKIP'
                if nextaction == 'SKIP':
                    updatefailedrev(row[0])
                else:
                    tradeid = traderes.id[0]
                    tradereport = CBs[modelindex].getOrderID(id=tradeid)
                    DB.insert('trades', tradereport)
                    closeout((tradeid,row[0]))
                    status,  jsbal = getbalance(CBs[modelindex])
                    status['trade'] = nextaction
                    status['action'] = 'CLOSE'
                    DB.insert('status', status)
                    if row[4] not in cb2report:
                        cb2report.append(modelindex)
                    profit = getprofit(row[0])[0]
                    # print(profit)
                    msg = f"Closing trade '{nextaction}' : Profit: {profit:.3f} " 
                    send_direct_mess(msg)
    except Exception as e:
        print(e)
    return cb2report

def clearoutstanding():
    openbuys = getorders()
    report = closebuys(openbuys)
    return report

def main():
    ticker = threading.Event()
    while not ticker.wait(CYCLE_TIME): #
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(now)
        report = clearoutstanding()
        if len(report) > 0:
            for p in report:
                bal = reportbalance(CBs[p])
                send_direct_mess(bal)
    return

if __name__ == '__main__':
    main()

