import threading
import requests
import pandas as pd

from config.Ttrader_config import chat_id,bot_token,TRADE_UNIT,DB_CONFIG, CYCLE_TIME
from util import DBaccess as db
from markettools import CBs, reportbalance

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
        sql += f""" where strftime('%s','now') - strftime('%s', done_at) > delay """
        sql += """ and sold = 0 ; """
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

def closebuys(trades):
    # print('Start closebuys')
    try:
        cb2report = []
        if len(trades) > 0:
            for row in trades:
                if row[3] == 'buy':
                    traderes = CBs[row[4]].marketSell(row[1], row[2])
                else:
                    traderes = CBs[row[4]].marketBuy(row[1], TRADE_UNIT)
                tradeid = traderes.id[0]
                tradereport = CBs[row[4]].getOrderID(id=tradeid)
                DB.insert('trades', tradereport)
                closeout((tradeid,row[0]))
                if row[4] not in cb2report:
                    cb2report.append(row[4])
    except Exception as e:
        print(e)
    return cb2report

def clearoutstanding():
    openbuys = getorders()
    report = closebuys(openbuys)
    return report

def main():
    ticker = threading.Event()
    while not ticker.wait(CYCLE_TIME):
        report = clearoutstanding()
        if len(report) > 0:
            for p in report:
                bal = reportbalance(CBs[p])
                send_direct_mess(bal)
    return

if __name__ == '__main__':
    main()

