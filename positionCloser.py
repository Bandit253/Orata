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

def closeout(sells):
    try:
        if len(sells) > 0:
            for sell in sells:
                sql = """ update trades """
                sql += f""" set sold = '{sell[0]}'"""
                sql += f""" where id = '{sell[1]}'; """    
                DB.update(sql)
                sql = """ update trades """
                sql += f""" set sold = '{sell[1]}'"""
                sql += f""" where id = '{sell[0]}'; """
                DB.update(sql)
            ret = True
        else:
            ret = False
    except Exception as e:
        print(e)
    return ret


def closebuys(trades):
    print('Start closebuys')
    try:
        buys2close = []
        for row in trades:
            if row[3] == 'buy':
                traderes = CBs[row[4]].marketSell(row[1], row[2])
            else:
                traderes = CBs[row[4]].marketBuy(row[1], TRADE_UNIT)
            tradeid = traderes.id[0]
            tradereport = CBs[row[4]].getOrderID(id=tradeid)
            DB.insert('trades', tradereport)
            buys2close.append((tradeid,row[0]))
    except Exception as e:
        print(e)
    return buys2close

def clearoutstanding():
    openbuys = getorders()
    buys2close = closebuys(openbuys)
    report = closeout(buys2close) 
    return report

def main():
    ticker = threading.Event()
    while not ticker.wait(CYCLE_TIME):
        report = clearoutstanding()
        # if report:
        #     bal = reportbalance()
        #     send_direct_mess(bal)
    return

if __name__ == '__main__':
    main()

