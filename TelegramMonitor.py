import datetime
import json
from telethon import TelegramClient, events
import pandas as pd
from config.Ttrader_config import api_hash,bot_token,chatname,api_id,TRADE_UNIT,DB_CONFIG, DEFAULT_CLOSE_DELAY
from util import DBaccess as db
from markettools import CBs, CBpub, getbalance, reportbalance, resetbalances
from graphing import createfillchart, dffromdb, createchart

DB = db.postgres(DB_CONFIG)

client = TelegramClient(chatname, api_id, api_hash).start(bot_token=bot_token)

def updatedb(CB, tradeid, model, delay, trade, action):
    tradereport = CBs[model].getOrderID(id=tradeid)
    tradereport['model'] = model
    tradereport['delay'] = delay
    DB.insert('trades', tradereport)
    status, jsstat = getbalance(CB)
    status['trade'] = trade
    status['action'] = action
    DB.insert('status', status)
    return

def getnow():
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    return now, yesterday

# def getaccountbalances(portfolio):
#     try:
#         sql = """select "BTC", "USD" """
#         sql += """ from status """
#         sql += f""" where model = 'model {portfolio}' """
#         sql += """ ORDER BY created desc limit 1; """
#         balances = DB.querydb(sql)
#     except Exception as e:
#         print(e)
#     return balances

# def checkbalances(portfolio, btc=None, dollars=None):
#     bal = getaccountbalances(portfolio)
#     if btc is not None and float(bal[0][0]) > btc:
#         return True
#     if dollars is not None and float(bal[0][1]) > dollars:
#         return True
#     return False


async def send_mess(entity, message):
    await client.send_message(entity=entity, message=message)

@client.on(events.NewMessage(chats=chatname))
async def my_event_handler(event):
    rec = event.raw_text
    # print(rec)  
    command = rec.split(" ")
    action =command[0].upper()
    if action in ('BUY', 'SELL', 'HOLD', 'BAL', 'RESET', 'CHART'):
        print(rec) 
        if action != 'CHART':
            modelindex = int(command[1]) 
        if len(command) >= 3: 
            closedelay = int(command[2])* 60
        else:
            closedelay = DEFAULT_CLOSE_DELAY
        if action == 'BUY':
            if DB.checkbalances(modelindex, dollars=TRADE_UNIT):
                buyres = CBs[modelindex].marketBuy('BTC-USD', TRADE_UNIT)
                tradeid = buyres.id[0]
                acc = reportbalance(CBs[modelindex])
                await send_mess(chatname, acc)
                updatedb(CBs[modelindex], tradeid, modelindex, closedelay, 'BUY', 'OPEN')
            else:
                err = f"Portfolio {modelindex} does not have enough funds to execute trade"
                await send_mess(chatname, err)
        elif action == 'SELL':
            rate = CBpub.getprice(f'BTC-USD')
            quantitytosell = TRADE_UNIT/ float(rate)
            if DB.checkbalances(modelindex, btc=quantitytosell):
                sellres =CBs[modelindex].marketSell('BTC-USD', quantitytosell)
                tradeid = sellres.id[0]
                acc = reportbalance(CBs[modelindex])
                await send_mess(chatname, acc)
                updatedb(CBs[modelindex], tradeid, modelindex, closedelay, 'SELL', 'OPEN')
            else:
                err = f"Portfolio {modelindex} does not have enough funds to execute trade"
                await send_mess(chatname, err)
        elif action == "HOLD":
            await send_mess(chatname, "HOLD functionality has been deprecated, use 'BAL <index>'")
        elif action == 'BAL':
            acc = reportbalance(CBs[modelindex])
            await send_mess(chatname, acc)
        elif action == 'CHART':
            type = command[1].upper()
            mods=[]
            for cmd in command[2:]:
                mods.append(cmd)
            dt_to, dt_from = getnow()
            zipchart = None
            if type == 'F':
                df = dffromdb('status', model=mods, dt_from=dt_from, dt_to=dt_to )
                zipchart = createfillchart(df)
            elif type == 'D':
                df = dffromdb('status', model=mods, dt_from=dt_from, dt_to=dt_to )
                zipchart = createchart(df, zero=False, field='USD') 
            elif type == 'DZ':
                df = dffromdb('status', model=mods, dt_from=dt_from, dt_to=dt_to )
                zipchart = createchart(df, zero=True, field='USD') 
            elif type == 'T':
                df = dffromdb('status', model=mods, dt_from=dt_from, dt_to=dt_to )
                zipchart = createchart(df, zero=False, field='Total') 
            elif type == 'TZ':
                df = dffromdb('status', model=mods, dt_from=dt_from, dt_to=dt_to )
                zipchart = createchart(df, zero=True, field='Total')                 
            else:   
                await client.send_message(chatname, f"Error: '{rec}' is not a valid chart command" )       
            if zipchart:                
                await client.send_file(chatname, zipchart )
        elif action == 'RESET':
            resetbalances(CBs[4], CBs[modelindex])
            dfbal, jsbal = getbalance(CBs[modelindex])
            jsbal['model'] = f'model {modelindex} - RESET'
            DB.insert('status', dfbal)
            sql = """ update trades """
            sql += f""" set sold = 'rest'"""
            sql += f""" where model = '{modelindex}' and sold = '0'; """
            DB.update(sql)
            ostr = json.dumps(jsbal)
            await send_mess(chatname, ostr)
        else:
            print(f"No advice given")
    return


def main():
    client.start()
    client.run_until_disconnected()


if __name__ == '__main__':
    main()


