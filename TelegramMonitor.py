import datetime
import json
from telethon import TelegramClient, events
import pandas as pd
from config.Ttrader_config import api_hash,bot_token,chatname,api_id,DB_CONFIG, DEFAULT_CLOSE_DELAY, MARGIN, TRADE_PERCENTAGE
from util import DBaccess as db
from markettools import CBs, CBpub, getbalance, reportbalance, resetbalances, getmarketprice
from graphing import createfillchart, dffromdb, createchart, dffromdbsql, createprofitchart
import uuid

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

def getparcelsize(portfolio, symbol, TRADE_UNIT, rate = 1):
    currbal = DB.getaccountbalances(portfolio)
    if symbol == 'USDC':
        parcel = float(currbal[0][1]) * TRADE_PERCENTAGE
        if parcel > TRADE_UNIT:
            return parcel
        else:
            print(float(currbal[0][1]))
            if TRADE_UNIT < float(currbal[0][1]):
                return TRADE_UNIT
            else:
                return 0
    elif symbol == 'BTC':
        parcel = float(currbal[0][0]) * TRADE_PERCENTAGE
        TU_BTC = TRADE_UNIT/ float(rate)
        if parcel > TU_BTC:
            return parcel
        else:
            if TU_BTC < float(currbal[0][0]):
                return TU_BTC
            else:
                return 0 


async def send_mess(entity, message):
    await client.send_message(entity=entity, message=message)

@client.on(events.NewMessage(chats=chatname))
async def my_event_handler(event):
    rec = event.raw_text
    # print(rec)  
    command = rec.split(" ")
    action =command[0].upper()
    if action in ('BUY', 'SELL','LBUY', 'LSELL', 'HOLD', 'BAL', 'RESET', 'CHART', 'ORDERS'):
        print(rec) 
        if action != 'CHART':
            modelindex = int(command[1]) 
        if len(command) >= 3: 
            closedelay = int(command[2])* 60
        else:
            closedelay = DEFAULT_CLOSE_DELAY
        TRADINGPAIR = CBs[modelindex].market
        TRADE_UNIT =  CBs[modelindex].trade_unit
        if action == 'BUY':
            parcel = getparcelsize(modelindex, 'USDC', TRADE_UNIT)
            print(f"parcel : {parcel}")
            if parcel > 0:
                buyres = CBs[modelindex].marketBuy(TRADINGPAIR, parcel)
                tradeid = buyres.id[0]
                acc = reportbalance(CBs[modelindex])
                await send_mess(chatname, f"{tradeid} - {acc}")
                updatedb(CBs[modelindex], tradeid, modelindex, closedelay, 'BUY', 'OPEN')
            else:
                err = f"Portfolio {modelindex} does not have enough funds to execute trade"
                await send_mess(chatname, err)
        #region LIMITED BUY
        # elif action == 'ORDERS':  ####################################
            
        #     orders = CBs[modelindex].getOrders(status='open') 
        #     print(orders.head())
        #         # tradeid = buyres.id[0]
        #     # acc = reportbalance(CBs[modelindex])

        #     await send_mess(chatname, orders.to_json())
        #     # updatedb(CBs[modelindex

        # elif action == 'LBUY':  ####################################
        #     if DB.checkbalances(modelindex, dollars=TRADE_UNIT):
        #         rate = float(CBpub.getprice(TRADINGPAIR))
        #         fprice = rate - (rate * MARGIN)
        #         futureprice = f"{fprice:.2f}"
        #         cli_id = str(uuid.uuid4())
        #         size = round(TRADE_UNIT/fprice, 8)
        #         buyres = CBs[modelindex].limitBuy(id=cli_id, market=TRADINGPAIR, price=futureprice, size=size,delay='min')
        #         # tradeid = buyres.id[0]
        #         acc = reportbalance(CBs[modelindex])

        #         await send_mess(chatname, acc)
        #         # updatedb(CBs[modelindex], tradeid, modelindex, closedelay, 'BUY', 'OPEN')
        #     else:
        #         err = f"Portfolio {modelindex} does not have enough funds to execute trade"
        #         await send_mess(chatname, err)
        #endregion

        elif action == 'SELL':
            rate = CBpub.getprice(TRADINGPAIR)
            parcel = getparcelsize(modelindex, 'BTC', TRADE_UNIT, rate)
            print(f"parcel : {parcel*float(rate)}")
            if parcel > 0:
                sellres =CBs[modelindex].marketSell(TRADINGPAIR, parcel)
                tradeid = sellres.id[0]
                acc = reportbalance(CBs[modelindex])
                await send_mess(chatname, f"{tradeid} - {acc}")
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
            elif type == 'T':
                df = dffromdbsql(model=mods, dt_from=dt_from, dt_to=dt_to )
                zipchart = o = createprofitchart(df, field='profit', zero=False)     
            elif type == 'P':   
                df = dffromdbsql(model=mods, dt_from=dt_from, dt_to=dt_to )
                zipchart = o = createprofitchart(df, field='profit', zero=False)       
            elif type == 'PZ':   
                df = dffromdbsql(model=mods, dt_from=dt_from, dt_to=dt_to )
                zipchart = o = createprofitchart(df, field='profit', zero=True)       
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


