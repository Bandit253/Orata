

from telethon import TelegramClient, events
import pandas as pd
from config.Ttrader_config import api_hash,bot_token,chatname,api_id,TRADE_UNIT,DB_CONFIG, DEFAULT_CLOSE_DELAY
from util import DBaccess as db
from markettools import CBs, CBpub, getbalance, reportbalance, resetbalances

DB = db.postgres(DB_CONFIG)

client = TelegramClient(chatname, api_id, api_hash).start(bot_token=bot_token)

def updatedb(CB, tradeid, model, delay):
    tradereport = CBs[model].getOrderID(id=tradeid)
    tradereport['model'] = model
    tradereport['delay'] = delay
    DB.insert('trades', tradereport)
    status, jsstat = getbalance(CB)
    DB.insert('status', status)
    return


async def send_mess(entity, message):
    await client.send_message(entity=entity, message=message)

@client.on(events.NewMessage(chats=chatname))
async def my_event_handler(event):
    rec = event.raw_text
    print(rec)  
    command = rec.split(" ")
    action =command[0].upper()
    if action in ('BUY', 'SELL', 'HOLD', 'BAL', 'reset'):
        modelindex = int(command[1]) 
        if len(command) > 3:
            closedelay = int(command[2])* 60
        else:
            closedelay = DEFAULT_CLOSE_DELAY
        if action == 'BUY':
            buyres = CBs[modelindex].marketBuy('BTC-USD', TRADE_UNIT)
            tradeid = buyres.id[0]
            acc = reportbalance(CBs[modelindex])
            await send_mess(chatname, acc)
            updatedb(CBs[modelindex], tradeid, modelindex, closedelay)
        elif action == 'SELL':
            rate = CBpub.getprice(f'BTC-USD')
            quantitytosell = TRADE_UNIT/ float(rate)
            sellres =CBs[modelindex].marketSell('BTC-USD', quantitytosell)
            tradeid = sellres.id[0]
            acc = reportbalance(CBs[modelindex])
            await send_mess(chatname, acc)
            updatedb(CBs[modelindex], tradeid, modelindex, closedelay)
        elif action == "HOLD":
            # acc = reportbalance(CBs[modelindex])
            await send_mess(chatname, "HOLD functionality has been deprecated, use 'BAL <index>'")
        elif action == 'BAL':
            acc = reportbalance(CBs[modelindex])
            await send_mess(chatname, acc)
        elif action == 'reset':
            resetbalances(CBs[4], CBs[modelindex])
            dfbal, jsbal = getbalance(CBs[modelindex])
            jsbal['model'] = f'model {modelindex} - RESET'
            DB.insert(dfbal)
            await send_mess(chatname, jsbal)
        else:
            print(f"No advice given")
    return


def main():
    client.start()
    client.run_until_disconnected()


if __name__ == '__main__':
    main()


