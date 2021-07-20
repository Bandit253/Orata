

from telethon import TelegramClient, events
import pandas as pd
from config.Ttrader_config import api_hash,bot_token,chatname,api_id,TRADE_UNIT,DB_CONFIG, DEFAULT_CLOSE_DELAY
from util import DBaccess as db
from markettools import CBs, CBpub, reportbalance

DB = db.postgres(DB_CONFIG)

client = TelegramClient(chatname, api_id, api_hash).start(bot_token=bot_token)


async def send_mess(entity, message):
    await client.send_message(entity=entity, message=message)

@client.on(events.NewMessage(chats=chatname))
async def my_event_handler(event):
    rec = event.raw_text
    print(rec)  
    action =rec.split(" ")[0].upper()
    if action in ('BUY', 'SELL', 'HOLD', 'BAL'):
        model = int(rec.split(" ")[1])
        closedelay = int(rec.split(" ")[2])
        # if not isinstance(closedelay, int):
        #     closedelay = DEFAULT_CLOSE_DELAY
        # else:
        closedelay = closedelay*60
        if action == 'BUY':
            buyres = CBs[model].marketBuy('BTC-USD', TRADE_UNIT)
            tradeid = buyres.id[0]
            # acc = reportbalance(CBs[model])
            # await send_mess(chatname, acc)
            buyreport = CBs[model].getOrderID(id=tradeid)
            buyreport['model'] = model
            buyreport['delay'] = closedelay
            DB.insert('trades', buyreport)

        elif action == 'SELL':
            rate = CBpub.getprice(f'BTC-USD')
            quantitytosell = TRADE_UNIT/ float(rate)
            sellres =CBs[model].marketSell('BTC-USD', quantitytosell)
            tradeid = sellres.id[0]
            acc = reportbalance(CBs[model])
            await send_mess(chatname, acc)
            sellreport = CBs[model].getOrderID(id=tradeid)
            sellreport['model'] = model
            sellreport['delay'] = closedelay
            DB.insert('trades', sellreport)
        elif action == "HOLD":
            acc = reportbalance(CBs[model])
            await send_mess(chatname, acc)
        elif action == 'BAL':
            print
        else:
            print(f"No advice given")
    return


def main():
    client.start()
    client.run_until_disconnected()


if __name__ == '__main__':
    main()


