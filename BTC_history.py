
import os
import time
import json
import pandas as pd
import dateparser
import pytz
from datetime import datetime
import requests
from util import DBaccess as db
from config.Ttrader_config import DB_CONFIG

DB = db.postgres(DB_CONFIG)

pickle1m = 'util\Oct_1_2019_1m.pkl'
BASEURL = "https://api3.binance.com"

def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds
    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/
    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0) #

def interval_to_milliseconds(interval):
    """Convert a Binance interval string to milliseconds
    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str
    :return:
         None if unit not one of m, h, d or w
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }
    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            pass
    return ms

def get_candlestick_data(symbol, interval, startTime=None, endTime=None, limit=500):
    """
    Kline/candlestick bars for a symbol.
    Klines are uniquely identified by their open time.

    Args:
        symbol ([type]): [description]
        interval ([type]): [description]
        startTime ([type], optional): [description]. Defaults to None.
        endTime ([type], optional): [description]. Defaults to None.
        limit (int, optional): [description]. Defaults to 500.


        1499040000000,      // Open time
        "0.01634790",       // Open
        "0.80000000",       // High
        "0.01575800",       // Low
        "0.01577100",       // Close
        "148976.11427815",  // Volume
        1499644799999,      // Close time
        "2434.19055334",    // Quote asset volume
        308,                // Number of trades
        "1756.87402397",    // Taker buy base asset volume
        "28.46694368",      // Taker buy quote asset volume
        "17928899.62484339" // Ignore.
    """
    try:
        params = f"symbol={symbol}&interval={interval}"
        if startTime:
            params += f"&startTime={startTime}"
        if endTime:
            params += f"&endTime={endTime}"
        if limit:
            params += f"&limit={limit}"
        endpt = f'/api/v3/klines?'

        url = f"{BASEURL}{endpt}{params}"
        # header = {"X-MBX-APIKEY": APIKey}, headers=header
        candle = requests.get(url)
        candlejs = candle.json()
    except Exception as e:
        print(e.message)
        candlejs = {}
    end = time.perf_counter()

    return candlejs

def get_historical_klines(symbol, interval, start_str, end_str=None):
    """Get Historical Klines from Binance
    See dateparse docs for valid start and end string formats http://dateparser.readthedocs.io/en/latest/
    If using offset strings for dates add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    :param symbol: Name of symbol pair e.g BNBBTC
    :type symbol: str
    :param interval: Biannce Kline interval
    :type interval: str
    :param start_str: Start date string in UTC format
    :type start_str: str
    :param end_str: optional - end date string in UTC format
    :type end_str: str
    :return: list of OHLCV values
    """
    # cli = TC.BinanceTrader()
    output_data = []
    limit = 500
    timeframe = interval_to_milliseconds(interval)
    start_ts = date_to_milliseconds(start_str)
    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)
    idx = 0
    symbol_existed = False
    while True:
        temp_data = get_candlestick_data(
            symbol=symbol,
            interval=interval,
            limit=limit,
            startTime=start_ts,
            endTime=end_ts
        )
        if not symbol_existed and len(temp_data):
            symbol_existed = True
        if symbol_existed:
            output_data += temp_data
            start_ts = temp_data[len(temp_data) - 1][0] + timeframe
        else:
            start_ts += timeframe
        idx += 1
        if len(temp_data) < limit:
            break
        if idx % 3 == 0:
            time.sleep(1)
    return output_data

def dffromfile(infile):
    hdr = ['OpenTime','Open', 'High', 'Low','Close','Volume','Closetime','QuoteAssetVolume',
            'NumberofTrades','TakerBuyBaseAssetVol','TakerBuyQuoteAssetVol', 'ignore']
    with open(infile, 'r') as ifile:
        data = ifile.read()
    datarr= []
    datalist = data.strip('[[').strip(']]').strip(']][[').split('], [')
    if len(datalist) > 2:
        for dl in datalist:
            r = []
            for num in dl.split(', '):
                ans = float(num.replace('"',''))
                r.append(ans)
            datarr.append(r)
        df =  pd.DataFrame(datarr, columns =hdr)
        df['OpenTime'] = df['OpenTime'].astype('int64')
        df.NumberofTrades = df.NumberofTrades.astype('int64')
        df.ignore = df.ignore.astype('int64')
        df.TakerBuyQuoteAssetVol = df.TakerBuyQuoteAssetVol.astype('int64')
        df.Closetime = df.Closetime.astype('int64')
        df.QuoteAssetVolume = df.QuoteAssetVolume.astype('int64')
        df['dt'] = (df['OpenTime']/1000).astype("datetime64[s]")
        df['dtz'] = df['dt'].dt.tz_localize('utc').dt.tz_convert('Australia/Sydney')
    else:
        df= None
    return df

def get_df_his(sym, interval, since, to=None):
    results = get_historical_klines(sym, interval, since, to)
    traildata = r'util\tempdata'
    tmpfile = 'temp.json'
    filepath= os.path.join(traildata, tmpfile)
    with open(filepath, 'w') as f:
        json.dump(results, f)
    df = dffromfile(filepath)
    return df

def getnextstart(lastvalue, interval):
    """get start next time

    Args:
        lastvalue (int): last value in millisec
        interval (int): intervals in minutes eg 1, 15, 60
    """
    lastvalue = lastvalue/1000
    deltatime = interval * 60
    pdt = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(lastvalue + deltatime))
    return pdt

def updatehistory():
    df = DB.dffromsql(f""" select * from "BTC_history"  ORDER BY "OpenTime" desc LIMIT 1;""")
    t = df['OpenTime'].iloc[-1]
    nexttime = getnextstart(t, 1)
    nextdf = get_df_his('BTCUSDT', '1m', nexttime)
    if nextdf is not None:
        nextdf['dtz'] = nextdf['dt'].dt.tz_localize('utc').dt.tz_convert('Australia/Sydney').dt.tz_localize(None)
        DB.insert('BTC_history', nextdf)
    return

def getbtc(since:str)-> pd.DataFrame:
    df = DB.dffromsql(f""" select * from "BTC_history" WHERE dtz >= '{since}' ORDER BY "OpenTime";""")
    return df


def main():
    df = updatehistory()
    
    # print(df.head())
    # print(df.shape)
    # begin = pd.Timestamp('2021-07-20 10:00:00')
    # end = pd.Timestamp('2019-10-01 10:04:00')
    # # mask = (df['dtz'] > '2019-10-01 10:00:00') & (df['dtz'] <= '2019-10-01 10:04:00')
    # # dfc = df.loc[(df['dtz'].to_pydatetime() > begin) & (df['dtz'].to_pydatetime() <= end)] 
    # dfc = df[df.dtz.to_pydatetime() > begin]

    # print(dfc.tail())
    # print(dfc.shape)

 
if __name__ == '__main__':
    main()