
import datetime
import pandas as pd
import os
from pandas.core.frame import DataFrame
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from config.Ttrader_config import DB_CONFIG
from util import DBaccess as db
import BTC_history as btc
from zipfile import ZipFile, ZIP_DEFLATED
pd.options.mode.chained_assignment = None

DB = db.postgres(DB_CONFIG)

appPath = os.path.dirname(os.path.realpath(__file__))
dataPath = os.path.join(appPath, 'data')

def getnow():
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    return now, yesterday

def dffromdb(table:str, model:list=[], fields:list=[], dt_from:str=None, dt_to:str=None) -> pd.DataFrame:
    if len(fields) > 0:
        flds = ", ".join(fields)
        sql = f"SELECT {flds} "
    else:
        sql = f"SELECT * "
    sql += f"FROM {table} "
    if len(model) > 0 or dt_from or dt_to:
        where =[]
        sql += "WHERE "
        if len(model) >0:
            mod = [f"'model {str(x)}'" for x in model]
            modstr = ", ".join(mod)
            where.append(f"model in ({modstr}) ")
        if dt_from:
            where.append( f"created >= '{dt_from}' ")
        if dt_to:
            where.append(f"created <= '{dt_to}' ")
        whereclaus = " and ".join(where)
        sql += whereclaus
    sql += "ORDER BY created"
    # print(sql)
    df = DB.dffromsql(sql)
    df['created'] = pd.to_datetime(df['created'], format='%Y-%m-%d %H:%M:%S')
    df['USD'] = pd.to_numeric(df['USD'])
    return df

def createchart(df: DataFrame, field:str='Total', zero:bool=False)->str:
    models = df['model'].unique()
    stats = []
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    for model in models:
        dfm = df.loc[df['model'] == model]
        min = dfm[field].min()
        max = dfm[field].max()
        f = dfm[field].iloc[0]
        l = dfm[field].iloc[-1]
        change = l-f
        perc = (change/f)*100
        tsat = f"{model}: Range (min, max): {min:.2f} to {max:.2f}, Change: {change:.2f} ({perc:.2f}%) "
        tsat += f"Current Total {l:.2f}<br>"
        stats.append(tsat)
        # print(dfm.head())
        if zero:
            first = dfm[field].iloc[0]
            dfm[field] = dfm[field] - first
        # print(dfm.head())
        fig.add_trace(go.Scatter(x = dfm['created'],
                        y = dfm[field],
                        mode='lines',
                        legendgroup=model,
                        name=model),
                        secondary_y=True)
        dfso = dfm.loc[(dfm['trade'] == 'SELL') & (dfm['action'] == 'OPEN')]
        dfsc = dfm.loc[(dfm['trade'] == 'SELL') & (dfm['action'] == 'CLOSE')]
        dfbo = dfm.loc[(dfm['trade'] == 'BUY') & (dfm['action'] == 'OPEN')]
        dfbc = dfm.loc[(dfm['trade'] == 'BUY') & (dfm['action'] == 'CLOSE')]
        dfpoints = [(dfbo, "BUY-OPEN", 'green', 'triangle-right'),
                    (dfbc, "BUY-CLOSE", 'green', 'triangle-left'),
                    (dfso, "SELL-OPEN", 'red', 'triangle-right'), 
                    (dfsc, "SELL-CLOSE", 'red', 'triangle-left')]
        for pts in dfpoints:
            fig.add_trace(go.Scatter(x = pts[0]['created'],
                        y = pts[0][field],
                        mode='markers',
                        marker = dict(size =8, color =pts[2], symbol =pts[3]),
                        legendgroup=model,
                        name=pts[1]),
                        secondary_y=True)
    
    btc.updatehistory()
    now, yesterday = getnow()
    df = btc.getbtc(yesterday)    
    
    min = df['Close'].min()
    max = df['Close'].max()
    f = df['Close'].iloc[0]
    l = df['Close'].iloc[-1]
    
    change = l-f
    perc = (change/f)*100
    tsat = f"BTC: Close Range (min, max): {min:.2f} to {max:.2f}, Change: {change:.2f} ({perc:.2f}%) "
    tsat += f"Current Rate {l:.2f}<br>"
    stats.append(tsat)

    fig.add_trace(go.Candlestick(
                        x=df['dtz'],
                        open=df['Open'],
                        high=df['High'],
                        low=df['Low'],
                        close=df['Close'],
                        name = 'Candles'),
                        secondary_y=False)

    fig.update_layout(xaxis_rangeslider_visible=False)
    title = f"Bots Versus Market: {field}<br>"
    for s in stats:
        title += s
    fig.update_layout(title_text=title)
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="<b>US $</b>", secondary_y=True)
    fig.update_yaxes(title_text="<b>1 Bitcoin</b>", secondary_y=False)

    # fig.show()
    mfile = "-".join(models).replace(" ","")
    html = f"data/{mfile}.html"
    fig.write_html(html)
    chartzip =os.path.join('zipfiles', mfile + ".zip")
    with ZipFile(chartzip, 'w') as zipf:
        zipf.write(html, arcname=f"{mfile}.html", compress_type=ZIP_DEFLATED)
    return chartzip

def createfillchart(df: DataFrame)->str:
    models = df['model'].unique()
    # print(models)
    chartname = ''
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    for model in models:
        dfm = df.loc[df['model'] == model]
        chartname = model
        fig.add_trace(go.Scatter(x = dfm['created'],
                        y = dfm['USD'],
                        mode='lines',
                        stackgroup=model,
                        line=dict(width=3, color='rgb(131, 90, 241)'),
                        name=f"{model} USD"
                        # ,groupnorm='percent'
                        ),
                        secondary_y=True)
        fig.add_trace(go.Scatter(x = dfm['created'],
                        y = dfm['BTC$'],
                        mode='lines',
                        stackgroup=model,
                        line=dict(width=3, color='rgb(127, 166, 238)'),
                        name=f"{model} BTC$"
                        # ,groupnorm='percent'
                        ),
                        secondary_y=True)
    fig.add_trace(go.Scatter(x = df['created'],
                        y = df['rate'],
                        mode='lines',
                        line=dict(
                                color='black',
                                width=2
                            ),
                        name='BTC$'),
                        secondary_y=False)

    
    fig.update_layout(title_text=f"Bots Versus Market - {chartname}")
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="<b>US $</b>", secondary_y=True)
    fig.update_yaxes(title_text="<b>1 Bitcoin</b>", secondary_y=False)

    # fig.show()
    mfile = "-".join(models).replace(" ","")
    html = f"data/{mfile}.html"
    fig.write_html(html)
    chartzip =os.path.join('zipfiles', mfile + ".zip")
    with ZipFile(chartzip, 'w') as zipf:
        zipf.write(html, arcname=f"{mfile}.html", compress_type=ZIP_DEFLATED)
    return chartzip


def main():
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dt_from = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    df = dffromdb('status', model=[3], dt_from=dt_from, dt_to=now )
    # print(df.head())
    o = createchart(df, field='USD', zero=True)
    print(o)

if __name__ == '__main__':
    main()