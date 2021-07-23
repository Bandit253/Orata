
import datetime
import pandas as pd
import os
from pandas.core.frame import DataFrame
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from config.Ttrader_config import DB_CONFIG
from util import DBaccess as db
from zipfile import ZipFile, ZIP_DEFLATED
pd.options.mode.chained_assignment = None

DB = db.postgres(DB_CONFIG)

appPath = os.path.dirname(os.path.realpath(__file__))
dataPath = os.path.join(appPath, 'data')


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

def createchart(df: DataFrame, zero:bool=False)->str:
    models = df['model'].unique()
    stats = []
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    for model in models:
        dfm = df.loc[df['model'] == model]
        min = dfm['Total'].min()
        max = dfm['Total'].max()
        f = dfm['Total'].iloc[0]
        l = dfm['Total'].iloc[-1]
        change = l-f
        perc = (change/f)*100
        tsat = f"{model}: Range (min, max): {min:.2f} to {max:.2f}, Change: {change:.2f} ({perc:.2f}%) "
        tsat += f"Current Total {l:.2f}<br>"
        stats.append(tsat)
        if zero:
            first = dfm['Total'].iloc[0]
            dfm['Total'] = dfm['Total'] - first
        print(dfm.head())
        fig.add_trace(go.Scatter(x = dfm['created'],
                        y = dfm['Total'],
                        mode='lines',
                        name=model),
                        secondary_y=True)
        dfso = df.loc[(df['trade'] == 'SELL') & (df['action'] == 'OPEN')]
        dfsc = df.loc[(df['trade'] == 'SELL') & (df['action'] == 'CLOSE')]
        dfbo = df.loc[(df['trade'] == 'BUY') & (df['action'] == 'OPEN')]
        dfbc = df.loc[(df['trade'] == 'BUY') & (df['action'] == 'CLOSE')]
        dfpoints = [(dfbo, "BUY-OPEN", 'green', 'triangle-up'),
                    (dfbc, "BUY-CLOSE", 'red', 'triangle-up'),
                    (dfso, "SELL-OPEN", 'green', 'triangle-down'), 
                    (dfsc, "SELL-CLOSE", 'red', 'triangle-down')]
        for pts in dfpoints:
            fig.add_trace(go.Scatter(x = pts[0]['created'],
                        y = pts[0]['Total'],
                        mode='markers',
                        marker = dict(size =8, color =pts[2], symbol =pts[3]),
                        name=pts[1]),
                        secondary_y=True)


    min = df['rate'].min()
    max = df['rate'].max()
    f = df['rate'].iloc[0]
    l = df['rate'].iloc[-1]
    
    change = l-f
    perc = (change/f)*100
    tsat = f"BTC: Range (min, max): {min:.2f} to {max:.2f}, Change: {change:.2f} ({perc:.2f}%) "
    tsat += f"Current Rate {l:.2f}<br>"
    stats.append(tsat)

    fig.add_trace(go.Scatter(x = df['created'],
                        y = df['rate'],
                        mode='lines',
                        line=dict(
                                color='black',
                                width=2
                            ),
                        name='BTC$'),
                        secondary_y=False)
    title = "Bots Versus Market<br>"
    for s in stats:
        title += s

    fig.update_layout(title_text=title)
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="<b>US $</b>", secondary_y=True)
    fig.update_yaxes(title_text="<b>1 Bitcoin</b>", secondary_y=False)

    fig.show()
    # mfile = "-".join(models).replace(" ","")
    # html = f"data/{mfile}.html"
    # fig.write_html(html)
    # chartzip =os.path.join('zipfiles', mfile + ".zip")
    # with ZipFile(chartzip, 'w') as zipf:
    #     zipf.write(html, arcname=f"{mfile}.html", compress_type=ZIP_DEFLATED)
    # return chartzip

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
    df = dffromdb('status', model=[1, 0], dt_from=dt_from, dt_to=now )
    # print(df.head())
    o = createchart(df)
    print(o)

if __name__ == '__main__':
    main()