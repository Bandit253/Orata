
import datetime
from exchange.coinbase_pro import api
from config.Ttrader_config import portfolios
import pandas as pd


CBs =[]
for model, cfg in portfolios.items(): 
    CB = api.AuthAPI(portfolios[model]['api_key'], 
                    portfolios[model]['api_secret'], 
                    portfolios[model]['api_passphrase'], 
                    portfolios[model]['api_url'],
                    portfolios[model]['name'])
    CBs.append(CB)
CBpub = api.PublicAPI()

def getmarketprice(symbol):
    if symbol != 'USD':
        rate = CBpub.getprice(f'{symbol}-USD')
        return float(rate)
    else:
        return 1

def reportbalance(CB):
    df = CB.getAccounts()
    df['Rate'] = df.apply(lambda row: getmarketprice(row['currency']), axis=1)
    df['Total $US'] = df.apply(lambda row: (row['Rate']*float(row['balance'])), axis=1)
    balance = CB.name
    balance +='currency : available : Rate : $US \n'
    for index, row in df.iterrows():
        balance += f"{row['currency']} : {float(row['available']):.10f} : {float(row['Rate']):.2f} : {float(row['Total $US']):.2f}\n"
    balance += f"Total $US {df['Total $US'].sum():.2f}"
    return balance

def recordstatus(CB):
    df = CB.getAccounts()
    df['Rate'] = df.apply(lambda row: getmarketprice(row['currency']), axis=1)
    df['Total $US'] = df.apply(lambda row: (row['Rate']*float(row['balance'])), axis=1)
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for index, row in df.iterrows():
        if row['currency'] == 'USD':
            dollarbal = row['available']
        elif row['currency'] == 'BTC':
            btcbal = row['available']
            btcrate = row['Rate']
            btcdollar = row['Total $US']
    status = {"created" : now, "model" : CB.name, "BTC" : btcbal, "rate" : btcrate,  "BTC$" : btcdollar, "USD" : dollarbal, "Total" : df['Total $US'].sum() }
    dfstatus = pd.DataFrame([status])
    return dfstatus

# def main():
#     r = recordstatus(CBs[4])
#     print(r)
 
# if __name__ == '__main__':
#     main()