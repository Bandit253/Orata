
import datetime
from exchange.coinbase_pro import api
from config.Ttrader_config import portfolios, START_BTC, START_USD
import pandas as pd


CBs =[]
for model, cfg in portfolios.items(): 
    CB = api.AuthAPI(portfolios[model]['api_key'], 
                    portfolios[model]['api_secret'], 
                    portfolios[model]['api_passphrase'], 
                    portfolios[model]['api_url'],
                    portfolios[model]['name'],
                    portfolios[model]['id'],
                    portfolios[model]['market'],
                    portfolios[model]['trade_unit'])
    CBs.append(CB)
CBpub = api.PublicAPI()

def getmarketprice(symbol):
    if symbol == 'USDC':
        rate = CBpub.getprice(f'USDT-USDC')
        return float(rate)
    elif symbol == 'USDC':
        rate = CBpub.getprice(f'USDT-USD')
        return float(rate)
    elif symbol != 'USD':
        rate = CBpub.getprice(f'{symbol}-USD')
        return float(rate)
    else:
        return 1

def reportbalance(CB):
    
    df = CB.getAccounts()
    df['Rate'] = df.apply(lambda row: getmarketprice(row['currency']), axis=1)
    df['Total $US'] = df.apply(lambda row: (row['Rate']*float(row['balance'])), axis=1)
    balance = CB.name
    balance +='\ncurrency : available : Rate : $US \n'
    for index, row in df.iterrows():
        balance += f"{row['currency']} : {float(row['available']):.10f} : {float(row['Rate']):.2f} : {float(row['Total $US']):.2f}\n"
    balance += f"Total $US {df['Total $US'].sum():.2f}"
    
    return balance

def getbalance(CB):
    df = CB.getAccounts()
    df['Rate'] = df.apply(lambda row: getmarketprice(row['currency']), axis=1)
    df['Total $US'] = df.apply(lambda row: (row['Rate']*float(row['balance'])), axis=1)
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    btcbal = 0
    for index, row in df.iterrows():
        if row['currency'] == 'USDC'or row['currency'] == 'USD':
            dollarbal = row['available']
            btcrate = 0
            btcdollar = row['Total $US']
        elif row['currency'] == 'BTC':
            btcbal = row['available']
            btcrate = row['Rate']
            btcdollar = row['Total $US']
    status = {"created" : now, "model" : CB.name, "BTC" : btcbal, "rate" : btcrate,  "BTC$" : btcdollar, "USD" : dollarbal, "Total" : df['Total $US'].sum() }
    dfstatus = pd.DataFrame([status])
    return dfstatus, status

def resetbalances(CBfrom, CBto):
    curbal = CBto.getAccounts()
    for index, row in curbal.iterrows():
        if row['currency'] == 'USD' or row['currency'] == 'USDC':
            dol_dif = START_USD - float(row['available'])
            if dol_dif > 0:
                CBfrom.transfer(CBfrom.id, CBto.id, 'USD', dol_dif )
            else:
                CBto.transfer(CBto.id, CBfrom.id,  'USD', -dol_dif )
        elif row['currency'] == 'BTC':
            BTC_dif = START_BTC - float(row['available'])
            if BTC_dif > 0:
                CBfrom.transfer(CBfrom.id, CBto.id, 'BTC', BTC_dif )
            else:
                CBto.transfer(CBto.id, CBfrom.id, 'BTC', -BTC_dif )
    return


# transfer(CBs[4], CBs[3])

# # def main():
#     r = recordstatus(CBs[4])
#     print(r)
 
# if __name__ == '__main__':
#     main()