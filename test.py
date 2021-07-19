
from config.Ttrader_config import portfolios
from exchange.coinbase_pro import api

p = portfolios

CbPortfolios =[]
for model, cfg in p.items(): 
    CB = api.AuthAPI(p[model]['api_key'], p[model]['api_secret'], p[model]['api_passphrase'], p[model]['api_url'],p[model]['name'])
    CbPortfolios.append(CB)

print(CbPortfolios[1])
