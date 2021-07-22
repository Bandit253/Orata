
from config.Ttrader_config import portfolios
from exchange.coinbase_pro import api

p = portfolios
model = '5'
# CbPortfolios =[]
# for model, cfg in p.items(): 
#     CB = api.AuthAPI(p[model]['api_key'], p[model]['api_secret'], p[model]['api_passphrase'], p[model]['api_url'],p[model]['name'])
#     CbPortfolios.append(CB)

# print(CbPortfolios[1])

a = "on 1 3 4"
cmd = a.split(" ")
for x in cmd[2:]:
    if int(cmd[int(x)]) < 5:
        print(int(x))
