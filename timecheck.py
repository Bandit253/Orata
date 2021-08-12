
import datetime
import requests


def displayClick():
    api_url =  "https://api.binance.com"
    local_dt = datetime.datetime.now()
    res = requests.get(api_url +'/api/v3/time')
    t = res.json()['serverTime']/1000
    s = datetime.datetime.fromtimestamp(t)

    msg = f"Local {local_dt}", f"Server {s}", f"Delta : {str(local_dt - s)}"
    return msg


def main():
    tim = displayClick()
    print(tim)
 
if __name__ == '__main__':
    main()