import requests
import datetime
import json
from datetime import date
from sqlalchemy import create_engine, text
from tqdm import tqdm
import config


engine = create_engine(config.DB.ethereum(), echo=False)

def daily_price_historical(symbol, comparison_symbol, all_data=True, limit=1, aggregate=1, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym={}&limit={}&aggregate={}'\
            .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate)
    if exchange:
        url += '&e={}'.format(exchange)
    if all_data:
        url += '&allData=true'
    page = requests.get(url)
    data = page.json()['Data']

    d_dict = []

    for d in tqdm(data):
        status = ''
        change = d['open']-d['close']
        if change >= 7:
            status = 'S_high'

        elif change >= 0:
            status = 'high'

        elif change <= -7:
            status = 'S_low'

        elif change < 0:
            status = 'low'


        d_dict.append({
            'datetime': datetime.datetime.fromtimestamp(d['time']).strftime('%Y-%m-%d'),
            'change': change,
            'status': status,
            'open_v': d['open'],
            'close_v': d['close'],
            'high': d['high'],
            'low': d['low'],
            'volumefrom': d['volumefrom'],
            'volumeto': d['volumeto']
        })

if __name__ == '__main__':
    data = daily_price_historical('ETH','USD')

    engine.execute(text("""
                  INSERT IGNORE INTO ethereum.price_history_day (`open`, `high`, `low`, `close`, `volumefrom`,
                  `volumeto`, `date`, `change`, `status`, exchange_N)
                  VALUES (:open_v, :high, :low, :close_v, :volumefrom, :volumeto, :datetime, 
                  :change, :status, 'ETH/USD')"""), data)