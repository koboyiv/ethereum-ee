import math
import time
import json
import requests
from web3 import Web3
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from tqdm import tqdm
import urllib.parse
from decimal import Decimal

import config
from web3.exceptions import BadFunctionCallOutput
import locale

locale.setlocale(locale.LC_ALL, '')
w3 = Web3(Web3.WebsocketProvider("ws://x.stockradars.co:8546"))
ERC20_ABI = json.loads(
    '[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_value","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"},{"name":"_spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_from","type":"address"},{"indexed":true,"name":"_to","type":"address"},{"indexed":false,"name":"_value","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_owner","type":"address"},{"indexed":true,"name":"_spender","type":"address"},{"indexed":false,"name":"_value","type":"uint256"}],"name":"Approval","type":"event"}]')  # noqa: 501

engine = create_engine(config.DB.radars(), echo=False)


def get_token_supply(token_address):
    try:
        erc20 = w3.eth.contract(
            address=Web3.toChecksumAddress(token_address.strip()),
            abi=ERC20_ABI,
        )
        return erc20.functions.totalSupply().call()
    except BadFunctionCallOutput as e:
        print('Unknown totalSupply', token_address)
        return 0


def get_holder(address, supply):
    result = []
    s = requests.Session()

    for page in range(1, 21):
        url = 'https://etherscan.io/token/generic-tokenholders2?a={address}&s={total_supply}&p={page}'.format(
            total_supply=urllib.parse.quote('%.8E' % Decimal(supply)),
            address=address,
            page=page)

        r = s.get(url)
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        if table is None:
            print(url)
            break

        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            r = [ele for ele in cols if ele]
            if len(r) == 4:
                result.append(r)

        time.sleep(2)

    return result


def insert_result(holders):
    engine.execute(text("""
           REPLACE INTO radars.top_holder (D_trade, N_address, I_coin, I_order, N_balance, Z_percentage) 
           VALUES (CURRENT_DATE(), :N_address, :I_coin, :I_order, :N_balance, :Z_percentage);
   """), holders)


def get_token():
    res = engine.execute(text("""
           SELECT id, symbol, address, totalSupply, decimals FROM ethereum.tokens ORDER BY id
   """))
    return res


if __name__ == '__main__':
    for token in get_token():
        print(token['symbol'])
        total_supply = get_token_supply(token['address'])
        if total_supply == 0:
            continue

        all_holders = get_holder(token['address'], total_supply)
        insert_data = []
        for h in all_holders:
            data = {
                "I_order": h[0],
                "N_address": h[1],
                "I_coin": token['id'],
                "N_balance": h[2],
                "Z_percentage": h[3].replace('%', '')
            }
            insert_data.append(data)

        insert_result(insert_data)
