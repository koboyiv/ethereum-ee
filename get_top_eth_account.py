import asyncio
import json
import math
import time
import csv
from operator import itemgetter
import aiohttp
import requests
import web3
import numpy as np
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from tqdm import tqdm
import config

engine = create_engine(config.DB.ethereum(), echo=False)

async def http_v1_get(address_list,block_number):
    msg_list = []
    result = []

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect("ws://x.stockradars.co:8546/") as ws:
            data = json_rpc_block(block_number)

            for index, address in enumerate(tqdm(address_list)):
                await ws.send_str(data % (address, index))
                msg_list.append(await ws.receive())

    for msg in msg_list:
        res = msg.json()

        if 'result' in res:
            result.append({
                'address': address_list[int(res['id'])],
                'balance': web3.Web3.toInt(hexstr=res['result'])
            })
        else:
            print(res)

    engine.execute(text("""
            INSERT IGNORE INTO ethereum.balance (address, token, blockNumber, balance)
            VALUES (:address, 'ETH', {}, :balance)
    """.format(block_number)), result)

    return result


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_address():
    result = []
    s = requests.Session()

    for page in tqdm(range(1,11)):
        r = s.get('https://etherscan.io/accounts/{}?ps=100'.format(page))
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        td = soup.select('td[width="330px"]')

        for t in td:
            result.append(t.text)

    return result

def get_numblocks():

    numblock = []
    with open('numblocks.csv') as csv_file:
        csv_reader = csv.reader(csv_file)
        print(csv_reader)

        for row in csv_reader:
            numblock.append(row[0])
        """ Convert str to int list"""
        numblock = list(map(int, numblock))

    return numblock

def json_rpc_block(numblock):
    data_font ='{"jsonrpc": "2.0", "method": "eth_getBalance", "params": ["%s", "'
    hex_block = hex(numblock)
    data_back = '"], "id": %s}'

    return data_font + hex_block + data_back

if __name__ == '__main__':

    all_address = get_address()
    numblock = get_numblocks()
    numblock = np.asarray(numblock)

    start = time.time()
    print(start)

    loop = asyncio.get_event_loop()

    result = []
    for i in numblock:
        if i == 2:
            break

        else:
            return_result = loop.run_until_complete(asyncio.gather(
                *[http_v1_get(part_address,i) for part_address in chunks(all_address, math.ceil(len(all_address) / 4.0))]
            ))

            for r in return_result:
                result += r

            toplist = sorted(result, key=itemgetter('balance'), reverse=True)
            print(time.time() - start)
            print("block number:", i)
            #print(toplist[0:101])
        result = []

    loop.close()

    """  
        result = []
        for r in return_result:
            result += r

        toplist = sorted(result, key=itemgetter('balance'), reverse=True)
        print(toplist[0:101])
    """
