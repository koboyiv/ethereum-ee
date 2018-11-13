import asyncio
import json
import math
import time

import aiohttp
import requests
import web3
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from tqdm import tqdm

import config

engine = create_engine(config.DB.ethereum(), echo=False)


async def http_v1_get(address_list):
    msg_list = []
    result = []

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect("ws://x.stockradars.co:8546/") as ws:
            data = '{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}'
            await ws.send_str(data)
            msg = await ws.receive()
            res = msg.json()
            block_number = web3.Web3.toInt(hexstr=res['result'])

            data = '{"jsonrpc": "2.0", "method": "eth_getBalance", "params": ["%s", "latest"], "id": %s}'

            for index, address in enumerate(tqdm(address_list)):
                await ws.send_str(data % (address, index))
                msg_list.append(await ws.receive())

    for msg in msg_list:
        res = msg.json()

        if 'result' in res:
            result.append({
                'address': address_list[int(res['id'])],
                'balance': '{}'.format(web3.Web3.fromWei(
                    web3.Web3.toInt(hexstr=res['result']),
                    'ether'
                ))
            })
        else:
            print(res)

    engine.execute(text("""
        INSERT IGNORE INTO ethereum.balance (address, token, blockNumber, balance, `change`, `date`)
        VALUES (:address, 'ETH', {}, :balance, null , current_date())
    """.format(block_number)), result)

    return result


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_address():
    result = []
    s = requests.Session()

    for page in tqdm(range(1, 101)):
        r = s.get('https://etherscan.io/accounts/{}?ps=100'.format(page))
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        td = soup.select('td[width="330px"]')

        for t in td:
            result.append(t.text)

    return result


if __name__ == '__main__':
    all_address = get_address()
    print(len(all_address))

    start = time.time()
    print(start)

    loop = asyncio.get_event_loop()

    return_result = loop.run_until_complete(asyncio.gather(
        *[http_v1_get(part_address) for part_address in chunks(all_address, math.ceil(len(all_address) / 4.0))]
    ))

    loop.close()
    print(time.time() - start)

    insert_radars()
