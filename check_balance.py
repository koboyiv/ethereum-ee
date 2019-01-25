import csv
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from web3 import Web3

etherscan_provider = Web3.HTTPProvider( 'http://x.stockradars.co:8545')
w3 = Web3(etherscan_provider)
balances = []

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

addresses = get_address()
#with open('address.csv') as csv_file:
   # csv_reader = csv.reader(csv_file, delimiter=',')
   # line_count = 0
   # for row in csv_reader:
for row in tqdm(addresses):
    #checksum_address = w3.toChecksumAddress(row[0])
    checksum_address = w3.toChecksumAddress(row)
    balance = w3.eth.getBalance(checksum_address, block_identifier=8000)
    #print(balance)
    balances.append((checksum_address,balance))

sorted_by_second = sorted(balances, key=lambda tup: tup[1], reverse=True)
print(sorted_by_second[0:100])

